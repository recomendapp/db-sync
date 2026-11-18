from prefect import task
from prefect.logging import get_run_logger
from prefect.blocks.system import Secret
from prefect.cache_policies import NO_CACHE
from itertools import cycle
import requests
from datetime import date, timedelta
from ..utils.file_manager import download_file, decompress_file
from ..utils.concurreny import limit_concurrency, map_concurrently
import os
import pandas as pd

# The changes endpoints reject page 501 with a 400, and a window wider than two weeks
# with a 422. Changes older than that are not served at all, so a longer gap can only be
# closed by the export reconciliation, not by this endpoint.
TMDB_CHANGES_MAX_PAGE = 500
TMDB_CHANGES_MAX_WINDOW_DAYS = 14

# Every outbound call goes through request, so this caps the whole process. Callers
# size their own thread pools from it to keep their workers busy without queueing
# up payloads behind the semaphore.
TMDB_MAX_CONCURRENT_REQUESTS = 20

class TMDBClient:
	def __init__(self, config: dict):
		api_keys = self._get_tmdb_api_keys()
		if not api_keys or len(api_keys) == 0:
			raise ValueError("No API keys found")
		self.api_key_cycle = cycle(api_keys)
		self.api_key: str = self._get_next_api_key()
		self.logger = get_run_logger()
		self.base_url = config.get("tmdb_base_url", "https://api.themoviedb.org/3")
	
	def _get_tmdb_api_keys(self) -> list:
		try:
			api_keys = Secret.load("tmdb-api-key").get().split()
			if not api_keys or len(api_keys) == 0:
				raise ValueError("No API keys found")
			return api_keys
		except Exception as e:
			raise ValueError(f"No API keys found")
		
	def _get_next_api_key(self) -> str:
		return next(self.api_key_cycle)
	
	@limit_concurrency(max_workers=TMDB_MAX_CONCURRENT_REQUESTS)
	def request(self, endpoint: str, params: dict = {}) -> dict:
		# rate_limit("tmdb-api")
		self.api_key = self._get_next_api_key()
		url = f"{self.base_url}/{endpoint}"
		params["api_key"] = self.api_key
		response = requests.get(url, params=params)
		response.raise_for_status()
		data = response.json()
		if "success" in data and not data["success"]:
			raise ValueError(f"Failed to get data from TMDB: {data}")
		return data

	@task(cache_policy=NO_CACHE)
	def get_export_ids(self, type: str, date: date, columns_to_keep: list = ['id', 'popularity']) -> pd.DataFrame:
		"""
		Downloads, reads, and filters a TMDB export file efficiently.
		- Reads the file in chunks to avoid loading everything into memory.
		- Keeps only the 'id' and 'popularity' columns.
		- Downcasts data types to further reduce memory usage.
		"""
		file = None
		try:
			tmdb_export_collection_url_template = "http://files.tmdb.org/p/exports/{type}_ids_{date}.json.gz"
			url = tmdb_export_collection_url_template.format(type=type, date=date.strftime("%m_%d_%Y"))

			self.logger.info(f"Downloading and processing {url}...")
			file = download_file(url=url, tmp_directory=".tmp", prefix=f"{type}_ids_{date}")
			file = decompress_file(file)

			chunk_iterator = pd.read_json(file, lines=True, chunksize=100000)
			
			processed_chunks = []
			
			for chunk in chunk_iterator:
				existing_columns = [col for col in columns_to_keep if col in chunk.columns]
				processed_chunks.append(chunk[existing_columns])
	
			df = pd.concat(processed_chunks, ignore_index=True)
			self.logger.info(f"Successfully loaded {len(df)} records with relevant columns.")

			if len(df) == 0:
				raise ValueError(f"No export ids found for {type} on {date}")

			df['id'] = pd.to_numeric(df['id'], downcast='integer')
			if 'popularity' in df.columns:
				df['popularity'] = pd.to_numeric(df['popularity'], downcast='float')
				
			return df

		except Exception as e:
			raise ValueError(f"Failed to get export ids: {e}")
		finally:
			if file and os.path.exists(file):
				os.remove(file)
	
	@task(cache_policy=NO_CACHE, log_prints=False)
	def get_changed_ids(self, type: str, start_date: date, end_date: date) -> set:
		try:
			ids: set = set()
			change_records: int = 0

			oldest_available = end_date - timedelta(days=TMDB_CHANGES_MAX_WINDOW_DAYS - 1)
			if start_date < oldest_available:
				self.logger.warning(f"Changes for {type} before {oldest_available} are no longer served by TMDB (asked from {start_date}): that gap has to be closed by the export reconciliation")
				start_date = oldest_available

			self.logger.info(f"Getting changed ids for {type} from {start_date} to {end_date}")

			# One day at a time: a single day stays far below the page cap, and the daily
			# windows add up exactly, so nothing is counted twice or missed.
			day = start_date
			while day <= end_date:
				day_ids, day_records = self._get_changed_ids_for_day(type, day)
				ids |= day_ids
				change_records += day_records
				day += timedelta(days=1)

			self.logger.info(f"Found {len(ids)} changed {type} ids across {change_records} change records")

			return ids
		except Exception as e:
			raise ValueError(f"Failed to get changed ids: {e}")

	def _get_changed_ids_for_day(self, type: str, day: date) -> tuple[set, int]:
		params = {"start_date": day.strftime("%Y-%m-%d"), "end_date": day.strftime("%Y-%m-%d")}
		first_page = self.request(f"{type}/changes", params)
		total_pages = first_page["total_pages"]
		pages = min(total_pages, TMDB_CHANGES_MAX_PAGE)

		if total_pages > pages:
			self.logger.warning(f"{type} changes for {day} span {total_pages} pages but only {pages} are reachable: the tail has to be closed by the export reconciliation")

		ids: set = {item["id"] for item in first_page.get("results", [])}

		def get_page(page: int) -> dict:
			return self.request(f"{type}/changes", {**params, "page": page})

		pages_results = map_concurrently(get_page, range(2, pages + 1), TMDB_MAX_CONCURRENT_REQUESTS)
		for page, results in enumerate(pages_results, start=2):
			if "results" not in results:
				raise ValueError(f"Failed to get changed ids for {day} page {page}: {results}")
			ids |= {item["id"] for item in results["results"]}

		return ids, first_page["total_results"]
	