from prefect import task
from prefect.logging import get_run_logger
from prefect.blocks.system import Secret
from itertools import cycle
import requests
from datetime import date
from ..utils.file_manager import download_file, decompress_file
from ..utils.concurreny import limit_concurrency
import os
import pandas as pd

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
	
	@task(cache_policy=None)
	@limit_concurrency(max_workers=20)
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

	@task(cache_policy=None)
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
	
	@task(cache_policy=None, log_prints=False)
	def get_changed_ids(self, type: str, start_date: date, end_date: date) -> set:
		try:
			ids: set = set()
			self.logger.info(f"Getting changed ids for {type} from {start_date} to {end_date}")

			data = self.request(f"{type}/changes", {"start_date": start_date.strftime("%Y-%m-%d"), "end_date": end_date.strftime("%Y-%m-%d")})
			numbers_of_pages = data["total_pages"]
			number_of_results = data["total_results"]

			responses = []
			for i in range(1, numbers_of_pages+1):
				responses.append(self.request.submit(f"{type}/changes", {"page": i, "start_date": start_date.strftime("%Y-%m-%d"), "end_date": end_date.strftime("%Y-%m-%d")}))

			for i, response in enumerate(responses, start=1):
				results = response.result()
				if "results" in results:
					ids |= set([item["id"] for item in results["results"]])
				else:
					raise ValueError(f"Failed to get changed ids for page {i}: {results}")
			
			if len(ids) != number_of_results:
				self.logger.warning(f"Number of ids does not match the number of results: {len(ids)} != {number_of_results}")
				# raise ValueError(f"Number of ids does not match the number of results: {len(ids)} != {number_of_results}")

			return ids
		except Exception as e:
			raise ValueError(f"Failed to get changed ids: {e}")
	