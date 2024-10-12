from prefect import task
from prefect.logging import get_run_logger
from prefect.blocks.system import Secret
from prefect.concurrency.sync import rate_limit 
from itertools import cycle
import requests
from datetime import date
from ..utils.file_manager import download_file, decompress_file
import os
import json

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
	
	@task 
	def request(self, endpoint: str, params: dict = {}) -> dict:
		rate_limit("tmdb-api")
		self.api_key = self._get_next_api_key()
		url = f"{self.base_url}/{endpoint}"
		params["api_key"] = self.api_key
		response = requests.get(url, params=params)
		response.raise_for_status()
		data = response.json()
		if "success" in data and not data["success"]:
			raise ValueError(f"Failed to get data from TMDB: {data}")
		return data

	@task
	def get_export_ids(self, type: str, date: date) -> list:
		try:
			tmdb_export_collection_url_template = "http://files.tmdb.org/p/exports/{type}_ids_{date}.json.gz"
			url = tmdb_export_collection_url_template.format(type=type, date=date.strftime("%m_%d_%Y"))

			file = download_file(url=url, tmp_directory=".tmp", prefix=f"{type}_ids_{date}")

			file = decompress_file(file)

			with open(file, "r", encoding="utf-8") as f:
				export_ids = [json.loads(line) for line in f.readlines()]
			
			if os.path.exists(file):
				os.remove(file)
			
			if len(export_ids) == 0:
				raise ValueError(f"No export ids found for {type} on {date}")
			
			return export_ids
		except Exception as e:
			raise ValueError(f"Failed to get export ids: {e}")
	
	@task
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
				raise ValueError(f"Number of ids does not match the number of results: {len(ids)} != {number_of_results}")

			return ids
		except Exception as e:
			raise ValueError(f"Failed to get changed ids: {e}")
	