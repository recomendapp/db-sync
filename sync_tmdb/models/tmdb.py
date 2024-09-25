from prefect import task
from prefect.logging import get_run_logger
from prefect.blocks.system import Secret
from itertools import cycle
import requests

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
		self.api_key = self._get_next_api_key()
		url = f"{self.base_url}/{endpoint}"
		params["api_key"] = self.api_key
		response = requests.get(url, params=params)
		response.raise_for_status()
		return response.json()
	