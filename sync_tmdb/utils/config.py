import os
import yaml
from sync_tmdb.utils.extra_language import ExtraLanguages
from sync_tmdb.utils.tmdb import TMDBClient
from prefect.blocks.system import Secret

class Config:
	def __init__(self, config_file: str = "sync_tmdb/config.yml"):
		self.config_file = config_file

		# Default values
		self.tmp_directory: str = ".tmp"
		self.extra_languages: ExtraLanguages = ExtraLanguages()
		self.tmdb_client: TMDBClient = None
		# self.tmdb_api_keys: list = []
		# self.tmdb_base_url: str = "https://api.themoviedb.org/3"

		# Load configuration
		self._load_config()

	def _load_config(self):
		if not os.path.exists(self.config_file):
			raise FileNotFoundError(f"Configuration file not found: {self.config_file}")
		with open(self.config_file, "r") as f:
			self.config = yaml.safe_load(f)
		
		self.tmp_directory = self.config.get("tmp_directory", self.tmp_directory)
		self.extra_languages = ExtraLanguages(languages=self.config.get("extra_languages", self.extra_languages))
		self.tmdb_client = TMDBClient(config=self.config)
		# self.tmdb_api_keys = self._get_tmdb_api_keys()
		# self.tmdb_base_url = self.config.get("tmdb_base_url", self.tmdb_base_url)

def get_config() -> Config:
	return Config()
