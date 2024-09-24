from sync_tmdb.utils.config import Config, get_config
from sync_tmdb.utils.tmdb import TMDBClient

class LanguageConfig:
	def __init__(self):
		self.main_config: Config = get_config()
		self.tmdb_client: TMDBClient = TMDBClient(self.main_config)


