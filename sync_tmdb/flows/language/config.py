from ...models.config import Config
from ...models.tmdb import TMDBClient

class LanguageConfig:
	def __init__(self):
		self.flow_name: str = "language"
		self.main_config = Config()
		# Tables
		self.table_language: str = self.main_config.config.get("db_tables", {}).get("language", "tmdb_language")
		self.table_language_translation: str = self.main_config.config.get("db_tables", {}).get("language_translation", "tmdb_language_translation")

		# CSV file containing the missing languages
		self.language: str = None
		self.language_translation: str = None


