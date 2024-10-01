from datetime import date
from ...models.config import Config

class LanguageConfig(Config):
	def __init__(self, date: date):
		super().__init__(date=date)
		self.flow_name: str = "language"

		# Tables
		self.table_language: str = self.config.get("db_tables", {}).get("language", "tmdb_language")
		self.table_language_translation: str = self.config.get("db_tables", {}).get("language_translation", "tmdb_language_translation")

		# CSV file containing the missing languages
		self.language: str = None
		self.language_translation: str = None


