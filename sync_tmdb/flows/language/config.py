from datetime import date
from ...models.config import Config

class LanguageConfig(Config):
	def __init__(self, date: date):
		super().__init__(date=date)
		self.flow_name: str = "language"

		# Tables
		self.table_language: str = self.config.get("db_tables", {}).get("language", "tmdb_language")

		# Columns
		self.language_column: str = "iso_639_1"


