from datetime import date
from ...models.config import Config

class GenreConfig(Config):
	def __init__(self, date: date):
		super().__init__(date=date)
		self.flow_name: str = "genre"

		# Tables
		self.table_genre: str = self.config.get("db_tables", {}).get("genre", "tmdb_genre")
		self.table_genre_translation: str = self.config.get("db_tables", {}).get("genre_translation", "tmdb_genre_translation")

		# CSV file containing the missing genres
		self.genre: str = None
		self.genre_translation: str = None


