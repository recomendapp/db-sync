from datetime import date
from ...models.config import Config

class CountryConfig(Config):
	def __init__(self, date: date):
		super().__init__(date=date)
		self.flow_name: str = "country"

		# Tables
		self.table_country: str = self.config.get("db_tables", {}).get("country", "tmdb_country")
		self.table_country_translation: str = self.config.get("db_tables", {}).get("country_translation", "tmdb_country_translation")

		# CSV file containing the missing countrys
		self.country: str = None
		self.country_translation: str = None


