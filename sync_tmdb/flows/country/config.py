from datetime import date
from ...models.config import Config

class CountryConfig(Config):
	def __init__(self, date: date):
		super().__init__(date=date)
		self.flow_name: str = "country"

		# Tables
		self.table_country: str = self.config.get("db_tables", {}).get("country", "tmdb_country")

		# Columns
		self.country_column: str = "iso_3166_1"

