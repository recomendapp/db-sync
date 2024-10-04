from datetime import date
from ...models.config import Config
from ...models.csv_file import CSVFile

class CompanyConfig(Config):
	def __init__(self, date: date):
		super().__init__(date=date)
		self.flow_name: str = "company"

		# Tables
		self.table_company: str = self.config.get("db_tables", {}).get("company", "tmdb_company")