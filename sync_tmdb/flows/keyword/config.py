from datetime import date
from ...models.config import Config

class KeywordConfig(Config):
	def __init__(self, date: date):
		super().__init__(date=date)
		self.flow_name: str = "keyword"

		# Tables
		self.table_keyword: str = self.config.get("db_tables", {}).get("keyword", "tmdb_keyword")

		# CSV file containing the missing keywords
		self.keyword: str = None


