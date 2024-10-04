from datetime import date
from ...models.config import Config
from ...models.csv_file import CSVFile

class CollectionConfig(Config):
	def __init__(self, date: date):
		super().__init__(date=date)
		self.flow_name: str = "collection"

		# Tables
		self.table_collection: str = self.config.get("db_tables", {}).get("collection", "tmdb_collection")
		self.table_collection_translation: str = self.config.get("db_tables", {}).get("collection_translation", "tmdb_collection_translation")
