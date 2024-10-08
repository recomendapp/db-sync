from datetime import date
from prefect import task
from ...models.config import Config
from ...models.csv_file import CSVFile
from ...utils.db import insert_into

class CollectionConfig(Config):
	def __init__(self, date: date):
		super().__init__(date=date)
		self.flow_name: str = "collection"

		# Tables
		self.table_collection: str = self.config.get("db_tables", {}).get("collection", "tmdb_collection")
		self.table_collection_translation: str = self.config.get("db_tables", {}).get("collection_translation", "tmdb_collection_translation")
		self.table_collection_image: str = self.config.get("db_tables", {}).get("collection_image", "tmdb_collection_image")

		# Ids
		self.extra_collections: set = {}
		self.missing_collections: set = {}

		# Columns
		self.collection_columns: list[str] = ["id", "name"]
		self.collection_translation_columns: list[str] = ["collection", "title", "overview", "homepage", "iso_639_1", "iso_3166_1"]
		self.collection_image_columns: list[str] = ["collection", "file_path", "type", "aspect_ratio", "height", "width", "vote_average", "vote_count", "iso_639_1"]

		# On conflict
		self.collection_on_conflict: list[str] = ["id"]
		self.collection_translation_on_conflict: list[str] = ["collection", "iso_639_1", "iso_3166_1"]
		self.collection_image_on_conflict: list[str] = ["collection", "file_path", "type"]

		# On conflict update
		self.collection_on_conflict_update: list[str] = [col for col in self.collection_columns if col not in self.collection_on_conflict]
		self.collection_translation_on_conflict_update: list[str] = [col for col in self.collection_translation_columns if col not in self.collection_translation_on_conflict]
		self.collection_image_on_conflict_update: list[str] = [col for col in self.collection_image_columns if col not in self.collection_image_on_conflict]

	@task
	def prune(self):
		"""Prune the extra collections from the database"""
		try:
			if len(self.extra_collections) > 0:
				with self.db_client.get_connection() as conn:
					with conn.cursor() as cursor:
						try:
							conn.autocommit = False
							cursor.execute(f"DELETE FROM {self.table_collection} WHERE id IN %s", (tuple(self.extra_collections),))
							conn.commit()
						except:
							conn.rollback()
							raise
						finally:
							conn.autocommit = True
		except Exception as e:
			raise ValueError(f"Failed to prune extra collections: {e}")
	
	@task
	def push(self, collection_csv: CSVFile, collection_translation_csv: CSVFile, collection_image_csv: CSVFile):
		"""Push the collections to the database"""
		try:
			# Clean duplicates from the CSV files
			collection_csv.clean_duplicates(conflict_columns=self.collection_on_conflict)
			collection_translation_csv.clean_duplicates(conflict_columns=self.collection_translation_on_conflict)
			collection_image_csv.clean_duplicates(conflict_columns=self.collection_image_on_conflict)

			with self.db_client.get_connection() as conn:
				with conn.cursor() as cursor:
					try:
						conn.autocommit = False
						cursor.execute(f"""
							CREATE TEMP TABLE temp_{self.table_collection} (LIKE {self.table_collection} INCLUDING ALL);
							CREATE TEMP TABLE temp_{self.table_collection_translation} (LIKE {self.table_collection_translation} INCLUDING ALL);
							CREATE TEMP TABLE temp_{self.table_collection_image} (LIKE {self.table_collection_image} INCLUDING ALL);
						""")

						with open(collection_csv.file_path, "r") as f:
							cursor.copy_expert(f"COPY temp_{self.table_collection} ({','.join(self.collection_columns)}) FROM STDIN WITH CSV HEADER", f)
						with open(collection_translation_csv.file_path, "r") as f:
							cursor.copy_expert(f"COPY temp_{self.table_collection_translation} ({','.join(self.collection_translation_columns)}) FROM STDIN WITH CSV HEADER", f)
						with open(collection_image_csv.file_path, "r") as f:
							cursor.copy_expert(f"COPY temp_{self.table_collection_image} ({','.join(self.collection_image_columns)}) FROM STDIN WITH CSV HEADER", f)

						# Insert collections
						insert_into(
							cursor=cursor,
							table=self.table_collection,
							temp_table=f"temp_{self.table_collection}",
							columns=self.collection_columns,
							on_conflict=self.collection_on_conflict,
							on_conflict_update=self.collection_on_conflict_update
						)

						# Insert translations
						insert_into(
							cursor=cursor,
							table=self.table_collection_translation,
							temp_table=f"temp_{self.table_collection_translation}",
							columns=self.collection_translation_columns,
							on_conflict=self.collection_translation_on_conflict,
							on_conflict_update=self.collection_translation_on_conflict_update
						)

						# Insert images
						insert_into(
							cursor=cursor,
							table=self.table_collection_image,
							temp_table=f"temp_{self.table_collection_image}",
							columns=self.collection_image_columns,
							on_conflict=self.collection_image_on_conflict,
							on_conflict_update=self.collection_image_on_conflict_update
						)

						# Delete outdated translations
						cursor.execute(f"""
							DELETE FROM {self.table_collection_translation}
							WHERE ({', '.join(self.collection_translation_on_conflict)}) NOT IN (
								SELECT {', '.join(self.collection_translation_on_conflict)}
								FROM temp_{self.table_collection_translation}
							)
							AND collection IN (
								SELECT id FROM temp_{self.table_collection}
							);
						""")

						# Delete outdated images
						cursor.execute(f"""
							DELETE FROM {self.table_collection_image}
							WHERE ({', '.join(self.collection_image_on_conflict)}) NOT IN (
								SELECT {', '.join(self.collection_image_on_conflict)}
								FROM temp_{self.table_collection_image}
							)
							AND collection IN (
								SELECT id FROM temp_{self.table_collection}
							);
						""")
						
						conn.commit()

						collection_csv.delete()
						collection_translation_csv.delete()
						collection_image_csv.delete()
					except:
						conn.rollback()
						raise
					finally:
						conn.autocommit = True
		except Exception as e:
			raise ValueError(f"Failed to push collections to the database: {e}")
