import pandas as pd
from prefect import task
from ...models.csv_file import CSVFile
from .config import CollectionConfig as Config

class Mapper:
	# Columns
	collection_columns: list[str] = ["id", "backdrop_path"]
	collection_translation_columns: list[str] = ["collection", "overview", "poster_path", "name", "language"]

	# On conflict
	collection_on_conflict: list[str] = ["id"]
	collection_on_conflict_update: list[str] = ["backdrop_path"]

	collection_translation_on_conflict: list[str] = ["collection", "language"]
	collection_translation_on_conflict_update: list[str] = ["overview", "poster_path", "name"]


	@staticmethod
	def collection(config: Config, collection: dict) -> pd.DataFrame:
		collection_data = [
			{
				"id": collection[config.default_language.code]["id"],
				"backdrop_path": collection[config.default_language.code]["backdrop_path"]
			}
		]
		return pd.DataFrame(collection_data)

	@staticmethod
	def collection_translation(collection: dict) -> pd.DataFrame:
		collection_translation_data = [
			{
				"collection": collection[language]["id"],
				"overview": collection[language]["overview"],
				"poster_path": collection[language]["poster_path"],
				"name": collection[language]["name"],
				"language": language
			}
			for language in collection.keys()
		]

		return pd.DataFrame(collection_translation_data)
	
	@staticmethod
	@task
	def push(config: Config, collection_csv: CSVFile, collection_translation_csv: CSVFile):
		try:
			with config.db_client.get_connection() as conn:
				with conn.cursor() as cursor:
					conn.autocommit = False
					cursor.execute(f"""
						CREATE TEMP TABLE temp_{config.table_collection} (LIKE {config.table_collection} INCLUDING ALL);
						CREATE TEMP TABLE temp_{config.table_collection_translation} (LIKE {config.table_collection_translation} INCLUDING ALL);
					""")

					with open(collection_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY temp_{config.table_collection} ({','.join(Mapper.collection_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(collection_translation_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY temp_{config.table_collection_translation} ({','.join(Mapper.collection_translation_columns)}) FROM STDIN WITH CSV HEADER", f)

					cursor.execute(f"""
						INSERT INTO {config.table_collection} ({','.join(Mapper.collection_columns)})
						SELECT {','.join(Mapper.collection_columns)} FROM temp_{config.table_collection}
						ON CONFLICT ({','.join(Mapper.collection_on_conflict)}) DO UPDATE
						SET {','.join([f"{column}=EXCLUDED.{column}" for column in Mapper.collection_on_conflict_update])};
					""")

					cursor.execute(f"""
						INSERT INTO {config.table_collection_translation} ({','.join(Mapper.collection_translation_columns)})
						SELECT {','.join(Mapper.collection_translation_columns)} FROM temp_{config.table_collection_translation}
						ON CONFLICT ({','.join(Mapper.collection_translation_on_conflict)}) DO UPDATE
						SET {','.join([f"{column}=EXCLUDED.{column}" for column in Mapper.collection_translation_on_conflict_update])};
					""")
					
					conn.commit()

					collection_csv.delete()
					collection_translation_csv.delete()
		except Exception as e:
			raise ValueError(f"Failed to push collections to the database: {e}")	
		
	