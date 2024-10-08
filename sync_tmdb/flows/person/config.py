from datetime import date
from ...models.config import Config
from ...models.csv_file import CSVFile

class PersonConfig(Config):
	def __init__(self, date: date):
		super().__init__(date=date)
		self.flow_name: str = "person"

		# Tables
		self.table_person: str = self.config.get("db_tables", {}).get("person", "tmdb_person")
		self.table_person_translation: str = self.config.get("db_tables", {}).get("person_translation", "tmdb_person_translation")

		# Ids
		self.extra_persons: set = None
		self.missing_persons: set = None

		# Columns
		self.person_columns: list[str] = ["id", "adult", "also_known_as", "birthday", "deathday", "gender", "homepage", "imdb_id", "known_for_department", "name", "place_of_birth", "popularity", "profile_path"]
		self.person_translation_columns: list[str] = ["person", "biography", "language"]

		# On conflict
		self.person_on_conflict: list[str] = ["id"]
		self.person_translation_on_conflict: list[str] = ["person", "language"]

		# On conflict update
		self.person_on_conflict_update: list[str] = [col for col in self.person_columns if col not in self.person_on_conflict]
		self.person_translation_on_conflict_update: list[str] = [col for col in self.person_translation_columns if col not in self.person_translation_on_conflict]

		# CSV file
		self.person_csv: CSVFile = CSVFile(
			columns=self.person_columns,
			tmp_directory=self.tmp_directory,
			prefix=self.flow_name
		)
		self.person_translation_csv: CSVFile = CSVFile(
			columns=self.person_translation_columns,
			tmp_directory=self.tmp_directory,
			prefix=f"{self.flow_name}_translation"
		)
	
	def __enter__(self):
		return self
	
	def __exit__(self, exc_type, exc_value, traceback):
		pass
		# if self.person_csv:
		# 	self.person_csv.delete()
		# if self.person_translation_csv:
		# 	self.person_translation_csv.delete()

	def prune(self):
		"""Prune the extra persons from the database"""
		try:
			if len(self.extra_persons) > 0:
				with self.db_client.get_connection() as conn:
					with conn.cursor() as cursor:
						conn.autocommit = False
						try:
							cursor.execute(f"DELETE FROM {self.table_person} WHERE id IN %s", (tuple(self.extra_persons),))
							conn.commit()
						except:
							conn.rollback()
							raise
		except Exception as e:
			raise ValueError(f"Failed to prune extra persons: {e}")
	
	def push(self):
		"""Push the persons to the database"""
		try:
			with self.db_client.get_connection() as conn:
				with conn.cursor() as cursor:
					conn.autocommit = False
					cursor.execute(f"""
						CREATE TEMP TABLE temp_{self.bem} (LIKE {self.table_person} INCLUDING ALL);
						CREATE TEMP TABLE temp_{self.table_person_translation} (LIKE {self.table_person_translation} INCLUDING ALL);
					""")

					with open(self.person_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY temp_{self.table_person} ({','.join(self.person_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(self.person_translation_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY temp_{self.table_person_translation} ({','.join(self.person_translation_columns)}) FROM STDIN WITH CSV HEADER", f)

					cursor.execute(f"""
						INSERT INTO {self.table_person} ({','.join(self.person_columns)})
						SELECT {','.join(self.person_columns)} FROM temp_{self.table_person}
						ON CONFLICT ({','.join(self.person_on_conflict)}) DO UPDATE
						SET {','.join([f"{column}=EXCLUDED.{column}" for column in self.person_on_conflict_update])};
					""")

					cursor.execute(f"""
						INSERT INTO {self.table_person_translation} ({','.join(self.person_translation_columns)})
						SELECT {','.join(self.person_translation_columns)} FROM temp_{self.table_person_translation}
						ON CONFLICT ({','.join(self.person_translation_on_conflict)}) DO UPDATE
						SET {','.join([f"{column}=EXCLUDED.{column}" for column in self.person_translation_on_conflict_update])};
					""")
					
					conn.commit()
		except Exception as e:
			raise ValueError(f"Failed to push persons to the database: {e}")
