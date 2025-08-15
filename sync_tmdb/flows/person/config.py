from datetime import date
from prefect import task
import uuid
from ...models.config import Config
from ...models.csv_file import CSVFile
from ...utils.db import insert_into

class PersonConfig(Config):
	def __init__(self, date: date):
		super().__init__(date=date)
		self.flow_name: str = "person"

		# Tables
		self.table_person: str = self.config.get("db_tables", {}).get("person", "tmdb_person")
		self.table_person_translation: str = self.config.get("db_tables", {}).get("person_translation", "tmdb_person_translation")
		self.table_person_image: str = self.config.get("db_tables", {}).get("person_image", "tmdb_person_image")
		self.table_person_external_id: str = self.config.get("db_tables", {}).get("person_external_id", "tmdb_person_external_id")
		self.table_person_also_known_as: str = self.config.get("db_tables", {}).get("person_also_known_as", "tmdb_person_also_known_as")

		# Ids
		self.extra_persons: set = None
		self.missing_persons: set = None

		# Columns
		self.person_columns: list[str] = ["id", "adult", "birthday", "deathday", "gender", "homepage", "imdb_id", "known_for_department", "name", "place_of_birth", "popularity"]
		self.person_translation_columns: list[str] = ["person", "biography", "iso_639_1", "iso_3166_1"]
		self.person_image_columns: list[str] = ["person", "file_path", "aspect_ratio", "height", "width", "vote_average", "vote_count"]
		self.person_external_id_columns: list[str] = ["person", "source", "value"]
		self.person_also_known_as_columns: list[str] = ["person", "name"]

		# On conflict
		self.person_on_conflict: list[str] = ["id"]
		self.person_translation_on_conflict: list[str] = ["person", "iso_639_1", "iso_3166_1"]
		self.person_image_on_conflict: list[str] = ["person", "file_path"]
		self.person_external_id_on_conflict: list[str] = ["person", "source"]
		self.person_also_known_as_on_conflict: list[str] = ["person", "name"]

		# On conflict update
		self.person_on_conflict_update: list[str] = [col for col in self.person_columns if col not in self.person_on_conflict]
		self.person_translation_on_conflict_update: list[str] = [col for col in self.person_translation_columns if col not in self.person_translation_on_conflict]
		self.person_image_on_conflict_update: list[str] = [col for col in self.person_image_columns if col not in self.person_image_on_conflict]
		self.person_external_id_on_conflict_update: list[str] = [col for col in self.person_external_id_columns if col not in self.person_external_id_on_conflict]
		self.person_also_known_as_on_conflict_update: list[str] = [col for col in self.person_also_known_as_columns if col not in self.person_also_known_as_on_conflict]

	@task(cache_policy=None)
	def prune(self):
		"""Prune the extra persons from the database and Typesense"""
		# DB
		conn = self.db_client.get_connection()
		try:
			if len(self.extra_persons) > 0:
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
		finally:
			self.db_client.return_connection(conn)
		
		# Typesense
		try:
			self.logger.info(f"Pruning {len(self.extra_persons)} extra persons from Typesense...")
			self.typesense_client.delete_documents(
				collection="persons",
				ids=list(self.extra_persons)
			)
		except Exception as e:
			raise ValueError(f"Failed to prune extra persons from Typesense: {e}")


	@task(cache_policy=None)
	def push(self, person_csv: CSVFile, person_translation_csv: CSVFile, person_image_csv: CSVFile, person_external_id_csv: CSVFile, person_also_known_as_csv: CSVFile):
		"""Push the persons to the database"""
		conn = self.db_client.get_connection()
		try:
			# Clean duplicates from the CSV files
			person_csv.clean_duplicates(conflict_columns=self.person_on_conflict)
			person_translation_csv.clean_duplicates(conflict_columns=self.person_translation_on_conflict)
			person_image_csv.clean_duplicates(conflict_columns=self.person_image_on_conflict)
			person_external_id_csv.clean_duplicates(conflict_columns=self.person_external_id_on_conflict)
			person_also_known_as_csv.clean_duplicates(conflict_columns=self.person_also_known_as_on_conflict)

			with conn.cursor() as cursor:
				try:
					conn.autocommit = False
					temp_person = f"temp_{self.table_person}_{uuid.uuid4().hex}"
					temp_person_translation = f"temp_{self.table_person_translation}_{uuid.uuid4().hex}"
					temp_person_image = f"temp_{self.table_person_image}_{uuid.uuid4().hex}"
					temp_person_external_id = f"temp_{self.table_person_external_id}_{uuid.uuid4().hex}"
					temp_person_also_known_as = f"temp_{self.table_person_also_known_as}_{uuid.uuid4().hex}"

					cursor.execute(f"""
						CREATE TEMP TABLE {temp_person} (LIKE {self.table_person} INCLUDING ALL);
						CREATE TEMP TABLE {temp_person_translation} (LIKE {self.table_person_translation} INCLUDING ALL);
						CREATE TEMP TABLE {temp_person_image} (LIKE {self.table_person_image} INCLUDING ALL);
						CREATE TEMP TABLE {temp_person_external_id} (LIKE {self.table_person_external_id} INCLUDING ALL);
						CREATE TEMP TABLE {temp_person_also_known_as} (LIKE {self.table_person_also_known_as} INCLUDING ALL);
					""")

					with open(person_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_person} ({','.join(self.person_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(person_translation_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_person_translation} ({','.join(self.person_translation_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(person_image_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_person_image} ({','.join(self.person_image_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(person_external_id_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_person_external_id} ({','.join(self.person_external_id_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(person_also_known_as_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_person_also_known_as} ({','.join(self.person_also_known_as_columns)}) FROM STDIN WITH CSV HEADER", f)

					insert_into(
						cursor=cursor,
						table=self.table_person,
						temp_table=temp_person,
						columns=self.person_columns,
						on_conflict=self.person_on_conflict,
						on_conflict_update=self.person_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_person_translation,
						temp_table=temp_person_translation,
						columns=self.person_translation_columns,
						on_conflict=self.person_translation_on_conflict,
						on_conflict_update=self.person_translation_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_person_image,
						temp_table=temp_person_image,
						columns=self.person_image_columns,
						on_conflict=self.person_image_on_conflict,
						on_conflict_update=self.person_image_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_person_external_id,
						temp_table=temp_person_external_id,
						columns=self.person_external_id_columns,
						on_conflict=self.person_external_id_on_conflict,
						on_conflict_update=self.person_external_id_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_person_also_known_as,
						temp_table=temp_person_also_known_as,
						columns=self.person_also_known_as_columns,
						on_conflict=self.person_also_known_as_on_conflict,
						on_conflict_update=self.person_also_known_as_on_conflict_update
					)

					# Delete outdated  translations
					cursor.execute(f"""
						DELETE FROM {self.table_person_translation}
						WHERE ({','.join(self.person_translation_on_conflict)}) NOT IN (
							SELECT {','.join(self.person_translation_on_conflict)}
							FROM {temp_person_translation}
						)
						AND person IN (
							SELECT id FROM {temp_person}
						);
					""")

					# Delete outdated images
					cursor.execute(f"""
						DELETE FROM {self.table_person_image}
						WHERE ({','.join(self.person_image_on_conflict)}) NOT IN (
							SELECT {','.join(self.person_image_on_conflict)}
							FROM {temp_person_image}
						)
						AND person IN (
							SELECT id FROM {temp_person}
						);
					""")

					# Delete outdated external ids
					cursor.execute(f"""
						DELETE FROM {self.table_person_external_id}
						WHERE ({','.join(self.person_external_id_on_conflict)}) NOT IN (
							SELECT {','.join(self.person_external_id_on_conflict)}
							FROM {temp_person_external_id}
						)
						AND person IN (
							SELECT id FROM {temp_person}
						);
					""")

					# Delete outdated also known as
					cursor.execute(f"""
						DELETE FROM {self.table_person_also_known_as}
						WHERE ({','.join(self.person_also_known_as_on_conflict)}) NOT IN (
							SELECT {','.join(self.person_also_known_as_on_conflict)}
							FROM {temp_person_also_known_as}
						)
						AND person IN (
							SELECT id FROM {temp_person}
						);
					""")
				
					conn.commit()

					person_csv.delete()
					person_translation_csv.delete()
					person_image_csv.delete()
					person_external_id_csv.delete()
					person_also_known_as_csv.delete()
				except Exception as e:
					conn.rollback()
					raise
				finally:
					conn.autocommit = True
		except Exception as e:
			raise ValueError(f"Failed to push persons to the database: {e}")
		finally:
			self.db_client.return_connection(conn)
