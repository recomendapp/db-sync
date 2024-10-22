from datetime import date
from prefect import task
import uuid
from ...models.config import Config
from ...models.csv_file import CSVFile
from ...utils.db import insert_into

class MovieConfig(Config):
	def __init__(self, date: date):
		super().__init__(date=date)
		self.flow_name: str = "movie"

		# Data
		self.db_languages: set = None
		self.db_countries: set = None
		self.db_genres: set = None
		self.db_keywords: set = None
		self.db_collections: set = None
		self.db_companies: set = None
		self.db_persons: set = None


		# Tables
		self.table_movie: str = self.config.get("db_tables", {}).get("movie", "tmdb_movie")
		self.table_movie_translation: str = self.config.get("db_tables", {}).get("movie_translation", "tmdb_movie_translation")
		self.table_movie_image: str = self.config.get("db_tables", {}).get("movie_image", "tmdb_movie_image")
		self.table_movie_video: str = self.config.get("db_tables", {}).get("movie_video", "tmdb_movie_video")
		self.table_movie_external_id: str = self.config.get("db_tables", {}).get("movie_external_id", "tmdb_movie_external_id")
		self.table_movie_keyword: str = self.config.get("db_tables", {}).get("movie_keyword", "tmdb_movie_keyword")
		self.table_movie_genre: str = self.config.get("db_tables", {}).get("movie_genre", "tmdb_movie_genre")
		self.table_movie_production: str = self.config.get("db_tables", {}).get("movie_production", "tmdb_movie_production")
		self.table_movie_language: str = self.config.get("db_tables", {}).get("movie_language", "tmdb_movie_language")
		self.table_movie_country: str = self.config.get("db_tables", {}).get("movie_country", "tmdb_movie_country")
		self.table_movie_credit: str = self.config.get("db_tables", {}).get("movie_credit", "tmdb_movie_credit")
		self.table_movie_role: str = self.config.get("db_tables", {}).get("movie_role", "tmdb_movie_role")
		

		self.table_language: str = self.config.get("db_tables", {}).get("language", "tmdb_language")
		self.table_country: str = self.config.get("db_tables", {}).get("country", "tmdb_country")
		self.table_genre: str = self.config.get("db_tables", {}).get("genre", "tmdb_genre")
		self.table_keyword: str = self.config.get("db_tables", {}).get("keyword", "tmdb_keyword")
		self.table_collection: str = self.config.get("db_tables", {}).get("collection", "tmdb_collection")
		self.table_company: str = self.config.get("db_tables", {}).get("company", "tmdb_company")
		self.table_person: str = self.config.get("db_tables", {}).get("person", "tmdb_person")

		# Ids
		self.extra_movies: set = None
		self.missing_movies: set = None

		# Columns
		# self.person_columns: list[str] = ["id", "adult", "birthday", "deathday", "gender", "homepage", "imdb_id", "known_for_department", "name", "place_of_birth", "popularity"]
		# self.person_translation_columns: list[str] = ["person", "biography", "iso_639_1", "iso_3166_1"]
		# self.person_image_columns: list[str] = ["person", "file_path", "aspect_ratio", "height", "width", "vote_average", "vote_count"]
		# self.person_external_id_columns: list[str] = ["person", "source", "value"]
		# self.person_also_known_as_columns: list[str] = ["person", "name"]

		# # On conflict
		# self.person_on_conflict: list[str] = ["id"]
		# self.person_translation_on_conflict: list[str] = ["person", "iso_639_1", "iso_3166_1"]
		# self.person_image_on_conflict: list[str] = ["person", "file_path"]
		# self.person_external_id_on_conflict: list[str] = ["person", "source"]
		# self.person_also_known_as_on_conflict: list[str] = ["person", "name"]

		# # On conflict update
		# self.person_on_conflict_update: list[str] = [col for col in self.person_columns if col not in self.person_on_conflict]
		# self.person_translation_on_conflict_update: list[str] = [col for col in self.person_translation_columns if col not in self.person_translation_on_conflict]
		# self.person_image_on_conflict_update: list[str] = [col for col in self.person_image_columns if col not in self.person_image_on_conflict]
		# self.person_external_id_on_conflict_update: list[str] = [col for col in self.person_external_id_columns if col not in self.person_external_id_on_conflict]
		# self.person_also_known_as_on_conflict_update: list[str] = [col for col in self.person_also_known_as_columns if col not in self.person_also_known_as_on_conflict]

	@task
	def get_db_data(self):
		"""Get the data from the database"""
		try:
			self.db_languages = self.db_client.get_table(table_name=self.table_language, columns=["iso_639_1"])
			self.db_countries = self.db_client.get_table(table_name=self.table_country, columns=["iso_3166_1"])
			self.db_genres = self.db_client.get_table(table_name=self.table_genre, columns=["id"])
			self.db_keywords = self.db_client.get_table(table_name=self.table_keyword, columns=["id"])
			self.db_collections = self.db_client.get_table(table_name=self.table_collection, columns=["id"])
			self.db_companies = self.db_client.get_table(table_name=self.table_company, columns=["id"])
			self.db_persons = self.db_client.get_table(table_name=self.table_person, columns=["id"])
		except Exception as e:
			raise ValueError(f"Failed to get the data from the database: {e}")
	@task
	def prune(self):
		"""Prune the extra movies from the database"""
		conn = self.db_client.get_connection()
		try:
			if len(self.extra_movies) > 0:
				with conn.cursor() as cursor:
					conn.autocommit = False
					try:
						cursor.execute(f"DELETE FROM {self.table_movie} WHERE id IN %s", (tuple(self.extra_movies),))
						conn.commit()
					except:
						conn.rollback()
						raise
		except Exception as e:
			raise ValueError(f"Failed to prune extra movies: {e}")
		finally:
			self.db_client.return_connection(conn)

	@task
	def push(self, csv: dict[str, CSVFile]):
		"""Push the persons to the database"""
		conn = self.db_client.get_connection()
		try:
			csv["movie"].clean_duplicates(conflict_columns=self.movie_on_conflict)

			# Clean duplicates from the CSV files
			# person_csv.clean_duplicates(conflict_columns=self.person_on_conflict)
			# person_translation_csv.clean_duplicates(conflict_columns=self.person_translation_on_conflict)
			# person_image_csv.clean_duplicates(conflict_columns=self.person_image_on_conflict)
			# person_external_id_csv.clean_duplicates(conflict_columns=self.person_external_id_on_conflict)
			# person_also_known_as_csv.clean_duplicates(conflict_columns=self.person_also_known_as_on_conflict)

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
