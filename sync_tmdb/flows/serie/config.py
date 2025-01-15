from datetime import date
from prefect import task
import uuid
from ...models.config import Config
from ...models.csv_file import CSVFile
from ...utils.db import insert_into

class SerieConfig(Config):
	def __init__(self, date: date):
		super().__init__(date=date)
		self.flow_name: str = "serie"

		# Data
		self.db_languages: set = None
		self.db_countries: set = None
		self.db_genres: set = None
		self.db_keywords: set = None
		self.db_collections: set = None
		self.db_companies: set = None
		self.db_persons: set = None


		# Tables
		self.table_serie: str = self.config.get("db_tables", {}).get("serie", "tmdb_tv_series")
		self.table_serie_alternative_titles: str = self.config.get("db_tables", {}).get("serie_alternative_titles", "tmdb_tv_series_alternative_titles")
		self.table_serie_content_ratings: str = self.config.get("db_tables", {}).get("serie_content_ratings", "tmdb_tv_series_content_ratings")
		self.table_serie_external_ids: str = self.config.get("db_tables", {}).get("serie_external_ids", "tmdb_tv_series_external_ids")
		self.table_serie_genres: str = self.config.get("db_tables", {}).get("serie_genres", "tmdb_tv_series_genres")
		self.table_serie_images: str = self.config.get("db_tables", {}).get("serie_images", "tmdb_tv_series_images")
		self.table_serie_keywords: str = self.config.get("db_tables", {}).get("serie_keywords", "tmdb_tv_series_keywords")
		self.table_serie_languages: str = self.config.get("db_tables", {}).get("serie_languages", "tmdb_tv_series_languages")
		self.table_serie_networks: str = self.config.get("db_tables", {}).get("serie_networks", "tmdb_tv_series_networks")
		self.table_serie_origin_country: str = self.config.get("db_tables", {}).get("serie_origin_country", "tmdb_tv_series_origin_country")
		self.table_serie_production_companies: str = self.config.get("db_tables", {}).get("serie_production_companies", "tmdb_tv_series_production_companies")
		self.table_serie_production_countries: str = self.config.get("db_tables", {}).get("serie_production_countries", "tmdb_tv_series_production_countries")
		self.table_serie_spoken_languages: str = self.config.get("db_tables", {}).get("serie_spoken_languages", "tmdb_tv_series_spoken_languages")
		self.table_serie_translations: str = self.config.get("db_tables", {}).get("serie_translations", "tmdb_tv_series_translations")
		self.table_serie_videos: str = self.config.get("db_tables", {}).get("serie_videos", "tmdb_tv_series_videos")
		self.table_serie_credits: str = self.config.get("db_tables", {}).get("serie_credits", "tmdb_tv_series_credits")
		
		# Seasons
		self.table_serie_season: str = self.config.get("db_tables", {}).get("serie_season", "tmdb_tv_series_seasons")


		self.table_language: str = self.config.get("db_tables", {}).get("language", "tmdb_language")
		self.table_country: str = self.config.get("db_tables", {}).get("country", "tmdb_country")
		self.table_genre: str = self.config.get("db_tables", {}).get("genre", "tmdb_genre")
		self.table_keyword: str = self.config.get("db_tables", {}).get("keyword", "tmdb_keyword")
		self.table_network: str = self.config.get("db_tables", {}).get("network", "tmdb_network")
		self.table_company: str = self.config.get("db_tables", {}).get("company", "tmdb_company")
		self.table_person: str = self.config.get("db_tables", {}).get("person", "tmdb_person")

		# Ids
		self.extra_series: set = None
		self.missing_series: set = None

		# Columns
		self.serie_columns: list[str] = ["id", "adult", "in_production", "original_language", "original_name", "popularity", "status", "type", "vote_average", "vote_count"]
		self.serie_alternative_titles_columns: list[str] = ["serie_id", "iso_3166_1", "title", "type"]
		self.serie_content_ratings_columns: list[str] = ["serie_id", "iso_3166_1", "rating", "descriptors"]
		self.serie_external_ids_columns: list[str] = ["serie_id", "source", "value"]
		self.serie_genres_columns: list[str] = ["serie_id", "genre_id"]
		self.serie_images_columns: list[str] = ["serie_id", "file_path", "type", "aspect_ratio", "height", "width", "vote_average", "vote_count", "iso_639_1"]
		self.serie_keywords_columns: list[str] = ["serie_id", "keyword_id"]
		self.serie_languages_columns: list[str] = ["serie_id", "iso_639_1"]
		self.serie_networks_columns: list[str] = ["serie_id", "network_id"]
		self.serie_origin_country_columns: list[str] = ["serie_id", "iso_3166_1"]
		self.serie_production_companies_columns: list[str] = ["serie_id", "company_id"]
		self.serie_production_countries_columns: list[str] = ["serie_id", "iso_3166_1"]
		self.serie_spoken_languages_columns: list[str] = ["serie_id", "iso_639_1"]
		self.serie_translations_columns: list[str] = ["serie_id", "name", "overview", "homepage", "tagline", "iso_639_1", "iso_3166_1"]
		self.serie_videos_columns: list[str] = ["id", "serie_id", "iso_639_1", "iso_3166_1", "name", "key", "site", "size", "type", "official", "published_at"]
		self.serie_credits_columns: list[str] = ["id", "serie_id", "person_id", "department", "job", "character"]

		# On conflict
		self.serie_on_conflict: list[str] = ["id"]
		self.serie_alternative_titles_on_conflict: list[str] = ["serie_id", "iso_3166_1", "title", "type"]
		self.serie_content_ratings_on_conflict: list[str] = ["serie_id", "iso_3166_1", "rating"]
		self.serie_external_ids_on_conflict: list[str] = ["serie_id", "source"]
		self.serie_genres_on_conflict: list[str] = ["serie_id", "genre_id"]
		self.serie_images_on_conflict: list[str] = ["serie_id", "file_path", "type"]
		self.serie_keywords_on_conflict: list[str] = ["serie_id", "keyword_id"]
		self.serie_languages_on_conflict: list[str] = ["serie_id", "iso_639_1"]
		self.serie_networks_on_conflict: list[str] = ["serie_id", "network_id"]
		self.serie_origin_country_on_conflict: list[str] = ["serie_id", "iso_3166_1"]
		self.serie_production_companies_on_conflict: list[str] = ["serie_id", "company_id"]
		self.serie_production_countries_on_conflict: list[str] = ["serie_id", "iso_3166_1"]
		self.serie_spoken_languages_on_conflict: list[str] = ["serie_id", "iso_639_1"]
		self.serie_translations_on_conflict: list[str] = ["serie_id", "iso_639_1", "iso_3166_1"]
		self.serie_videos_on_conflict: list[str] = ["id"]
		self.serie_credits_on_conflict: list[str] = ["credit_id"]

		# On conflict update
		self.serie_on_conflict_update: list[str] = [col for col in self.serie_columns if col not in self.serie_on_conflict]
		self.serie_alternative_titles_on_conflict_update: list[str] = [col for col in self.serie_alternative_titles_columns if col not in self.serie_alternative_titles_on_conflict]
		self.serie_content_ratings_on_conflict_update: list[str] = [col for col in self.serie_content_ratings_columns if col not in self.serie_content_ratings_on_conflict]
		self.serie_external_ids_on_conflict_update: list[str] = [col for col in self.serie_external_ids_columns if col not in self.serie_external_ids_on_conflict]
		self.serie_genres_on_conflict_update: list[str] = [col for col in self.serie_genres_columns if col not in self.serie_genres_on_conflict]
		self.serie_images_on_conflict_update: list[str] = [col for col in self.serie_images_columns if col not in self.serie_images_on_conflict]
		self.serie_keywords_on_conflict_update: list[str] = [col for col in self.serie_keywords_columns if col not in self.serie_keywords_on_conflict]
		self.serie_languages_on_conflict_update: list[str] = [col for col in self.serie_languages_columns if col not in self.serie_languages_on_conflict]
		self.serie_networks_on_conflict_update: list[str] = [col for col in self.serie_networks_columns if col not in self.serie_networks_on_conflict]
		self.serie_origin_country_on_conflict_update: list[str] = [col for col in self.serie_origin_country_columns if col not in self.serie_origin_country_on_conflict]
		self.serie_production_companies_on_conflict_update: list[str] = [col for col in self.serie_production_companies_columns if col not in self.serie_production_companies_on_conflict]
		self.serie_production_countries_on_conflict_update: list[str] = [col for col in self.serie_production_countries_columns if col not in self.serie_production_countries_on_conflict]
		self.serie_spoken_languages_on_conflict_update: list[str] = [col for col in self.serie_spoken_languages_columns if col not in self.serie_spoken_languages_on_conflict]
		self.serie_translations_on_conflict_update: list[str] = [col for col in self.serie_translations_columns if col not in self.serie_translations_on_conflict]
		self.serie_videos_on_conflict_update: list[str] = [col for col in self.serie_videos_columns if col not in self.serie_videos_on_conflict]
		self.serie_credits_on_conflict_update: list[str] = [col for col in self.serie_credits_columns if col not in self.serie_credits_on_conflict]
	@task
	def get_db_data(self):
		"""Get the data from the database"""
		try:
			db_languages = self.db_client.get_table(table_name=self.table_language, columns=["iso_639_1"])
			self.db_languages = set([item[0] for item in db_languages])
			db_countries = self.db_client.get_table(table_name=self.table_country, columns=["iso_3166_1"])
			self.db_countries = set([item[0] for item in db_countries])
			db_genres = self.db_client.get_table(table_name=self.table_genre, columns=["id"])
			self.db_genres = set([item[0] for item in db_genres])
			db_keywords = self.db_client.get_table(table_name=self.table_keyword, columns=["id"])
			self.db_keywords = set([item[0] for item in db_keywords])
			db_networks = self.db_client.get_table(table_name=self.table_network, columns=["id"])
			self.db_networks = set([item[0] for item in db_networks])
			db_companies = self.db_client.get_table(table_name=self.table_company, columns=["id"])
			self.db_companies = set([item[0] for item in db_companies])
			db_persons = self.db_client.get_table(table_name=self.table_person, columns=["id"])
			self.db_persons = set([item[0] for item in db_persons])
		except Exception as e:
			raise ValueError(f"Failed to get the data from the database: {e}")
	@task
	def prune(self):
		"""Prune the extra series from the database"""
		conn = self.db_client.get_connection()
		try:
			if len(self.extra_series) > 0:
				with conn.cursor() as cursor:
					conn.autocommit = False
					try:
						cursor.execute(f"DELETE FROM {self.table_serie} WHERE id IN %s", (tuple(self.extra_series),))
						conn.commit()
					except:
						conn.rollback()
						raise
		except Exception as e:
			raise ValueError(f"Failed to prune extra series: {e}")
		finally:
			self.db_client.return_connection(conn)

	@task
	def push(self, csv: dict[str, CSVFile]):
		"""Push the series to the database"""
		conn = self.db_client.get_connection()
		self.logger.info(f"Connections: {self.db_client.nb_open_connections} open, {self.db_client.nb_close_connections} close")
		try:
			# Clean duplicates from the CSV files
			# csv["movie"].clean_duplicates(conflict_columns=self.movie_on_conflict)
			# csv["movie_alternative_titles"].clean_duplicates(conflict_columns=self.movie_alternative_titles_on_conflict)
			# csv["movie_credits"].clean_duplicates(conflict_columns=self.movie_credits_on_conflict)
			# csv["movie_external_ids"].clean_duplicates(conflict_columns=self.movie_external_ids_on_conflict)
			# csv["movie_genres"].clean_duplicates(conflict_columns=self.movie_genres_on_conflict)
			# csv["movie_images"].clean_duplicates(conflict_columns=self.movie_images_on_conflict)
			# csv["movie_keywords"].clean_duplicates(conflict_columns=self.movie_keywords_on_conflict)
			# csv["movie_origin_country"].clean_duplicates(conflict_columns=self.movie_origin_country_on_conflict)
			# csv["movie_production_companies"].clean_duplicates(conflict_columns=self.movie_production_companies_on_conflict)
			# csv["movie_production_countries"].clean_duplicates(conflict_columns=self.movie_production_countries_on_conflict)
			# csv["movie_release_dates"].clean_duplicates(conflict_columns=self.movie_release_dates_on_conflict)
			# csv["movie_roles"].clean_duplicates(conflict_columns=self.movie_roles_on_conflict)
			# csv["movie_spoken_languages"].clean_duplicates(conflict_columns=self.movie_spoken_languages_on_conflict)
			# csv["movie_translations"].clean_duplicates(conflict_columns=self.movie_translations_on_conflict)
			# csv["movie_videos"].clean_duplicates(conflict_columns=self.movie_videos_on_conflict)

			with conn.cursor() as cursor:
				try:
					conn.autocommit = False

					# temp_movie = f"temp_{self.table_movie}_{uuid.uuid4().hex}"
					# temp_movie_alternative_titles = f"temp_{self.table_movie_alternative_titles}_{uuid.uuid4().hex}"
					# temp_movie_credits = f"temp_{self.table_movie_credits}_{uuid.uuid4().hex}"
					# temp_movie_external_ids = f"temp_{self.table_movie_external_ids}_{uuid.uuid4().hex}"
					# temp_movie_genres = f"temp_{self.table_movie_genres}_{uuid.uuid4().hex}"
					# temp_movie_images = f"temp_{self.table_movie_images}_{uuid.uuid4().hex}"
					# temp_movie_keywords = f"temp_{self.table_movie_keywords}_{uuid.uuid4().hex}"
					# temp_movie_origin_country = f"temp_{self.table_movie_origin_country}_{uuid.uuid4().hex}"
					# temp_movie_production_companies = f"temp_{self.table_movie_production_companies}_{uuid.uuid4().hex}"
					# temp_movie_production_countries = f"temp_{self.table_movie_production_countries}_{uuid.uuid4().hex}"
					# temp_movie_release_dates = f"temp_{self.table_movie_release_dates}_{uuid.uuid4().hex}"
					# temp_movie_roles = f"temp_{self.table_movie_roles}_{uuid.uuid4().hex}"
					# temp_movie_spoken_languages = f"temp_{self.table_movie_spoken_languages}_{uuid.uuid4().hex}"
					# temp_movie_translations = f"temp_{self.table_movie_translations}_{uuid.uuid4().hex}"
					# temp_movie_videos = f"temp_{self.table_movie_videos}_{uuid.uuid4().hex}"

					# cursor.execute(f"""
					# 	CREATE TEMP TABLE {temp_movie} (LIKE {self.table_movie} INCLUDING ALL);
					# 	CREATE TEMP TABLE {temp_movie_alternative_titles} (LIKE {self.table_movie_alternative_titles} INCLUDING ALL);
					# 	CREATE TEMP TABLE {temp_movie_credits} (LIKE {self.table_movie_credits} INCLUDING ALL);
					# 	CREATE TEMP TABLE {temp_movie_external_ids} (LIKE {self.table_movie_external_ids} INCLUDING ALL);
					# 	CREATE TEMP TABLE {temp_movie_genres} (LIKE {self.table_movie_genres} INCLUDING ALL);
					# 	CREATE TEMP TABLE {temp_movie_images} (LIKE {self.table_movie_images} INCLUDING ALL);
					# 	CREATE TEMP TABLE {temp_movie_keywords} (LIKE {self.table_movie_keywords} INCLUDING ALL);
					# 	CREATE TEMP TABLE {temp_movie_origin_country} (LIKE {self.table_movie_origin_country} INCLUDING ALL);
					# 	CREATE TEMP TABLE {temp_movie_production_companies} (LIKE {self.table_movie_production_companies} INCLUDING ALL);
					# 	CREATE TEMP TABLE {temp_movie_production_countries} (LIKE {self.table_movie_production_countries} INCLUDING ALL);
					# 	CREATE TEMP TABLE {temp_movie_release_dates} (LIKE {self.table_movie_release_dates} INCLUDING ALL);
					# 	CREATE TEMP TABLE {temp_movie_roles} (LIKE {self.table_movie_roles} INCLUDING ALL);
					# 	CREATE TEMP TABLE {temp_movie_spoken_languages} (LIKE {self.table_movie_spoken_languages} INCLUDING ALL);
					# 	CREATE TEMP TABLE {temp_movie_translations} (LIKE {self.table_movie_translations} INCLUDING ALL);
					# 	CREATE TEMP TABLE {temp_movie_videos} (LIKE {self.table_movie_videos} INCLUDING ALL);
					# """)

					# with open(csv["movie"].file_path, "r") as f:
					# 	cursor.copy_expert(f"COPY {temp_movie} ({','.join(self.movie_columns)}) FROM STDIN WITH CSV HEADER", f)
					# with open(csv["movie_alternative_titles"].file_path, "r") as f:
					# 	cursor.copy_expert(f"COPY {temp_movie_alternative_titles} ({','.join(self.movie_alternative_titles_columns)}) FROM STDIN WITH CSV HEADER", f)
					# with open(csv["movie_credits"].file_path, "r") as f:
					# 	cursor.copy_expert(f"COPY {temp_movie_credits} ({','.join(self.movie_credits_columns)}) FROM STDIN WITH CSV HEADER", f)
					# with open(csv["movie_external_ids"].file_path, "r") as f:
					# 	cursor.copy_expert(f"COPY {temp_movie_external_ids} ({','.join(self.movie_external_ids_columns)}) FROM STDIN WITH CSV HEADER", f)
					# with open(csv["movie_genres"].file_path, "r") as f:
					# 	cursor.copy_expert(f"COPY {temp_movie_genres} ({','.join(self.movie_genres_columns)}) FROM STDIN WITH CSV HEADER", f)
					# with open(csv["movie_images"].file_path, "r") as f:
					# 	cursor.copy_expert(f"COPY {temp_movie_images} ({','.join(self.movie_images_columns)}) FROM STDIN WITH CSV HEADER", f)
					# with open(csv["movie_keywords"].file_path, "r") as f:
					# 	cursor.copy_expert(f"COPY {temp_movie_keywords} ({','.join(self.movie_keywords_columns)}) FROM STDIN WITH CSV HEADER", f)
					# with open(csv["movie_origin_country"].file_path, "r") as f:
					# 	cursor.copy_expert(f"COPY {temp_movie_origin_country} ({','.join(self.movie_origin_country_columns)}) FROM STDIN WITH CSV HEADER", f)
					# with open(csv["movie_production_companies"].file_path, "r") as f:
					# 	cursor.copy_expert(f"COPY {temp_movie_production_companies} ({','.join(self.movie_production_companies_columns)}) FROM STDIN WITH CSV HEADER", f)
					# with open(csv["movie_production_countries"].file_path, "r") as f:
					# 	cursor.copy_expert(f"COPY {temp_movie_production_countries} ({','.join(self.movie_production_countries_columns)}) FROM STDIN WITH CSV HEADER", f)
					# with open(csv["movie_release_dates"].file_path, "r") as f:
					# 	cursor.copy_expert(f"COPY {temp_movie_release_dates} ({','.join(self.movie_release_dates_columns)}) FROM STDIN WITH CSV HEADER", f)
					# with open(csv["movie_roles"].file_path, "r") as f:
					# 	cursor.copy_expert(f"COPY {temp_movie_roles} ({','.join(self.movie_roles_columns)}) FROM STDIN WITH CSV HEADER", f)
					# with open(csv["movie_spoken_languages"].file_path, "r") as f:
					# 	cursor.copy_expert(f"COPY {temp_movie_spoken_languages} ({','.join(self.movie_spoken_languages_columns)}) FROM STDIN WITH CSV HEADER", f)
					# with open(csv["movie_translations"].file_path, "r") as f:
					# 	cursor.copy_expert(f"COPY {temp_movie_translations} ({','.join(self.movie_translations_columns)}) FROM STDIN WITH CSV HEADER", f)
					# with open(csv["movie_videos"].file_path, "r") as f:
					# 	cursor.copy_expert(f"COPY {temp_movie_videos} ({','.join(self.movie_videos_columns)}) FROM STDIN WITH CSV HEADER", f)

					# # Delete all outdated alternative titles before inserting
					# cursor.execute(f"""
					# 	DELETE FROM {self.table_movie_alternative_titles}
					# 	WHERE {self.movie_alternative_titles_columns[0]} IN (
					# 		SELECT id FROM {temp_movie}
					# 	);
					# """)

					# # Delete all outdated credits before inserting
					# cursor.execute(f"""
					# 	DELETE FROM {self.table_movie_credits}
					# 	WHERE {self.movie_credits_columns[1]} IN (
					# 		SELECT id FROM {temp_movie}
					# 	);
					# """)
					# # No need to delete roles because is one-to-one relationship with credits

					# # Delete all outdated external ids before inserting
					# cursor.execute(f"""
					# 	DELETE FROM {self.table_movie_external_ids}
					# 	WHERE {self.movie_external_ids_columns[0]} IN (
					# 		SELECT id FROM {temp_movie}
					# 	);
					# """)

					# # Delete all outdated genres before inserting
					# cursor.execute(f"""	
					# 	DELETE FROM {self.table_movie_genres}
					# 	WHERE {self.movie_genres_columns[0]} IN (
					# 		SELECT id FROM {temp_movie}
					# 	);
					# """)

					# # Delete all outdated images before inserting
					# cursor.execute(f"""
					# 	DELETE FROM {self.table_movie_images}
					# 	WHERE {self.movie_images_columns[0]} IN (
					# 		SELECT id FROM {temp_movie}
					# 	);
					# """)

					# # Delete all outdated keywords before inserting
					# cursor.execute(f"""
					# 	DELETE FROM {self.table_movie_keywords}
					# 	WHERE {self.movie_keywords_columns[0]} IN (
					# 		SELECT id FROM {temp_movie}
					# 	);
					# """)

					# # Delete all outdated origin countries before inserting
					# cursor.execute(f"""
					# 	DELETE FROM {self.table_movie_origin_country}
					# 	WHERE {self.movie_origin_country_columns[0]} IN (
					# 		SELECT id FROM {temp_movie}
					# 	);
					# """)

					# # Delete all outdated production companies before inserting
					# cursor.execute(f"""
					# 	DELETE FROM {self.table_movie_production_companies}
					# 	WHERE {self.movie_production_companies_columns[0]} IN (
					# 		SELECT id FROM {temp_movie}
					# 	);
					# """)

					# # Delete all outdated production countries before inserting
					# cursor.execute(f"""
					# 	DELETE FROM {self.table_movie_production_countries}
					# 	WHERE {self.movie_production_countries_columns[0]} IN (
					# 		SELECT id FROM {temp_movie}
					# 	);
					# """)

					# # Delete all outdated release dates before inserting
					# cursor.execute(f"""
					# 	DELETE FROM {self.table_movie_release_dates}
					# 	WHERE {self.movie_release_dates_columns[0]} IN (
					# 		SELECT id FROM {temp_movie}
					# 	);
					# """)

					# # Delete all outdated spoken languages before inserting
					# cursor.execute(f"""
					# 	DELETE FROM {self.table_movie_spoken_languages}
					# 	WHERE {self.movie_spoken_languages_columns[0]} IN (
					# 		SELECT id FROM {temp_movie}
					# 	);
					# """)

					# # Delete all outdated translations before inserting
					# cursor.execute(f"""
					# 	DELETE FROM {self.table_movie_translations}
					# 	WHERE {self.movie_translations_columns[0]} IN (
					# 		SELECT id FROM {temp_movie}
					# 	);
					# """)

					# # Delete all outdated videos before inserting
					# cursor.execute(f"""
					# 	DELETE FROM {self.table_movie_videos}
					# 	WHERE {self.movie_videos_columns[1]} IN (
					# 		SELECT id FROM {temp_movie}
					# 	);
					# """)

					# insert_into(
					# 	cursor=cursor,
					# 	table=self.table_movie,
					# 	temp_table=temp_movie,
					# 	columns=self.movie_columns,
					# 	on_conflict=self.movie_on_conflict,
					# 	on_conflict_update=self.movie_on_conflict_update
					# )

					# # Here we disable the on conflict to do nothing
					# insert_into(
					# 	cursor=cursor,
					# 	table=self.table_movie_alternative_titles,
					# 	temp_table=temp_movie_alternative_titles,
					# 	columns=self.movie_alternative_titles_columns,
					# 	# on_conflict=[],
					# 	# on_conflict_update=[]
					# )

					# insert_into(
					# 	cursor=cursor,
					# 	table=self.table_movie_credits,
					# 	temp_table=temp_movie_credits,
					# 	columns=self.movie_credits_columns,
					# 	# on_conflict=self.movie_credits_on_conflict,
					# 	# on_conflict_update=self.movie_credits_on_conflict_update
					# )

					# insert_into(
					# 	cursor=cursor,
					# 	table=self.table_movie_external_ids,
					# 	temp_table=temp_movie_external_ids,
					# 	columns=self.movie_external_ids_columns,
					# 	# on_conflict=self.movie_external_ids_on_conflict,
					# 	# on_conflict_update=self.movie_external_ids_on_conflict_update
					# )

					# insert_into(
					# 	cursor=cursor,
					# 	table=self.table_movie_genres,
					# 	temp_table=temp_movie_genres,
					# 	columns=self.movie_genres_columns,
					# 	# on_conflict=self.movie_genres_on_conflict,
					# 	# on_conflict_update=self.movie_genres_on_conflict_update
					# )

					# insert_into(
					# 	cursor=cursor,
					# 	table=self.table_movie_images,
					# 	temp_table=temp_movie_images,
					# 	columns=self.movie_images_columns,
					# 	# on_conflict=self.movie_images_on_conflict,
					# 	# on_conflict_update=self.movie_images_on_conflict_update
					# )

					# insert_into(
					# 	cursor=cursor,
					# 	table=self.table_movie_keywords,
					# 	temp_table=temp_movie_keywords,
					# 	columns=self.movie_keywords_columns,
					# 	# on_conflict=self.movie_keywords_on_conflict,
					# 	# on_conflict_update=self.movie_keywords_on_conflict_update
					# )

					# insert_into(
					# 	cursor=cursor,
					# 	table=self.table_movie_origin_country,
					# 	temp_table=temp_movie_origin_country,
					# 	columns=self.movie_origin_country_columns,
					# 	# on_conflict=self.movie_origin_country_on_conflict,
					# 	# on_conflict_update=self.movie_origin_country_on_conflict_update
					# )

					# insert_into(
					# 	cursor=cursor,
					# 	table=self.table_movie_production_companies,
					# 	temp_table=temp_movie_production_companies,
					# 	columns=self.movie_production_companies_columns,
					# 	# on_conflict=self.movie_production_companies_on_conflict,
					# 	# on_conflict_update=self.movie_production_companies_on_conflict_update
					# )

					# insert_into(
					# 	cursor=cursor,
					# 	table=self.table_movie_production_countries,
					# 	temp_table=temp_movie_production_countries,
					# 	columns=self.movie_production_countries_columns,
					# 	# on_conflict=self.movie_production_countries_on_conflict,
					# 	# on_conflict_update=self.movie_production_countries_on_conflict_update
					# )

					# insert_into(
					# 	cursor=cursor,
					# 	table=self.table_movie_release_dates,
					# 	temp_table=temp_movie_release_dates,
					# 	columns=self.movie_release_dates_columns,
					# 	# on_conflict=self.movie_release_dates_on_conflict,
					# 	# on_conflict_update=self.movie_release_dates_on_conflict_update
					# )

					# insert_into(
					# 	cursor=cursor,
					# 	table=self.table_movie_roles,
					# 	temp_table=temp_movie_roles,
					# 	columns=self.movie_roles_columns,
					# 	# on_conflict=self.movie_roles_on_conflict,
					# 	# on_conflict_update=self.movie_roles_on_conflict_update
					# )

					# insert_into(
					# 	cursor=cursor,
					# 	table=self.table_movie_spoken_languages,
					# 	temp_table=temp_movie_spoken_languages,
					# 	columns=self.movie_spoken_languages_columns,
					# 	# on_conflict=self.movie_spoken_languages_on_conflict,
					# 	# on_conflict_update=self.movie_spoken_languages_on_conflict_update
					# )

					# insert_into(
					# 	cursor=cursor,
					# 	table=self.table_movie_translations,
					# 	temp_table=temp_movie_translations,
					# 	columns=self.movie_translations_columns,
					# 	# on_conflict=self.movie_translations_on_conflict,
					# 	# on_conflict_update=self.movie_translations_on_conflict_update
					# )

					# insert_into(
					# 	cursor=cursor,
					# 	table=self.table_movie_videos,
					# 	temp_table=temp_movie_videos,
					# 	columns=self.movie_videos_columns,
					# 	# on_conflict=self.movie_videos_on_conflict,
					# 	# on_conflict_update=self.movie_videos_on_conflict_update
					# )

					# conn.commit()

					# # Delete the CSV files
					# csv["movie"].delete()
					# csv["movie_alternative_titles"].delete()
					# csv["movie_credits"].delete()
					# csv["movie_external_ids"].delete()
					# csv["movie_genres"].delete()
					# csv["movie_images"].delete()
					# csv["movie_keywords"].delete()
					# csv["movie_origin_country"].delete()
					# csv["movie_production_companies"].delete()
					# csv["movie_production_countries"].delete()
					# csv["movie_release_dates"].delete()
					# csv["movie_roles"].delete()
					# csv["movie_spoken_languages"].delete()
					# csv["movie_translations"].delete()
					# csv["movie_videos"].delete()
				except Exception as e:
					conn.rollback()
					raise
				finally:
					conn.autocommit = True
		except Exception as e:
			raise ValueError(f"Failed to push series to the database: {e}")
		finally:
			self.db_client.return_connection(conn)
