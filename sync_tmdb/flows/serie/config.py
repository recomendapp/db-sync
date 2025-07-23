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
		self.db_networks: set = None

		self.tmp_credit_ids: set = set()


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
		self.table_serie_season_credits: str = self.config.get("db_tables", {}).get("serie_season_credits", "tmdb_tv_series_seasons_credits")
		self.table_serie_season_translations: str = self.config.get("db_tables", {}).get("serie_season_translations", "tmdb_tv_series_seasons_translations")
		
		# Episodes
		self.table_serie_episode: str = self.config.get("db_tables", {}).get("serie_episode", "tmdb_tv_series_episodes")
		self.table_serie_episode_credits: str = self.config.get("db_tables", {}).get("serie_episode_credits", "tmdb_tv_series_episodes_credits")

		# Others
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
		self.serie_columns: list[str] = ["id", "adult", "in_production", "original_language", "original_name", "popularity", "status", "type", "vote_average", "vote_count", "number_of_episodes", "number_of_seasons", "first_air_date", "last_air_date"]
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
		self.serie_credits_columns: list[str] = ["id", "serie_id", "person_id", "department", "job", "character", "episode_count"]

		# Seasons
		self.serie_season_columns: list[str] = ["id", "serie_id", "season_number", "vote_average", "vote_count", "poster_path"]
		self.serie_season_credits_columns: list[str] = ["credit_id", "season_id", '"order"']
		self.serie_season_translations_columns: list[str] = ["season_id", "name", "overview", "iso_639_1", "iso_3166_1"]

		# Episodes
		self.serie_episode_columns: list[str] = ["id", "season_id", "air_date", "episode_number", "episode_type", "name", "overview", "production_code", "runtime", "still_path", "vote_average", "vote_count"]
		self.serie_episode_credits_columns: list[str] = ["credit_id", "episode_id"]

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
		self.serie_credits_on_conflict: list[str] = ["id"]

		# Seasons
		self.serie_season_on_conflict: list[str] = ["id"]
		self.serie_season_credits_on_conflict: list[str] = ["credit_id", "season_id"]
		self.serie_season_translations_on_conflict: list[str] = ["season_id", "iso_639_1", "iso_3166_1"]

		# Episodes
		self.serie_episode_on_conflict: list[str] = ["id"]
		self.serie_episode_credits_on_conflict: list[str] = ["credit_id", "episode_id"]

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

		# Seasons
		self.serie_season_on_conflict_update: list[str] = [col for col in self.serie_season_columns if col not in self.serie_season_on_conflict]
		self.serie_season_credits_on_conflict_update: list[str] = [col for col in self.serie_season_credits_columns if col not in self.serie_season_credits_on_conflict]
		self.serie_season_translations_on_conflict_update: list[str] = [col for col in self.serie_season_translations_columns if col not in self.serie_season_translations_on_conflict]

		# Episodes
		self.serie_episode_on_conflict_update: list[str] = [col for col in self.serie_episode_columns if col not in self.serie_episode_on_conflict]
		self.serie_episode_credits_on_conflict_update: list[str] = [col for col in self.serie_episode_credits_columns if col not in self.serie_episode_credits_on_conflict]
	
	@task(cache_policy=None)
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
	@task(cache_policy=None)
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

	@task(cache_policy=None)
	def push(self, csv: dict[str, CSVFile]):
		"""Push the series to the database"""
		conn = self.db_client.get_connection()
		self.logger.info(f"Connections: {self.db_client.nb_open_connections} open, {self.db_client.nb_close_connections} close")
		try:
			# Clean duplicates from the CSV files
			csv["serie"].clean_duplicates(conflict_columns=self.serie_on_conflict)
			csv["serie_alternative_titles"].clean_duplicates(conflict_columns=self.serie_alternative_titles_on_conflict)
			csv["serie_content_ratings"].clean_duplicates(conflict_columns=self.serie_content_ratings_on_conflict)
			csv["serie_external_ids"].clean_duplicates(conflict_columns=self.serie_external_ids_on_conflict)
			csv["serie_genres"].clean_duplicates(conflict_columns=self.serie_genres_on_conflict)
			csv["serie_images"].clean_duplicates(conflict_columns=self.serie_images_on_conflict)
			csv["serie_keywords"].clean_duplicates(conflict_columns=self.serie_keywords_on_conflict)
			csv["serie_languages"].clean_duplicates(conflict_columns=self.serie_languages_on_conflict)
			csv["serie_networks"].clean_duplicates(conflict_columns=self.serie_networks_on_conflict)
			csv["serie_origin_country"].clean_duplicates(conflict_columns=self.serie_origin_country_on_conflict)
			csv["serie_production_companies"].clean_duplicates(conflict_columns=self.serie_production_companies_on_conflict)
			csv["serie_production_countries"].clean_duplicates(conflict_columns=self.serie_production_countries_on_conflict)
			csv["serie_spoken_languages"].clean_duplicates(conflict_columns=self.serie_spoken_languages_on_conflict)
			csv["serie_translations"].clean_duplicates(conflict_columns=self.serie_translations_on_conflict)
			csv["serie_videos"].clean_duplicates(conflict_columns=self.serie_videos_on_conflict)
			csv["serie_credits"].clean_duplicates(conflict_columns=self.serie_credits_on_conflict)
			csv["serie_season"].clean_duplicates(conflict_columns=self.serie_season_on_conflict)
			csv["serie_season_credits"].clean_duplicates(conflict_columns=self.serie_season_credits_on_conflict)
			csv["serie_season_translations"].clean_duplicates(conflict_columns=self.serie_season_translations_on_conflict)
			csv["serie_episode"].clean_duplicates(conflict_columns=self.serie_episode_on_conflict)
			csv["serie_episode_credits"].clean_duplicates(conflict_columns=self.serie_episode_credits_on_conflict)

			with conn.cursor() as cursor:
				try:
					conn.autocommit = False

					temp_serie = f"temp_{self.table_serie}_{uuid.uuid4().hex}"
					temp_serie_alternative_titles = f"temp_{self.table_serie_alternative_titles}_{uuid.uuid4().hex}"
					temp_serie_content_ratings = f"temp_{self.table_serie_content_ratings}_{uuid.uuid4().hex}"
					temp_serie_external_ids = f"temp_{self.table_serie_external_ids}_{uuid.uuid4().hex}"
					temp_serie_genres = f"temp_{self.table_serie_genres}_{uuid.uuid4().hex}"
					temp_serie_images = f"temp_{self.table_serie_images}_{uuid.uuid4().hex}"
					temp_serie_keywords = f"temp_{self.table_serie_keywords}_{uuid.uuid4().hex}"
					temp_serie_languages = f"temp_{self.table_serie_languages}_{uuid.uuid4().hex}"
					temp_serie_networks = f"temp_{self.table_serie_networks}_{uuid.uuid4().hex}"
					temp_serie_origin_country = f"temp_{self.table_serie_origin_country}_{uuid.uuid4().hex}"
					temp_serie_production_companies = f"temp_{self.table_serie_production_companies}_{uuid.uuid4().hex}"
					temp_serie_production_countries = f"temp_{self.table_serie_production_countries}_{uuid.uuid4().hex}"
					temp_serie_spoken_languages = f"temp_{self.table_serie_spoken_languages}_{uuid.uuid4().hex}"
					temp_serie_translations = f"temp_{self.table_serie_translations}_{uuid.uuid4().hex}"
					temp_serie_videos = f"temp_{self.table_serie_videos}_{uuid.uuid4().hex}"
					temp_serie_credits = f"temp_{self.table_serie_credits}_{uuid.uuid4().hex}"
					temp_serie_season = f"temp_{self.table_serie_season}_{uuid.uuid4().hex}"
					temp_serie_season_credits = f"temp_{self.table_serie_season_credits}_{uuid.uuid4().hex}"
					temp_serie_season_translations = f"temp_{self.table_serie_season_translations}_{uuid.uuid4().hex}"
					temp_serie_episode = f"temp_{self.table_serie_episode}_{uuid.uuid4().hex}"
					temp_serie_episode_credits = f"temp_{self.table_serie_episode_credits}_{uuid.uuid4().hex}"

					cursor.execute(f"""
						CREATE TEMP TABLE {temp_serie} (LIKE {self.table_serie} INCLUDING ALL);
						CREATE TEMP TABLE {temp_serie_alternative_titles} (LIKE {self.table_serie_alternative_titles} INCLUDING ALL);
						CREATE TEMP TABLE {temp_serie_content_ratings} (LIKE {self.table_serie_content_ratings} INCLUDING ALL);
						CREATE TEMP TABLE {temp_serie_external_ids} (LIKE {self.table_serie_external_ids} INCLUDING ALL);
						CREATE TEMP TABLE {temp_serie_genres} (LIKE {self.table_serie_genres} INCLUDING ALL);
						CREATE TEMP TABLE {temp_serie_images} (LIKE {self.table_serie_images} INCLUDING ALL);
						CREATE TEMP TABLE {temp_serie_keywords} (LIKE {self.table_serie_keywords} INCLUDING ALL);
						CREATE TEMP TABLE {temp_serie_languages} (LIKE {self.table_serie_languages} INCLUDING ALL);
						CREATE TEMP TABLE {temp_serie_networks} (LIKE {self.table_serie_networks} INCLUDING ALL);
						CREATE TEMP TABLE {temp_serie_origin_country} (LIKE {self.table_serie_origin_country} INCLUDING ALL);
						CREATE TEMP TABLE {temp_serie_production_companies} (LIKE {self.table_serie_production_companies} INCLUDING ALL);
						CREATE TEMP TABLE {temp_serie_production_countries} (LIKE {self.table_serie_production_countries} INCLUDING ALL);
						CREATE TEMP TABLE {temp_serie_spoken_languages} (LIKE {self.table_serie_spoken_languages} INCLUDING ALL);
						CREATE TEMP TABLE {temp_serie_translations} (LIKE {self.table_serie_translations} INCLUDING ALL);
						CREATE TEMP TABLE {temp_serie_videos} (LIKE {self.table_serie_videos} INCLUDING ALL);
						CREATE TEMP TABLE {temp_serie_credits} (LIKE {self.table_serie_credits} INCLUDING ALL);
						CREATE TEMP TABLE {temp_serie_season} (LIKE {self.table_serie_season} INCLUDING ALL);
						CREATE TEMP TABLE {temp_serie_season_credits} (LIKE {self.table_serie_season_credits} INCLUDING ALL);
						CREATE TEMP TABLE {temp_serie_season_translations} (LIKE {self.table_serie_season_translations} INCLUDING ALL);
						CREATE TEMP TABLE {temp_serie_episode} (LIKE {self.table_serie_episode} INCLUDING ALL);
						CREATE TEMP TABLE {temp_serie_episode_credits} (LIKE {self.table_serie_episode_credits} INCLUDING ALL);
					""")

					with open(csv["serie"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie} ({','.join(self.serie_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(csv["serie_alternative_titles"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie_alternative_titles} ({','.join(self.serie_alternative_titles_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(csv["serie_content_ratings"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie_content_ratings} ({','.join(self.serie_content_ratings_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(csv["serie_external_ids"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie_external_ids} ({','.join(self.serie_external_ids_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(csv["serie_genres"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie_genres} ({','.join(self.serie_genres_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(csv["serie_images"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie_images} ({','.join(self.serie_images_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(csv["serie_keywords"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie_keywords} ({','.join(self.serie_keywords_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(csv["serie_languages"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie_languages} ({','.join(self.serie_languages_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(csv["serie_networks"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie_networks} ({','.join(self.serie_networks_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(csv["serie_origin_country"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie_origin_country} ({','.join(self.serie_origin_country_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(csv["serie_production_companies"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie_production_companies} ({','.join(self.serie_production_companies_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(csv["serie_production_countries"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie_production_countries} ({','.join(self.serie_production_countries_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(csv["serie_spoken_languages"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie_spoken_languages} ({','.join(self.serie_spoken_languages_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(csv["serie_translations"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie_translations} ({','.join(self.serie_translations_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(csv["serie_videos"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie_videos} ({','.join(self.serie_videos_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(csv["serie_credits"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie_credits} ({','.join(self.serie_credits_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(csv["serie_season"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie_season} ({','.join(self.serie_season_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(csv["serie_season_credits"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie_season_credits} ({','.join(self.serie_season_credits_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(csv["serie_season_translations"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie_season_translations} ({','.join(self.serie_season_translations_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(csv["serie_episode"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie_episode} ({','.join(self.serie_episode_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(csv["serie_episode_credits"].file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_serie_episode_credits} ({','.join(self.serie_episode_credits_columns)}) FROM STDIN WITH CSV HEADER", f)

					# Delete all outdated alternative titles before inserting
					cursor.execute(f"""
						DELETE FROM {self.table_serie_alternative_titles}
						WHERE {self.serie_alternative_titles_columns[0]} IN (
							SELECT id FROM {temp_serie}
						);
					""")

					# Delete all outdated content ratings before inserting
					cursor.execute(f"""
						DELETE FROM {self.table_serie_content_ratings}
						WHERE {self.serie_content_ratings_columns[0]} IN (
							SELECT id FROM {temp_serie}
						);
					""")

					# Delete all outdated external ids before inserting
					cursor.execute(f"""
						DELETE FROM {self.table_serie_external_ids}
						WHERE {self.serie_external_ids_columns[0]} IN (
							SELECT id FROM {temp_serie}
						);
					""")

					# Delete all outdated genres before inserting
					cursor.execute(f"""
						DELETE FROM {self.table_serie_genres}
						WHERE {self.serie_genres_columns[0]} IN (
							SELECT id FROM {temp_serie}
						);
					""")

					# Delete all outdated images before inserting
					cursor.execute(f"""
						DELETE FROM {self.table_serie_images}
						WHERE {self.serie_images_columns[0]} IN (
							SELECT id FROM {temp_serie}
						);
					""")

					# Delete all outdated keywords before inserting
					cursor.execute(f"""
						DELETE FROM {self.table_serie_keywords}
						WHERE {self.serie_keywords_columns[0]} IN (
							SELECT id FROM {temp_serie}
						);
					""")

					# Delete all outdated languages before inserting
					cursor.execute(f"""
						DELETE FROM {self.table_serie_languages}
						WHERE {self.serie_languages_columns[0]} IN (
							SELECT id FROM {temp_serie}
						);
					""")

					# Delete all outdated networks before inserting
					cursor.execute(f"""
						DELETE FROM {self.table_serie_networks}
						WHERE {self.serie_networks_columns[0]} IN (
							SELECT id FROM {temp_serie}
						);
					""")

					# Delete all outdated origin countries before inserting
					cursor.execute(f"""
						DELETE FROM {self.table_serie_origin_country}
						WHERE {self.serie_origin_country_columns[0]} IN (
							SELECT id FROM {temp_serie}
						);
					""")
					
					# Delete all outdated production companies before inserting
					cursor.execute(f"""
						DELETE FROM {self.table_serie_production_companies}
						WHERE {self.serie_production_companies_columns[0]} IN (
							SELECT id FROM {temp_serie}
						);
					""")

					# Delete all outdated production countries before inserting
					cursor.execute(f"""
						DELETE FROM {self.table_serie_production_countries}
						WHERE {self.serie_production_countries_columns[0]} IN (
							SELECT id FROM {temp_serie}
						);
					""")

					# Delete all outdated spoken languages before inserting
					cursor.execute(f"""
						DELETE FROM {self.table_serie_spoken_languages}
						WHERE {self.serie_spoken_languages_columns[0]} IN (
							SELECT id FROM {temp_serie}
						);
					""")

					# Delete all outdated translations before inserting
					cursor.execute(f"""
						DELETE FROM {self.table_serie_translations}
						WHERE {self.serie_translations_columns[0]} IN (
							SELECT id FROM {temp_serie}
						);
					""")

					# Delete all outdated videos before inserting
					cursor.execute(f"""
						DELETE FROM {self.table_serie_videos}
						WHERE {self.serie_videos_columns[1]} IN (
							SELECT id FROM {temp_serie}
						);
					""")

					# Delete all outdated credits before inserting
					cursor.execute(f"""
						DELETE FROM {self.table_serie_credits}
						WHERE {self.serie_credits_columns[1]} IN (
							SELECT id FROM {temp_serie}
						);
					""")

					# Delete all outdated seasons before inserting
					# Careful, we dont have to delete all because user can have activity with seasons so we have to delete only the ones not in the new data BUT only series that are in the new data
					cursor.execute(f"""
						DELETE FROM {self.table_serie_season}
						WHERE {self.serie_season_columns[0]} NOT IN (
							SELECT id FROM {temp_serie_season}
						) AND {self.serie_season_columns[1]} IN (
							SELECT id FROM {temp_serie}
						);
					""")

					# Delete all outdated season credits before inserting
					cursor.execute(f"""
						DELETE FROM {self.table_serie_season_credits}
						WHERE {self.serie_season_credits_columns[1]} IN (
							SELECT id FROM {temp_serie_season}
						);
					""")

					# Delete all outdated season translations before inserting
					cursor.execute(f"""
						DELETE FROM {self.table_serie_season_translations}
						WHERE {self.serie_season_translations_columns[0]} IN (
							SELECT id FROM {temp_serie_season}
						);
					""")

					# Delete all outdated episodes before inserting
					# Careful, we dont have to delete all because user can have activity with episodes so we have to delete only the ones not in the new data BUT only series that are in the new data
					cursor.execute(f"""
						DELETE FROM {self.table_serie_episode}
						WHERE {self.serie_episode_columns[0]} NOT IN (
							SELECT id FROM {temp_serie_episode}
						) AND {self.serie_episode_columns[1]} IN (
							SELECT id FROM {temp_serie_season}
						);
					""")

					# Delete all outdated episode credits before inserting
					cursor.execute(f"""
						DELETE FROM {self.table_serie_episode_credits}
						WHERE {self.serie_episode_credits_columns[1]} IN (
							SELECT id FROM {temp_serie_episode}
						);
					""")

					insert_into(
						cursor=cursor,
						table=self.table_serie,
						temp_table=temp_serie,
						columns=self.serie_columns,
						on_conflict=self.serie_on_conflict,
						on_conflict_update=self.serie_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_serie_alternative_titles,
						temp_table=temp_serie_alternative_titles,
						columns=self.serie_alternative_titles_columns,
						# on_conflict=self.serie_alternative_titles_on_conflict,
						# on_conflict_update=self.serie_alternative_titles_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_serie_content_ratings,
						temp_table=temp_serie_content_ratings,
						columns=self.serie_content_ratings_columns,
						# on_conflict=self.serie_content_ratings_on_conflict,
						# on_conflict_update=self.serie_content_ratings_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_serie_external_ids,
						temp_table=temp_serie_external_ids,
						columns=self.serie_external_ids_columns,
						# on_conflict=self.serie_external_ids_on_conflict,
						# on_conflict_update=self.serie_external_ids_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_serie_genres,
						temp_table=temp_serie_genres,
						columns=self.serie_genres_columns,
						# on_conflict=self.serie_genres_on_conflict,
						# on_conflict_update=self.serie_genres_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_serie_images,
						temp_table=temp_serie_images,
						columns=self.serie_images_columns,
						# on_conflict=self.serie_images_on_conflict,
						# on_conflict_update=self.serie_images_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_serie_keywords,
						temp_table=temp_serie_keywords,
						columns=self.serie_keywords_columns,
						# on_conflict=self.serie_keywords_on_conflict,
						# on_conflict_update=self.serie_keywords_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_serie_languages,
						temp_table=temp_serie_languages,
						columns=self.serie_languages_columns,
						# on_conflict=self.serie_languages_on_conflict,
						# on_conflict_update=self.serie_languages_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_serie_networks,
						temp_table=temp_serie_networks,
						columns=self.serie_networks_columns,
						# on_conflict=self.serie_networks_on_conflict,
						# on_conflict_update=self.serie_networks_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_serie_origin_country,
						temp_table=temp_serie_origin_country,
						columns=self.serie_origin_country_columns,
						# on_conflict=self.serie_origin_country_on_conflict,
						# on_conflict_update=self.serie_origin_country_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_serie_production_companies,
						temp_table=temp_serie_production_companies,
						columns=self.serie_production_companies_columns,
						# on_conflict=self.serie_production_companies_on_conflict,
						# on_conflict_update=self.serie_production_companies_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_serie_production_countries,
						temp_table=temp_serie_production_countries,
						columns=self.serie_production_countries_columns,
						# on_conflict=self.serie_production_countries_on_conflict,
						# on_conflict_update=self.serie_production_countries_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_serie_spoken_languages,
						temp_table=temp_serie_spoken_languages,
						columns=self.serie_spoken_languages_columns,
						# on_conflict=self.serie_spoken_languages_on_conflict,
						# on_conflict_update=self.serie_spoken_languages_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_serie_translations,
						temp_table=temp_serie_translations,
						columns=self.serie_translations_columns,
						# on_conflict=self.serie_translations_on_conflict,
						# on_conflict_update=self.serie_translations_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_serie_videos,
						temp_table=temp_serie_videos,
						columns=self.serie_videos_columns,
						# on_conflict=self.serie_videos_on_conflict,
						# on_conflict_update=self.serie_videos_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_serie_credits,
						temp_table=temp_serie_credits,
						columns=self.serie_credits_columns,
						# on_conflict=self.serie_credits_on_conflict,
						# on_conflict_update=self.serie_credits_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_serie_season,
						temp_table=temp_serie_season,
						columns=self.serie_season_columns,
						on_conflict=self.serie_season_on_conflict,
						on_conflict_update=self.serie_season_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_serie_season_credits,
						temp_table=temp_serie_season_credits,
						columns=self.serie_season_credits_columns,
						# on_conflict=self.serie_season_credits_on_conflict,
						# on_conflict_update=self.serie_season_credits_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_serie_season_translations,
						temp_table=temp_serie_season_translations,
						columns=self.serie_season_translations_columns,
						# on_conflict=self.serie_season_translations_on_conflict,
						# on_conflict_update=self.serie_season_translations_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_serie_episode,
						temp_table=temp_serie_episode,
						columns=self.serie_episode_columns,
						on_conflict=self.serie_episode_on_conflict,
						on_conflict_update=self.serie_episode_on_conflict_update
					)

					insert_into(
						cursor=cursor,
						table=self.table_serie_episode_credits,
						temp_table=temp_serie_episode_credits,
						columns=self.serie_episode_credits_columns,
						# on_conflict=self.serie_episode_credits_on_conflict,
						# on_conflict_update=self.serie_episode_credits_on_conflict_update
					)

					conn.commit()

					# Delete the CSV files
					csv["serie"].delete()
					csv["serie_alternative_titles"].delete()
					csv["serie_content_ratings"].delete()
					csv["serie_external_ids"].delete()
					csv["serie_genres"].delete()
					csv["serie_images"].delete()
					csv["serie_keywords"].delete()
					csv["serie_languages"].delete()
					csv["serie_networks"].delete()
					csv["serie_origin_country"].delete()
					csv["serie_production_companies"].delete()
					csv["serie_production_countries"].delete()
					csv["serie_spoken_languages"].delete()
					csv["serie_translations"].delete()
					csv["serie_videos"].delete()
					csv["serie_credits"].delete()
					csv["serie_season"].delete()
					csv["serie_season_credits"].delete()
					csv["serie_season_translations"].delete()
					csv["serie_episode"].delete()
					csv["serie_episode_credits"].delete()
				except Exception as e:
					conn.rollback()
					raise
				finally:
					conn.autocommit = True
		except Exception as e:
			raise ValueError(f"Failed to push series to the database: {e}")
		finally:
			self.db_client.return_connection(conn)
