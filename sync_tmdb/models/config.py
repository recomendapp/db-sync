import os
import shutil
from datetime import date
from .db_client import DBClient
from .language import Language
from .extra_languages import ExtraLanguages
from .tmdb import TMDBClient
from .sync_logs_manager import SyncLogsManager
from prefect.variables import Variable
from prefect.logging import get_run_logger
from prefect import task
import psycopg2.extras

class Config:
	def __init__(self, date: date):
		self.date = date
		self.logger = get_run_logger()
		self.config = Variable.get("sync_tmdb_config", {})
		self.tmp_directory: str = self.config.get("tmp_directory", ".tmp")
		self.default_language = Language(name="English", code="en-US", tmdb_language="en-US")
		self.extra_languages = ExtraLanguages(languages=self.config.get("extra_languages", []))
		self.db_client = DBClient()
		self.tmdb_client = TMDBClient(config=self.config)
		self.log_manager = SyncLogsManager(config=self)
		self.chunk_size = self.config.get("chunk_size", 1000)

	@task(cache_policy=None)
	def update_popularity(self, popularity_data: dict, table_name: str, content_type: str = "items"):
		if not popularity_data:
			self.logger.info(f"No popularity data to update for {content_type}")
			return

		self.logger.info(f"Updating popularity for {len(popularity_data)} {content_type} with batch method...")
		
		conn = self.db_client.get_connection()
		try:
			with conn.cursor() as cursor:
				conn.autocommit = False

				cursor.execute(f"CREATE TEMP TABLE temp_{table_name}_popularity_update (id INTEGER PRIMARY KEY, popularity REAL) ON COMMIT DROP;")

				data_to_insert = list(popularity_data.items())
				
				psycopg2.extras.execute_values(
					cursor,
					f"INSERT INTO temp_{table_name}_popularity_update (id, popularity) VALUES %s",
					data_to_insert
				)
				
				update_query = f"""
				UPDATE {table_name} AS p
				SET popularity = t.popularity
				FROM temp_{table_name}_popularity_update AS t
				WHERE p.id = t.id;
				"""
				cursor.execute(update_query)
				
				updated_count = cursor.rowcount
				conn.commit()
				
				self.logger.info(f"Successfully updated popularity for {updated_count} {content_type}.")
				
		except Exception as e:
			conn.rollback()
			raise ValueError(f"Failed to update popularity for {content_type}: {e}")
		finally:
			self.db_client.return_connection(conn)


	# def __del__(self):
	# 	self.logger.info("Cleaning up...")
	# 	if self.tmp_directory and os.path.exists(self.tmp_directory):
	# 		shutil.rmtree(self.tmp_directory)

