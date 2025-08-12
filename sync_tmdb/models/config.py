from datetime import date
from .db_client import DBClient
from .language import Language
from .extra_languages import ExtraLanguages
from .tmdb import TMDBClient
from .sync_logs_manager import SyncLogsManager
from .csv_file import CSVFile
from prefect.variables import Variable
from prefect.logging import get_run_logger
from prefect import task
import psycopg2.extras
import pandas as pd
import gc

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

	# @task(cache_policy=None)
	def update_popularity(self, tmdb_popularity_data: dict, table_name: str, content_type: str = "items"):
		"""
		Updates popularity in the database by comparing against the full TMDB dataset.
		The comparison and update logic is handled efficiently by the database itself.
		"""
		if not tmdb_popularity_data:
			self.logger.info(f"No TMDB popularity data to process for {content_type}")
			return

		self.logger.info(f"Updating popularity for {content_type} by comparing against {len(tmdb_popularity_data)} TMDB records...")
		
		conn = self.db_client.get_connection()
		popularity_csv = None
		try:
			popularity_df = pd.DataFrame(list(tmdb_popularity_data.items()), columns=['id', 'popularity'])

			popularity_csv = CSVFile(
				columns=['id', 'popularity'],
				tmp_directory=self.tmp_directory,
				prefix=f"{content_type}_popularity_update"
			)
			popularity_csv.append(rows_data=popularity_df)

			del popularity_df
			gc.collect()
		
			with conn.cursor() as cursor:
				conn.autocommit = False

				temp_table_name = f"temp_{table_name}_popularity_update"
				cursor.execute(f"CREATE TEMP TABLE {temp_table_name} (id INTEGER PRIMARY KEY, popularity REAL) ON COMMIT DROP;")

				with open(popularity_csv.file_path, "r", encoding="utf-8") as f:
					cursor.copy_expert(f"COPY {temp_table_name} ({','.join(popularity_csv.columns)}) FROM STDIN WITH CSV HEADER", f)

				update_query = f"""
				UPDATE {table_name} AS main_table
				SET popularity = temp_table.popularity
				FROM {temp_table_name} AS temp_table
				WHERE 
					main_table.id = temp_table.id 
					AND main_table.popularity IS DISTINCT FROM temp_table.popularity;
				"""
				cursor.execute(update_query)
				
				updated_count = cursor.rowcount
				conn.commit()
				
				self.logger.info(f"Successfully updated popularity for {updated_count} {content_type} whose values had changed.")
				
		except Exception as e:
			conn.rollback()
			raise ValueError(f"Failed to update popularity for {content_type}: {e}")
		finally:
			if conn:
				self.db_client.return_connection(conn)
			if popularity_csv:
				popularity_csv.delete()


	# def __del__(self):
	# 	self.logger.info("Cleaning up...")
	# 	if self.tmp_directory and os.path.exists(self.tmp_directory):
	# 		shutil.rmtree(self.tmp_directory)

