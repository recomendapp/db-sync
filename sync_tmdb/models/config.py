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
		"""Update the popularity in the database for any content type
		
		Args:
			popularity_data: Dict with {id: popularity} mapping
			table_name: Name of the table to update
			content_type: Type of content for logging (e.g., "persons", "movies", "series")
		"""
		if not popularity_data:
			self.logger.info(f"No popularity data to update for {content_type}")
			return
			
		conn = self.db_client.get_connection()
		try:
			with conn.cursor() as cursor:
				try:
					conn.autocommit = False
					
					# Construire la requête UPDATE avec CASE en une seule transaction
					case_statements = [
						f"WHEN {item_id} THEN {popularity}" 
						for item_id, popularity in popularity_data.items()
					]
					
					ids_str = ','.join(map(str, popularity_data.keys()))
					query = f"""
					UPDATE {table_name} 
					SET popularity = CASE id 
						{' '.join(case_statements)}
					END
					WHERE id IN ({ids_str})
					"""
					
					cursor.execute(query)
					updated_count = cursor.rowcount
					conn.commit()
					
					self.logger.info(f"Updated popularity for {updated_count} {content_type} (out of {len(popularity_data)} from TMDB)")
					
				except Exception as e:
					conn.rollback()
					raise ValueError(f"Failed to update popularity for {content_type}: {e}")
				finally:
					conn.autocommit = True
		except Exception as e:
			raise ValueError(f"Failed to update popularity for {content_type}: {e}")
		finally:
			self.db_client.return_connection(conn)

	# def __del__(self):
	# 	self.logger.info("Cleaning up...")
	# 	if self.tmp_directory and os.path.exists(self.tmp_directory):
	# 		shutil.rmtree(self.tmp_directory)

