import os
import shutil
from datetime import date
from .db import DBClient
from .language import Language
from .extra_languages import ExtraLanguages
from .tmdb import TMDBClient
from .sync_logs_manager import SyncLogsManager
from prefect.variables import Variable
from prefect.logging import get_run_logger

class Config:
	def __init__(self, date: date):
		self.date = date
		self.logger = get_run_logger()
		self.config = Variable.get("sync_tmdb_config", {})
		self.tmp_directory: str = self.config.get("tmp_directory", ".tmp")
		self.default_language = Language(name="English", code="en", tmdb_language="en-US")
		self.extra_languages = ExtraLanguages(languages=self.config.get("extra_languages", []))
		self.db_client = DBClient()
		self.tmdb_client = TMDBClient(config=self.config)
		self.log_manager = SyncLogsManager(config=self)
		# self.log_manager = SyncLogsManager(db_client=self.db_client, table=self.config.get("logs", {}).get("table", "sync_logs"))

	# def __del__(self):
	# 	if self.tmp_directory and os.path.exists(self.tmp_directory):
	# 		shutil.rmtree(self.tmp_directory)

