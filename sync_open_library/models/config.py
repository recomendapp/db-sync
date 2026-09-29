from datetime import date
from .db_client import DBClient
from .sync_logs_manager import SyncLogsManager
from prefect.variables import Variable
from prefect.logging import get_run_logger

DEFAULT_DUMP_URLS = {
	"author": "https://openlibrary.org/data/ol_dump_authors_latest.txt.gz",
	"work": "https://openlibrary.org/data/ol_dump_works_latest.txt.gz",
	"edition": "https://openlibrary.org/data/ol_dump_editions_latest.txt.gz",
	"redirects": "https://openlibrary.org/data/ol_dump_redirects_latest.txt.gz",
}

class Config:
	def __init__(self, date: date):
		self.date = date
		self.logger = get_run_logger()
		self.config = Variable.get("sync_open_library_config", {})
		self.tmp_directory: str = self.config.get("tmp_directory", ".tmp")
		self.dump_urls: dict = {**DEFAULT_DUMP_URLS, **self.config.get("dump_urls", {})}
		self.db_client = DBClient()
		self.log_manager = SyncLogsManager(config=self)
		self.chunk_size = self.config.get("chunk_size", 20000)
