from prefect import get_run_logger
from prefect.blocks.system import Secret
from .db_client import DBClient
from .storage_client import StorageClient
import os

class Config:
    def __init__(self):
        self.logger = get_run_logger()
        self.db_client = DBClient()
        self.storage_client = StorageClient()
        self.site_url = self._get_site_url()
        self.sitemap_base_url = f"{self.site_url}/sitemaps"

    def _get_site_url(self) -> str:
        try:
            return Secret.load("web-app-url").get()
        except Exception as e:
            raise ValueError(f"Postgres connection string not found: {e}")
