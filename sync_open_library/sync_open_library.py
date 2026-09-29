# ---------------------------------------------------------------------------- #
#                                    Imports                                   #
# ---------------------------------------------------------------------------- #

from datetime import date
import os
import shutil

# ---------------------------------- Prefect --------------------------------- #

from prefect import flow
from prefect.logging import get_run_logger

# ----------------------------------- Flows ---------------------------------- #
from . import flows
from .models.config import DEFAULT_DUMP_URLS
from .utils.file_manager import download_file
# ---------------------------------------------------------------------------- #

def sync_entity(logger, entity_flow, entity_name: str, current_date: date, dump_url: str, redirects_path: str, tmp_directory: str, max_records: int | None = None):
	"""
	Download one Open Library dump, run its flow, then delete the dump right away.

	Each entity dump is fetched and cleaned up one at a time (not all three up front)
	since the editions dump alone is close to 10GB compressed — keeping all three on
	disk simultaneously would need far more space than processing them one at a time.
	The (much smaller) redirects dump is downloaded once by the caller and shared here.
	"""
	logger.info(f"Downloading {entity_name} dump...")
	dump_path = download_file(dump_url, tmp_directory=tmp_directory, prefix=f"ol_dump_{entity_name}")
	try:
		entity_flow(date=current_date, dump_path=dump_path, redirects_path=redirects_path, max_records=max_records)
	finally:
		if os.path.exists(dump_path):
			os.remove(dump_path)

@flow(name="sync_open_library", log_prints=True)
def sync_open_library(
	current_date: date = date.today(),
	author: bool = True,
	work: bool = True,
	edition: bool = True,
	max_records: int | None = None,
):
	logger = get_run_logger()
	logger.info(f"Starting synchronization with Open Library for {current_date}...")

	tmp_directory = ".tmp"
	redirects_path = None

	try:
		# Downloaded once and shared across all three flows: the same ~50MB file covers
		# author, work and edition redirects alike, no reason to fetch it three times.
		logger.info("Downloading redirects dump...")
		redirects_path = download_file(DEFAULT_DUMP_URLS["redirects"], tmp_directory=tmp_directory, prefix="ol_dump_redirects")

		# Order matters: work_author is only kept for authors that already exist
		# (WorkConfig.push), and edition.work_key is nulled out when the work it
		# points to doesn't exist yet (EditionConfig.push).
		if author:
			sync_entity(logger, flows.sync_open_library_author, "authors", current_date, DEFAULT_DUMP_URLS["author"], redirects_path, tmp_directory, max_records)
		if work:
			sync_entity(logger, flows.sync_open_library_work, "works", current_date, DEFAULT_DUMP_URLS["work"], redirects_path, tmp_directory, max_records)
		if edition:
			sync_entity(logger, flows.sync_open_library_edition, "editions", current_date, DEFAULT_DUMP_URLS["edition"], redirects_path, tmp_directory, max_records)

		logger.info(f"Successfully synchronized with Open Library for {current_date}.")

	except Exception as e:
		logger.error(f"Syncing with Open Library failed: {e}")
		if os.path.exists(tmp_directory):
			shutil.rmtree(tmp_directory)
		raise
	finally:
		if redirects_path and os.path.exists(redirects_path):
			os.remove(redirects_path)
