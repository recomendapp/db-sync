from prefect import flow, task
from prefect.logging import get_run_logger

from . import flows

@flow(name="sync_typesense", log_prints=True)
def sync_typesense():
	logger = get_run_logger()
	logger.info("Starting synchronization with Typesense...")
	try:
		flows.sync_typesense_movies()
		# flows.sync_typesense_tv_series()
		# flows.sync_typesense_persons()
		# flows.sync_typesense_playlists()
		# flows.sync_typesense_users()
		logger.info("Successfully synchronized with Typesense.")
	except Exception as e:
		logger.error(f"Syncing with Typesense failed: {e}")
		raise