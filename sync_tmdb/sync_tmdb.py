# ---------------------------------------------------------------------------- #
#                                    Imports                                   #
# ---------------------------------------------------------------------------- #

from datetime import date

# ---------------------------------- Prefect --------------------------------- #

from prefect import flow
from prefect.logging import get_run_logger

# ----------------------------------- Flows ---------------------------------- #
from . import flows
# ---------------------------------------------------------------------------- #

@flow(name="sync_tmdb", log_prints=True)
def sync_tmdb(current_date: date = date.today()):
	logger = get_run_logger()
	logger.info(f"Starting synchronization with TMDb for {current_date}...")
	try:
		# flows.sync_tmdb_language(date=current_date)
		# flows.sync_tmdb_country(date=current_date)
		# flows.sync_tmdb_genre(date=current_date)
		# flows.sync_tmdb_keyword(date=current_date)
		# flows.sync_tmdb_collection(date=current_date)
		# flows.sync_tmdb_company(date=current_date)
		# flows.sync_tmdb_person(date=current_date)
		flows.sync_tmdb_movie(date=current_date)
	except Exception as e:
		logger.error(f"Syncing with TMDb failed: {e}")
		exit(1)