# ---------------------------------------------------------------------------- #
#                                    Imports                                   #
# ---------------------------------------------------------------------------- #

from datetime import date

# ---------------------------------- Prefect --------------------------------- #

from prefect import flow, task
from prefect.logging import get_run_logger

# ----------------------------------- Flows ---------------------------------- #
from .flows.language.sync_language import sync_language

# ---------------------------------------------------------------------------- #

@flow(name="sync_tmdb", log_prints=True)
def sync_tmdb():
	date = date.today()
	logger = get_run_logger()
	logger.info(f"Starting synchronization with TMDb for {date}...")
	
	sync_language(date=date)