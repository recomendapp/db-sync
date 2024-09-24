# ---------------------------------------------------------------------------- #
#                                    Imports                                   #
# ---------------------------------------------------------------------------- #

from datetime import date

# ---------------------------------- Prefect --------------------------------- #
from prefect import flow, task
from prefect.logging import get_run_logger

# ----------------------------------- Class ---------------------------------- #
from .config import Config

@flow(name="sync_language", log_prints=True)
# Add facultative parameter to the task day
def sync_language(date: date = date.today()):
	logger = get_run_logger()
	logger.info(f"Syncing language for {date}...")
	config = Config(extra_languages=["es"])

