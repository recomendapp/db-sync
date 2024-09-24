# ---------------------------------------------------------------------------- #
#                                    Imports                                   #
# ---------------------------------------------------------------------------- #

from datetime import date

# ---------------------------------- Prefect --------------------------------- #
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.blocks.system import Secret

# ----------------------------------- Class ---------------------------------- #
from .config import LanguageConfig

# ---------------------------------------------------------------------------- #


def get_tmdb_languages(config: LanguageConfig) -> set:
	try:
		response = config.tmdb_client.request("configurations/languages")
		return set([lang["iso_639_1"] for lang in response])
	except Exception as e:
		raise ValueError(f"Failed to get TMDb languages: {e}")


@flow(name="sync_language", log_prints=True)
def sync_language(date: date = date.today()):
	logger = get_run_logger()
	logger.info(f"Syncing language for {date}...")
	try:
		config = LanguageConfig()

		# Get the list of languages from TMDb
		tmdb_languages: set = get_tmdb_languages(config)
		# Get the list of languages from DB
		db_languages: set = set(config.main_config.extra_languages.languages)

		
	except Exception as e:
		logger.error(f"Syncing language failed: {e}")

