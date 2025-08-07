# ---------------------------------------------------------------------------- #
#                                    Imports                                   #
# ---------------------------------------------------------------------------- #

from datetime import date
from psycopg2.extras import execute_values

# ---------------------------------- Prefect --------------------------------- #
from prefect import flow
from prefect.logging import get_run_logger

from ...utils.file_manager import create_csv, get_csv_header
from .config import LanguageConfig

# ---------------------------------------------------------------------------- #

# ---------------------------------------------------------------------------- #
#                                    Getters                                   #
# ---------------------------------------------------------------------------- #

def get_tmdb_languages(config: LanguageConfig) -> set:
	try:
		tmdb_languages = config.tmdb_client.request("configuration/languages")
		tmdb_languages_set = set([lang["iso_639_1"] for lang in tmdb_languages])
		return tmdb_languages_set
	except Exception as e:
		raise ValueError(f"Failed to get TMDb languages: {e}")

def get_db_languages(config: LanguageConfig) -> set:
	try:
		with config.db_client.connection() as conn:
			with conn.cursor() as cursor:
				cursor.execute(f"SELECT {config.language_column} FROM {config.table_language}")
				db_languages = cursor.fetchall()
				db_languages_set = set([lang[0] for lang in db_languages])
				return db_languages_set
	except Exception as e:
		raise ValueError(f"Failed to get database languages: {e}")

# ---------------------------------------------------------------------------- #

def process_extra_languages(config: LanguageConfig, extra_languages: set):
	try:
		if len(extra_languages) > 0:
			config.logger.warning(f"Found {len(extra_languages)} extra languages in the database")
			with config.db_client.connection() as conn:
				with conn.cursor() as cursor:
					conn.autocommit = False
					try:
						cursor.execute(f"DELETE FROM {config.table_language} WHERE {config.language_column} IN %s", (tuple(extra_languages),))
						conn.commit()
					except Exception as e:
						conn.rollback()
						raise
	except Exception as e:
		raise ValueError(f"Failed to process extra languages: {e}")
	
def process_missing_languages(config: LanguageConfig, missing_languages_set: set):
	try:
		if len(missing_languages_set) > 0:
			config.logger.warning(f"Found {len(missing_languages_set)} missing languages in the database")
		
			with config.db_client.connection() as conn:
				with conn.cursor() as cursor:
					conn.autocommit = False
					try:
						config.logger.info(f"Inserting {missing_languages_set} into {config.table_language}")
						execute_values(cursor, f"""
							INSERT INTO {config.table_language} ({config.language_column})
							VALUES %s
							ON CONFLICT ({config.language_column}) DO NOTHING;
						""", [(lang,) for lang in missing_languages_set])
						
						conn.commit()
					except Exception as e:
						conn.rollback()
						raise
					finally:
						conn.autocommit = True
	except Exception as e:
		raise ValueError(f"Failed to process missing languages: {e}")
			

@flow(name="sync_tmdb_language", log_prints=True)
def sync_tmdb_language(date: date = date.today()):
	logger = get_run_logger()
	logger.info(f"Syncing language for {date}...")
	config = LanguageConfig(date=date)
	try:
		config.log_manager.init(type="tmdb_language")

		# Get the list of languages from TMDB and the database
		config.log_manager.fetching_data()
		tmdb_languages_set = get_tmdb_languages(config)
		db_languages_set = get_db_languages(config)
		config.log_manager.data_fetched()

		# Compare the languages
		extra_languages: set = db_languages_set - tmdb_languages_set
		missing_languages: set = tmdb_languages_set - db_languages_set

		# Process extra and missing languages
		config.log_manager.syncing_to_db()
		process_extra_languages(config, extra_languages)
		process_missing_languages(config, missing_languages)

		config.log_manager.success()
	except Exception as e:
		config.log_manager.failed()
		raise ValueError(f"Failed to sync language: {e}")

