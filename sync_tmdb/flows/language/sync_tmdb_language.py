# ---------------------------------------------------------------------------- #
#                                    Imports                                   #
# ---------------------------------------------------------------------------- #

from datetime import date
import pandas as pd

# ---------------------------------- Prefect --------------------------------- #
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.blocks.system import Secret

from ...utils.file_manager import create_csv, get_csv_header
from .config import LanguageConfig
from .mappers import Mappers

# ---------------------------------------------------------------------------- #

# ---------------------------------------------------------------------------- #
#                                    Getters                                   #
# ---------------------------------------------------------------------------- #

def get_tmdb_languages(config: LanguageConfig) -> tuple:
	try:
		tmdb_languages = config.tmdb_client.request("configuration/languages")
		tmdb_languages_set = set([lang["iso_639_1"] for lang in tmdb_languages])
		return tmdb_languages, tmdb_languages_set
	except Exception as e:
		raise ValueError(f"Failed to get TMDb languages: {e}")

def get_db_languages(config: LanguageConfig) -> set:
	try:
		with config.db_client.get_connection() as conn:
			with conn.cursor() as cursor:
				cursor.execute(f"SELECT iso_639_1 FROM {config.table_language}")
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
			with config.db_client.get_connection() as conn:
				with conn.cursor() as cursor:
					conn.autocommit = False
					try:
						cursor.execute(f"DELETE FROM {config.table_language} WHERE iso_639_1 IN %s", (tuple(extra_languages),))
						conn.commit()
					except Exception as e:
						conn.rollback()
						raise
	except Exception as e:
		raise ValueError(f"Failed to process extra languages: {e}")
	
def process_missing_languages(config: LanguageConfig, tmdb_languages: list, missing_languages_set: set):
	try:
		if len(missing_languages_set) > 0:
			config.logger.warning(f"Found {len(missing_languages_set)} missing languages in the database")
		
		# Initialize the mappers
		mappers = Mappers(language=tmdb_languages, default_language=config.default_language, extra_languages=config.extra_languages)

		# Generate the CSV files
		config.language = create_csv(data=mappers.language, tmp_directory=config.tmp_directory, prefix="language")
		config.language_translation = create_csv(data=mappers.language_translation, tmp_directory=config.tmp_directory, prefix="language_translation")

		# Load the CSV files into the database using copy
		with config.db_client.get_connection() as conn:
			with conn.cursor() as cursor:
				conn.autocommit = False
				try:
					cursor.execute(f"""
						CREATE TEMP TABLE temp_{config.table_language} (LIKE {config.table_language} INCLUDING ALL);
						CREATE TEMP TABLE temp_{config.table_language_translation} (LIKE {config.table_language_translation} INCLUDING ALL);
					""")

					with open(config.language, "r") as f:
						cursor.copy_expert(f"COPY temp_{config.table_language} ({','.join(get_csv_header(f))}) FROM STDIN WITH CSV HEADER", f)
					with open(config.language_translation, "r") as f:
						cursor.copy_expert(f"COPY temp_{config.table_language_translation} ({','.join(get_csv_header(f))}) FROM STDIN WITH CSV HEADER", f)

					cursor.execute(f"""
						INSERT INTO {config.table_language} (iso_639_1, name_in_native_language)
						SELECT iso_639_1, name_in_native_language FROM temp_{config.table_language}
						ON CONFLICT (iso_639_1) DO UPDATE
						SET name_in_native_language = EXCLUDED.name_in_native_language;
					""")

					cursor.execute(f"""
						INSERT INTO {config.table_language_translation} (iso_639_1, name, language)
						SELECT iso_639_1, name, language FROM temp_{config.table_language_translation}
						ON CONFLICT (iso_639_1, language) DO UPDATE
						SET name = EXCLUDED.name;
					""")
					
					conn.commit()
				except Exception as e:
					conn.rollback()
					raise
	except Exception as e:
		raise ValueError(f"Failed to process missing languages: {e}")
			

@flow(name="sync_tmdb_language", log_prints=True)
def sync_tmdb_language(date: date = date.today()):
	logger = get_run_logger()
	logger.info(f"Syncing language for {date}...")
	try:
		config = LanguageConfig(date=date)

		config.log_manager.init(type="tmdb_language")

		# Get the list of languages from TMDB and the database
		config.log_manager.fetching_data()
		tmdb_languages, tmdb_languages_set = get_tmdb_languages(config)
		db_languages_set = get_db_languages(config)
		config.log_manager.data_fetched()

		# Compare the languages
		extra_languages: set = db_languages_set - tmdb_languages_set
		missing_languages: set = tmdb_languages_set - db_languages_set

		# Process extra and missing languages
		config.log_manager.syncing_to_db()
		process_extra_languages(config, extra_languages)
		process_missing_languages(config, tmdb_languages, missing_languages)

		config.log_manager.success()
	except Exception as e:
		config.log_manager.failed()
		raise ValueError(f"Failed to sync language: {e}")

