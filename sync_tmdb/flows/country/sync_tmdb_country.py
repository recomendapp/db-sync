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
from .config import CountryConfig
from .mappers import Mappers

# ---------------------------------------------------------------------------- #

# ---------------------------------------------------------------------------- #
#                                    Getters                                   #
# ---------------------------------------------------------------------------- #

def get_tmdb_countries(config: CountryConfig) -> tuple:
	try:
		tmdb_countries = config.tmdb_client.request("configuration/countries", params={"language": "fr-FR"})
		tmdb_countries_set = set([item["iso_3166_1"] for item in tmdb_countries])
		return tmdb_countries, tmdb_countries_set
	except Exception as e:
		raise ValueError(f"Failed to get TMDB countries: {e}")

def get_db_countries(config: CountryConfig) -> set:
	try:
		with config.db_client.get_connection() as conn:
			with conn.cursor() as cursor:
				cursor.execute(f"SELECT iso_3166_1 FROM {config.table_country}")
				db_countries = cursor.fetchall()
				db_countries_set = set([item[0] for item in db_countries])
				return db_countries_set
	except Exception as e:
		raise ValueError(f"Failed to get database countries: {e}")

# ---------------------------------------------------------------------------- #

def process_extra_countries(config: CountryConfig, extra_countries: set):
	try:
		if len(extra_countries) > 0:
			config.logger.warning(f"Found {len(extra_countries)} extra countries in the database")
			with config.db_client.get_connection() as conn:
				with conn.cursor() as cursor:
					conn.autocommit = False
					try:
						cursor.execute(f"DELETE FROM {config.table_country} WHERE iso_3166_1 IN %s", (tuple(extra_countries),))
						conn.commit()
					except Exception as e:
						conn.rollback()
						raise
	except Exception as e:
		raise ValueError(f"Failed to process extra countries: {e}")
	
def process_missing_countries(config: CountryConfig, tmdb_countries: list, missing_countries_set: set):
	try:
		if len(missing_countries_set) > 0:
			config.logger.warning(f"Found {len(missing_countries_set)} missing countries in the database")
		
		# Initialize the mappers
		mappers = Mappers(country=tmdb_countries, default_language=config.default_language, extra_languages=config.extra_languages)

		# Generate the CSV files
		config.country = create_csv(data=mappers.country, tmp_directory=config.tmp_directory, prefix="country")
		config.country_translation = create_csv(data=mappers.country_translation, tmp_directory=config.tmp_directory, prefix="country_translation")

		# Load the CSV files into the database using copy
		with config.db_client.get_connection() as conn:
			with conn.cursor() as cursor:
				conn.autocommit = False
				try:
					cursor.execute(f"""
						CREATE TEMP TABLE temp_{config.table_country} (LIKE {config.table_country} INCLUDING ALL);
						CREATE TEMP TABLE temp_{config.table_country_translation} (LIKE {config.table_country_translation} INCLUDING ALL);
					""")

					with open(config.country, "r") as f:
						cursor.copy_expert(f"COPY temp_{config.table_country} ({','.join(get_csv_header(f))}) FROM STDIN WITH CSV HEADER", f)
					with open(config.country_translation, "r") as f:
						cursor.copy_expert(f"COPY temp_{config.table_country_translation} ({','.join(get_csv_header(f))}) FROM STDIN WITH CSV HEADER", f)

					cursor.execute(f"""
						INSERT INTO {config.table_country} (iso_3166_1)
						SELECT iso_3166_1 FROM temp_{config.table_country}
						ON CONFLICT (iso_3166_1) DO NOTHING;
					""")

					cursor.execute(f"""
						INSERT INTO {config.table_country_translation} (iso_3166_1, name, language)
						SELECT iso_3166_1, name, language FROM temp_{config.table_country_translation}
						ON CONFLICT (iso_3166_1, language) DO UPDATE
						SET name = EXCLUDED.name;
					""")
					
					conn.commit()
				except Exception as e:
					conn.rollback()
					raise
	except Exception as e:
		raise ValueError(f"Failed to process missing languages: {e}")
			

@flow(name="sync_tmdb_country", log_prints=True)
def sync_tmdb_country(date: date = date.today()):
	logger = get_run_logger()
	logger.info(f"Syncing country for {date}...")
	try:
		config = CountryConfig(date=date)

		config.log_manager.init(type="tmdb_country")

		# Get the list of country from TMDB and the database
		config.log_manager.fetching_data()
		tmdb_countries, tmdb_countries_set = get_tmdb_countries(config)
		db_countries_set = get_db_countries(config)
		config.log_manager.data_fetched()

		# Compare the languages
		extra_countries: set = db_countries_set - tmdb_countries_set
		missing_countries: set = tmdb_countries_set - db_countries_set

		# Process extra and missing languages
		config.log_manager.syncing_to_db()
		process_extra_countries(config, extra_countries)
		process_missing_countries(config, tmdb_countries, missing_countries)

		config.log_manager.success()
	except Exception as e:
		config.log_manager.failed()
		logger.error(f"Syncing country failed: {e}")

