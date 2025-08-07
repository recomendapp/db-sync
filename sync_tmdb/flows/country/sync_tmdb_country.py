# ---------------------------------------------------------------------------- #
#                                    Imports                                   #
# ---------------------------------------------------------------------------- #

from datetime import date
from psycopg2.extras import execute_values

# ---------------------------------- Prefect --------------------------------- #
from prefect import flow
from prefect.logging import get_run_logger

from .config import CountryConfig

# ---------------------------------------------------------------------------- #

# ---------------------------------------------------------------------------- #
#                                    Getters                                   #
# ---------------------------------------------------------------------------- #

def get_tmdb_countries(config: CountryConfig) -> set:
	try:
		tmdb_countries = config.tmdb_client.request("configuration/countries")
		tmdb_countries_set = set([item["iso_3166_1"] for item in tmdb_countries])
		return tmdb_countries_set
	except Exception as e:
		raise ValueError(f"Failed to get TMDB countries: {e}")

def get_db_countries(config: CountryConfig) -> set:
	try:
		with config.db_client.connection() as conn:
			with conn.cursor() as cursor:
				cursor.execute(f"SELECT {config.country_column} FROM {config.table_country}")
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
			with config.db_client.connection() as conn:
				with conn.cursor() as cursor:
					conn.autocommit = False
					try:
						cursor.execute(f"DELETE FROM {config.table_country} WHERE {config.country_column} IN %s", (tuple(extra_countries),))
						conn.commit()
					except Exception as e:
						conn.rollback()
						raise
	except Exception as e:
		raise ValueError(f"Failed to process extra countries: {e}")
	
def process_missing_countries(config: CountryConfig, missing_countries_set: set):
	try:
		if len(missing_countries_set) > 0:
			config.logger.warning(f"Found {len(missing_countries_set)} missing countries in the database")

			with config.db_client.connection() as conn:
				with conn.cursor() as cursor:
					conn.autocommit = False
					try:
						execute_values(cursor, f"""
							INSERT INTO {config.table_country} ({config.country_column})
							VALUES %s
							ON CONFLICT ({config.country_column}) DO NOTHING;
						""", [(country,) for country in missing_countries_set])
						
						conn.commit()
					except Exception as e:
						conn.rollback()
						raise
					finally:
						conn.autocommit = True
	except Exception as e:
		raise ValueError(f"Failed to process missing countries: {e}")
			

@flow(name="sync_tmdb_country", log_prints=True)
def sync_tmdb_country(date: date = date.today()):
	logger = get_run_logger()
	logger.info(f"Syncing country for {date}...")
	config = CountryConfig(date=date)
	try:
		config.log_manager.init(type="tmdb_country")

		# Get the list of country from TMDB and the database
		config.log_manager.fetching_data()
		tmdb_countries_set = get_tmdb_countries(config)
		db_countries_set = get_db_countries(config)
		config.log_manager.data_fetched()

		# Compare the countries
		extra_countries: set = db_countries_set - tmdb_countries_set
		missing_countries: set = tmdb_countries_set - db_countries_set

		# Process extra and missing countries
		config.log_manager.syncing_to_db()
		process_extra_countries(config, extra_countries)
		process_missing_countries(config, missing_countries)

		config.log_manager.success()
	except Exception as e:
		config.log_manager.failed()
		raise ValueError(f"Failed to sync country: {e}")

