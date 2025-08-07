# ---------------------------------------------------------------------------- #
#                                    Imports                                   #
# ---------------------------------------------------------------------------- #

from datetime import date

# ---------------------------------- Prefect --------------------------------- #
from prefect import flow
from prefect.logging import get_run_logger

from ...utils.file_manager import create_csv, get_csv_header
from .config import KeywordConfig
from .mappers import Mappers

# ---------------------------------------------------------------------------- #

# ---------------------------------------------------------------------------- #
#                                    Getters                                   #
# ---------------------------------------------------------------------------- #


def get_tmdb_keywords(config: KeywordConfig) -> tuple:
	try:
		tmdb_keywords = config.tmdb_client.get_export_ids(type="keyword", date=config.date)
		tmdb_keywords_set = set([item["id"] for item in tmdb_keywords])

		return tmdb_keywords, tmdb_keywords_set
	except Exception as e:
		raise ValueError(f"Failed to get TMDB keywords: {e}")

def get_db_keywords(config: KeywordConfig) -> set:
	try:
		with config.db_client.connection() as conn:
			with conn.cursor() as cursor:
				cursor.execute(f"SELECT id FROM {config.table_keyword}")
				db_keywords = cursor.fetchall()
				db_keywords_set = set([item[0] for item in db_keywords])
				return db_keywords_set
	except Exception as e:
		raise ValueError(f"Failed to get database keywords: {e}")

# ---------------------------------------------------------------------------- #

def process_extra_keywords(config: KeywordConfig, extra_keywords: set):
	try:
		if len(extra_keywords) > 0:
			config.logger.warning(f"Found {len(extra_keywords)} extra keywords in the database")
			with config.db_client.connection() as conn:
				with conn.cursor() as cursor:
					conn.autocommit = False
					try:
						cursor.execute(f"DELETE FROM {config.table_keyword} WHERE id IN %s", (tuple(extra_keywords),))
						conn.commit()
					except Exception as e:
						conn.rollback()
						raise
	except Exception as e:
		raise ValueError(f"Failed to process extra keywords: {e}")
	
def process_missing_keywords(config: KeywordConfig, tmdb_keywords: list, missing_keywords_set: set):
	try:
		if len(missing_keywords_set) > 0:
			config.logger.warning(f"Found {len(missing_keywords_set)} missing keywords in the database")
		
			# Initialize the mappers
			tmdb_keywords = [keyword for keyword in tmdb_keywords if keyword["id"] in missing_keywords_set]
			mappers = Mappers(keywords=tmdb_keywords)

			# Generate the CSV files
			config.keyword = create_csv(data=mappers.keyword, tmp_directory=config.tmp_directory, prefix="keyword")

			# Load the CSV files into the database using copy
			with config.db_client.connection() as conn:
				with conn.cursor() as cursor:
					conn.autocommit = False
					try:
						cursor.execute(f"""
							CREATE TEMP TABLE temp_{config.table_keyword} (LIKE {config.table_keyword} INCLUDING ALL);
						""")

						with open(config.keyword, "r") as f:
							cursor.copy_expert(f"COPY temp_{config.table_keyword} ({','.join(get_csv_header(f))}) FROM STDIN WITH CSV HEADER", f)
					
						cursor.execute(f"""
							INSERT INTO {config.table_keyword} (id, name)
							SELECT id, name FROM temp_{config.table_keyword}
							ON CONFLICT (id) DO UPDATE
							SET name = EXCLUDED.name;
						""")
						
						conn.commit()
					except Exception as e:
						conn.rollback()
						raise
	except Exception as e:
		raise ValueError(f"Failed to process missing keywords: {e}")
			

@flow(name="sync_tmdb_keyword", log_prints=True)
def sync_tmdb_keyword(date: date = date.today()):
	logger = get_run_logger()
	logger.info(f"Syncing keyword for {date}...")
	config = KeywordConfig(date=date)
	try:
		config.log_manager.init(type="tmdb_keyword")

		# Get the list of keyword from TMDB and the database
		config.log_manager.fetching_data()
		tmdb_keywords, tmdb_keywords_set = get_tmdb_keywords(config)
		db_keywords_set = get_db_keywords(config)
		config.log_manager.data_fetched()

		# Compare the keywords
		extra_keywords: set = db_keywords_set - tmdb_keywords_set
		missing_keywords: set = tmdb_keywords_set - db_keywords_set

		# Process extra and missing keywords
		config.log_manager.syncing_to_db()
		process_extra_keywords(config, extra_keywords)
		process_missing_keywords(config, tmdb_keywords, missing_keywords)

		config.log_manager.success()
	except Exception as e:
		config.log_manager.failed()
		raise ValueError(f"Failed to sync keyword: {e}")

