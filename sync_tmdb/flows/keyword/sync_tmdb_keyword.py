# ---------------------------------------------------------------------------- #
#                                    Imports                                   #
# ---------------------------------------------------------------------------- #

from datetime import date
import pandas as pd

# ---------------------------------- Prefect --------------------------------- #
from prefect import flow
from prefect.logging import get_run_logger

from ...utils.file_manager import create_csv, get_csv_header
from .config import KeywordConfig
from ...models.csv_file import CSVFile

# ---------------------------------------------------------------------------- #

# ---------------------------------------------------------------------------- #
#                                    Getters                                   #
# ---------------------------------------------------------------------------- #

def get_db_keywords(config: KeywordConfig) -> set:
	try:
		with config.db_client.connection() as conn:
			with conn.cursor() as cursor:
				cursor.execute(f"SELECT id FROM {config.table_keyword}")
				return {item[0] for item in cursor}
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

def process_missing_keywords(config: KeywordConfig, missing_keywords: pd.DataFrame):
	try:
		if len(missing_keywords) > 0:
			config.logger.warning(f"Found {len(missing_keywords)} missing keywords in the database")

			keyword_csv = CSVFile(
				columns=['id', 'name'],
				tmp_directory=config.tmp_directory,
				prefix=config.flow_name
			)

			keyword_csv.append(rows_data=missing_keywords)

			# Load the CSV files into the database using copy
			with config.db_client.connection() as conn:
				with conn.cursor() as cursor:
					conn.autocommit = False
					try:
						cursor.execute(f"""
							CREATE TEMP TABLE temp_{config.table_keyword} (LIKE {config.table_keyword} INCLUDING ALL);
						""")

						with open(keyword_csv.file_path, "r") as f:
							cursor.copy_expert(f"COPY temp_{config.table_keyword} ({','.join(get_csv_header(f))}) FROM STDIN WITH CSV HEADER", f)
					
						cursor.execute(f"""
							INSERT INTO {config.table_keyword} (id, name)
							SELECT id, name FROM temp_{config.table_keyword}
							ON CONFLICT (id) DO UPDATE
							SET name = EXCLUDED.name;
						""")
						
						conn.commit()

						keyword_csv.delete()
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
		tmdb_keywords_df = config.tmdb_client.get_export_ids(type="keyword", date=config.date, columns_to_keep=["id", "name"])
		tmdb_keywords_set = set(tmdb_keywords_df["id"])
		db_keywords_set = get_db_keywords(config)
		config.log_manager.data_fetched()

		# Compare the keywords
		extra_keywords: set = db_keywords_set - tmdb_keywords_set
		missing_keywords: set = tmdb_keywords_set - db_keywords_set

		# Process extra and missing keywords
		config.log_manager.syncing_to_db()
		process_extra_keywords(config, extra_keywords)
		process_missing_keywords(config, missing_keywords=tmdb_keywords_df[tmdb_keywords_df["id"].isin(missing_keywords)])

		config.log_manager.success()
	except Exception as e:
		config.log_manager.failed()
		raise ValueError(f"Failed to sync keyword: {e}")

