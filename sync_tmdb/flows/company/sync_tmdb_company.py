# ---------------------------------------------------------------------------- #
#                                    Imports                                   #
# ---------------------------------------------------------------------------- #

from datetime import date
import pandas as pd
from more_itertools import chunked
import time

# ---------------------------------- Prefect --------------------------------- #
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.blocks.system import Secret
from prefect.futures import wait

from ...utils.file_manager import create_csv, get_csv_header
from .config import CompanyConfig
from .mapper import Mapper
from ...models.csv_file import CSVFile

# ---------------------------------------------------------------------------- #

# ---------------------------------------------------------------------------- #
#                                    Getters                                   #
# ---------------------------------------------------------------------------- #

def get_tmdb_companies(config: CompanyConfig) -> set:
	try:
		tmdb_companies = config.tmdb_client.get_export_ids(type="production_company", date=config.date)
		tmdb_companies_set = set([item["id"] for item in tmdb_companies])

		return tmdb_companies_set
	except Exception as e:
		raise ValueError(f"Failed to get TMDB companies: {e}")

def get_db_companies(config: CompanyConfig) -> set:
	try:
		with config.db_client.get_connection() as conn:
			with conn.cursor() as cursor:
				cursor.execute(f"SELECT id FROM {config.table_company}")
				db_companies = cursor.fetchall()
				# delete company with id 3, 5, 8
				db_companies = [item for item in db_companies if item[0] not in [3, 4,5]]

				db_companies_set = set([item[0] for item in db_companies])
				return db_companies_set
	except Exception as e:
		raise ValueError(f"Failed to get database companies: {e}")

@task
def get_tmdb_company_details(config: CompanyConfig, company_id: int) -> dict:
	try:
		company_details = config.tmdb_client.request(f"company/{company_id}")
		return company_details
			
	except Exception as e:
		config.logger.error(f"Failed to get company details for {company_id}: {e}")
		return None

# ---------------------------------------------------------------------------- #

def process_extra_companies(config: CompanyConfig, extra_companies: set):
	try:
		if len(extra_companies) > 0:
			config.logger.warning(f"Found {len(extra_companies)} extra companies in the database")
			with config.db_client.get_connection() as conn:
				with conn.cursor() as cursor:
					conn.autocommit = False
					try:
						cursor.execute(f"DELETE FROM {config.table_company} WHERE id IN %s", (tuple(extra_companies),))
						conn.commit()
					except Exception as e:
						conn.rollback()
						raise
	except Exception as e:
		raise ValueError(f"Failed to process extra companies: {e}")
	
def process_missing_companies(config: CompanyConfig, missing_companies_set: set):
	try:
		if len(missing_companies_set) > 0:
			config.logger.warning(f"Found {len(missing_companies_set)} missing companies in the database")
			submits = []
			chunks = list(chunked(missing_companies_set, config.chunk_size))
			for chunk in chunks:
				company_csv = CSVFile(columns=Mapper.company_columns, tmp_directory=config.tmp_directory, prefix="company")

				companies_details_futures = get_tmdb_company_details.map(config=config, company_id=chunk)

				for company_details_response in companies_details_futures:
					company_details = company_details_response.result()
					if company_details is not None:
						company_csv.append(rows_data=Mapper.company(config=config, company=company_details))
				
				# submits.append(Mapper.push.submit(config=config, company_csv=company_csv))
			
			wait(submits) # wait for all the submits to finish
	except Exception as e:
		raise ValueError(f"Failed to process missing companies: {e}")

@flow(name="sync_tmdb_company", log_prints=True)
def sync_tmdb_company(date: date = date.today()):
	logger = get_run_logger()
	logger.info(f"Syncing company for {date}...")
	try:
		config = CompanyConfig(date=date)

		config.log_manager.init(type="tmdb_company")

		# Get the list of companies from TMDB and the database
		config.log_manager.fetching_data()
		tmdb_companies_set = get_tmdb_companies(config)
		db_companies_set = get_db_companies(config)
	
		# Compare the companies
		extra_companies: set = db_companies_set - tmdb_companies_set
		missing_companies: set = tmdb_companies_set - db_companies_set
		config.log_manager.data_fetched()

		# Process extra and missing companies
		config.log_manager.syncing_to_db()
		process_extra_companies(config, extra_companies)
		process_missing_companies(config, missing_companies)

		config.log_manager.success()
	except Exception as e:
		config.log_manager.failed()
		logger.error(f"Syncing company failed: {e}")

