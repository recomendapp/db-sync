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
	conn = config.db_client.get_connection()
	try:
		with conn.cursor() as cursor:
			cursor.execute(f"SELECT id FROM {config.table_company}")
			db_companies = cursor.fetchall()
			db_companies_set = set([item[0] for item in db_companies])
			return db_companies_set
	except Exception as e:
		raise ValueError(f"Failed to get database companies: {e}")
	finally:
		config.db_client.return_connection(conn)

@task
def get_tmdb_company_details(config: CompanyConfig, company_id: int) -> dict:
	try:
		company_details = config.tmdb_client.request(f"company/{company_id}")
		company_images = config.tmdb_client.request(f"company/{company_id}/images")
		company_alternative_names = config.tmdb_client.request(f"company/{company_id}/alternative_names")
		return {
			"details": company_details,
			"images": company_images,
			"alternative_names": company_alternative_names
		}
	except Exception as e:
		config.logger.error(f"Failed to get company details for {company_id}: {e}")
		return None

# ---------------------------------------------------------------------------- #
	
def process_missing_companies(config: CompanyConfig):
	try:
		if len(config.missing_companies) > 0:
			chunks = list(chunked(config.missing_companies, 100))
			submits = []
			for chunk in chunks:
				company_csv = CSVFile(
					columns=config.company_columns,
					tmp_directory=config.tmp_directory,
					prefix=config.flow_name
				)
				company_image_csv = CSVFile(
					columns=config.company_image_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_image"
				)
				company_alternative_name_csv = CSVFile(
					columns=config.company_alternative_name_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_alternative_name"
				)

				companies_details_futures = get_tmdb_company_details.map(config=config, company_id=chunk)


				for company_details_response in companies_details_futures:
					company_data = company_details_response.result()
					if company_data is not None:
						company_csv.append(rows_data=Mapper.company(company=company_data["details"]))
						company_image_csv.append(rows_data=Mapper.company_image(company=company_data["images"]))
						company_alternative_name_csv.append(rows_data=Mapper.company_alternative_name(company=company_data["alternative_names"]))
				
				submits.append(config.push.submit(company_csv=company_csv, company_image_csv=company_image_csv, company_alternative_name_csv=company_alternative_name_csv))
			
				break
			# Wait for all the submits to finish
			wait(submits)
	except Exception as e:
		raise ValueError(f"Failed to process missing companies: {e}")

# ---------------------------------------------------------------------------- #

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
		config.extra_companies = db_companies_set - tmdb_companies_set
		config.missing_companies = tmdb_companies_set - db_companies_set
		logger.info(f"Found {len(config.extra_companies)} extra companies and {len(config.missing_companies)} missing companies")
		config.log_manager.data_fetched()

		# Process extra and missing companies
		config.log_manager.syncing_to_db()
		config.prune()
		process_missing_companies(config=config)
		config.log_manager.success()
	except Exception as e:
		config.log_manager.failed()
		raise ValueError(f"Failed to sync company: {e}")

