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
from prefect.futures import wait

from .config import PersonConfig
from .mapper import Mapper
from ...models.csv_file import CSVFile

# ---------------------------------------------------------------------------- #

# ---------------------------------------------------------------------------- #
#                                    Getters                                   #
# ---------------------------------------------------------------------------- #

def get_tmdb_persons(config: PersonConfig) -> set:
	try:
		tmdb_persons = config.tmdb_client.get_export_ids(type="person", date=config.date)
		tmdb_persons_set = set([item["id"] for item in tmdb_persons])
		return tmdb_persons_set
	except Exception as e:
		raise ValueError(f"Failed to get TMDB persons: {e}")

def get_db_persons(config: PersonConfig) -> set:
	conn = config.db_client.get_connection()
	try:
		with conn.cursor() as cursor:
			cursor.execute(f"SELECT id FROM {config.table_person}")
			db_persons = cursor.fetchall()
			db_persons_set = set([item[0] for item in db_persons])
			return db_persons_set
	except Exception as e:
		raise ValueError(f"Failed to get database persons: {e}")
	finally:
		config.db_client.return_connection(conn)
	
def get_tmdb_persons_changed(config: PersonConfig):
	try:
		config.logger.info("Getting changed persons...")
		changed_persons = config.tmdb_client.get_changed_ids(type="person", start_date=config.log_manager.last_success_log.date, end_date=config.date)
		config.missing_persons |= changed_persons
	except Exception as e:
		raise ValueError(f"Failed to get changed persons: {e}")

@task(cache_policy=None)
def get_tmdb_person_details(config: PersonConfig, person_id: int) -> dict:
	try:
		# Get persons details from TMDB in the default language and the extra languages
		person = config.tmdb_client.request(f"person/{person_id}", {"append_to_response": "images,external_ids,translations"})

		return person
	except Exception as e:
		config.logger.error(f"Failed to get person details for {person_id}: {e}")
		return None

# ---------------------------------------------------------------------------- #
	
def process_missing_persons(config: PersonConfig):
	try:
		if len(config.missing_persons) > 0:
			chunks = list(chunked(config.missing_persons, 500))
			for chunk in chunks:
				person_csv = CSVFile(
					columns=config.person_columns,
					tmp_directory=config.tmp_directory,
					prefix=config.flow_name
				)
				person_translation_csv = CSVFile(
					columns=config.person_translation_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_translation"
				)
				person_image_csv = CSVFile(
					columns=config.person_image_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_image"
				)
				person_external_id_csv = CSVFile(
					columns=config.person_external_id_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_external_ids"
				)
				person_also_known_as_csv = CSVFile(
					columns=config.person_also_known_as_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_also_known_as"
				)

				persons_details_futures = get_tmdb_person_details.map(config=config, person_id=chunk)

				for person_details_response in persons_details_futures:
					person_details = person_details_response.result()
					if person_details is not None:
						person_csv.append(rows_data=Mapper.person(person=person_details))
						person_translation_csv.append(rows_data=Mapper.person_translation(person=person_details))
						person_image_csv.append(rows_data=Mapper.person_image(person=person_details))
						person_external_id_csv.append(rows_data=Mapper.person_external_id(person=person_details))
						person_also_known_as_csv.append(rows_data=Mapper.person_also_known_as(person=person_details))
				
				# submits.append(config.push.submit(person_csv=person_csv, person_translation_csv=person_translation_csv, person_image_csv=person_image_csv, person_external_id_csv=person_external_id_csv, person_also_known_as_csv=person_also_known_as_csv))
				config.logger.info(f"Push persons to the database...")
				push_future = config.push.submit(person_csv=person_csv, person_translation_csv=person_translation_csv, person_image_csv=person_image_csv, person_external_id_csv=person_external_id_csv, person_also_known_as_csv=person_also_known_as_csv)
				push_future.result(raise_on_failure=True)
				config.logger.info(f"Succesfully submitted persons to the database")

	except Exception as e:
		raise ValueError(f"Failed to process missing persons: {e}")
	
# ---------------------------------------------------------------------------- #

@flow(name="sync_tmdb_person", log_prints=True)
def sync_tmdb_person(date: date = date.today()):
	logger = get_run_logger()
	logger.info(f"Syncing person for {date}...")
	try:
		config = PersonConfig(date=date)
		config.log_manager.init(type="tmdb_person")

		# Get the list of person from TMDB and the database
		config.log_manager.fetching_data()
		tmdb_persons_set = get_tmdb_persons(config)
		db_persons_set = get_db_persons(config)

		# Compare the persons and process missing persons
		config.extra_persons = db_persons_set - tmdb_persons_set
		config.missing_persons = tmdb_persons_set - db_persons_set
		get_tmdb_persons_changed(config)
		logger.info(f"Found {len(config.extra_persons)} extra persons and {len(config.missing_persons)} missing persons")
		config.log_manager.data_fetched()

		# Sync the persons to the database
		config.log_manager.syncing_to_db()
		config.prune()
		process_missing_persons(config=config)
		config.log_manager.success()
	except Exception as e:
		config.log_manager.failed()
		raise ValueError(f"Failed to sync person: {e}")

