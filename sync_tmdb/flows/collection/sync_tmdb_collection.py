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
from .config import CollectionConfig
from .mapper import Mapper
from ...models.csv_file import CSVFile

# ---------------------------------------------------------------------------- #

# ---------------------------------------------------------------------------- #
#                                    Getters                                   #
# ---------------------------------------------------------------------------- #

def get_tmdb_collections(config: CollectionConfig) -> set:
	try:
		tmdb_collections = config.tmdb_client.get_export_ids(type="collection", date=config.date)
		tmdb_collections_set = set([item["id"] for item in tmdb_collections])

		return tmdb_collections_set
	except Exception as e:
		raise ValueError(f"Failed to get TMDB collections: {e}")

def get_db_collections(config: CollectionConfig) -> set:
	try:
		with config.db_client.get_connection() as conn:
			with conn.cursor() as cursor:
				cursor.execute(f"SELECT id FROM {config.table_collection}")
				db_collections = cursor.fetchall()
				db_collections_set = set([item[0] for item in db_collections])
				return db_collections_set
	except Exception as e:
		raise ValueError(f"Failed to get database collections: {e}")

@task
def get_tmdb_collection_details(config: CollectionConfig, collection_id: int) -> dict:
	try:
		# Get collection details from TMDB in the default language and the extra languages
		collection_details = {}
		collection_details[config.default_language.code] = config.tmdb_client.request(f"collection/{collection_id}", {"language": config.default_language.tmdb_language})

		for language in config.extra_languages:
			collection_details[language.code] = config.tmdb_client.request(f"collection/{collection_id}", {"language": language.tmdb_language})
		
		return collection_details
			
	except Exception as e:
		config.logger.error(f"Failed to get collection details for {collection_id}: {e}")
		return None

# ---------------------------------------------------------------------------- #

def process_extra_collections(config: CollectionConfig, extra_collections: set):
	try:
		if len(extra_collections) > 0:
			config.logger.warning(f"Found {len(extra_collections)} extra collections in the database")
			with config.db_client.get_connection() as conn:
				with conn.cursor() as cursor:
					conn.autocommit = False
					try:
						cursor.execute(f"DELETE FROM {config.table_collection} WHERE id IN %s", (tuple(extra_collections),))
						conn.commit()
					except Exception as e:
						conn.rollback()
						raise
	except Exception as e:
		raise ValueError(f"Failed to process extra collections: {e}")
	
def process_missing_collections(config: CollectionConfig, missing_collections_set: set):
	try:
		if len(missing_collections_set) > 0:
			config.logger.warning(f"Found {len(missing_collections_set)} missing collections in the database")
			submits = []
			chunks = list(chunked(missing_collections_set, config.chunk_size))
			for chunk in chunks:
				collection_csv = CSVFile(columns=Mapper.collection_columns, tmp_directory=config.tmp_directory, prefix="collection")
				collection_translation_csv = CSVFile(columns=Mapper.collection_translation_columns, tmp_directory=config.tmp_directory, prefix="collection_translation")

				collections_details_futures = get_tmdb_collection_details.map(config=config, collection_id=chunk)

				for collection_details_response in collections_details_futures:
					collection_details = collection_details_response.result()
					if collection_details is not None:
						collection_csv.append(rows_data=Mapper.collection(config=config, collection=collection_details))
						collection_translation_csv.append(rows_data=Mapper.collection_translation(collection_details))
				
				submits.append(Mapper.push.submit(config=config, collection_csv=collection_csv, collection_translation_csv=collection_translation_csv))
			
			wait(submits) # wait for all the submits to finish
	except Exception as e:
		raise ValueError(f"Failed to process missing collections: {e}")

@flow(name="sync_tmdb_collection", log_prints=True)
def sync_tmdb_collection(date: date = date.today()):
	logger = get_run_logger()
	logger.info(f"Syncing collection for {date}...")
	try:
		config = CollectionConfig(date=date)

		config.log_manager.init(type="tmdb_collection")

		# Get the list of collection from TMDB and the database
		config.log_manager.fetching_data()
		tmdb_collections_set = get_tmdb_collections(config)
		db_collections_set = get_db_collections(config)
	
		# Compare the collections
		extra_collections: set = db_collections_set - tmdb_collections_set
		missing_collections: set = tmdb_collections_set - db_collections_set
		config.log_manager.data_fetched()

		# Process extra and missing collections
		config.log_manager.syncing_to_db()
		process_extra_collections(config, extra_collections)
		process_missing_collections(config, missing_collections)

		config.log_manager.success()
	except Exception as e:
		config.log_manager.failed()
		logger.error(f"Syncing collection failed: {e}")

