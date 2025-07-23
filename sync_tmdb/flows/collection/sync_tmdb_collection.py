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
	conn = config.db_client.get_connection()
	try:
		with conn.cursor() as cursor:
			cursor.execute(f"SELECT id FROM {config.table_collection}")
			db_collections = cursor.fetchall()
			db_collections_set = set([item[0] for item in db_collections])
			return db_collections_set
	except Exception as e:
		raise ValueError(f"Failed to get database collections: {e}")
	finally:
		config.db_client.return_connection(conn)

@task(cache_policy=None)
def get_tmdb_collection_details(config: CollectionConfig, collection_id: int) -> dict:
	try:
		collection_details = config.tmdb_client.request(f"collection/{collection_id}")
		collection_translations = config.tmdb_client.request(f"collection/{collection_id}/translations")
		collection_images = config.tmdb_client.request(f"collection/{collection_id}/images")

		return {
			"details": collection_details,
			"translations": collection_translations,
			"images": collection_images
		}
	except Exception as e:
		config.logger.error(f"Failed to get collection details for {collection_id}: {e}")
		return None

# ---------------------------------------------------------------------------- #
	
def process_missing_collections(config: CollectionConfig):
	try:
		if len(config.missing_collections) > 0:
			chunks = list(chunked(config.missing_collections, 100))
			for chunk in chunks:
				collection_csv = CSVFile(
					columns=config.collection_columns,
					tmp_directory=config.tmp_directory,
					prefix=config.flow_name
				)
				collection_translation_csv = CSVFile(
					columns=config.collection_translation_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_translation"
				)
				collection_image_csv = CSVFile(
					columns=config.collection_image_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_image"
				)

				collections_details_futures = get_tmdb_collection_details.map(config=config, collection_id=chunk)

				for collection_details_response in collections_details_futures:
					collection_data = collection_details_response.result()
					if collection_data is not None:
						collection_csv.append(rows_data=Mapper.collection(collection=collection_data["details"]))
						collection_translation_csv.append(rows_data=Mapper.collection_translation(collection_data["translations"]))
						collection_image_csv.append(rows_data=Mapper.collection_image(collection=collection_data["images"]))

				config.logger.info(f"Pushing collections to the database...")
				push_future = config.push.submit(collection_csv=collection_csv, collection_translation_csv=collection_translation_csv, collection_image_csv=collection_image_csv)
				push_future.result(raise_on_failure=True)
				config.logger.info(f"Succesfully submitted collections to the database")

	except Exception as e:
		raise ValueError(f"Failed to process missing collections: {e}")
	
# ---------------------------------------------------------------------------- #

@flow(name="sync_tmdb_collection", log_prints=True)
def sync_tmdb_collection(date: date = date.today()):
	logger = get_run_logger()
	logger.info(f"Syncing collection for {date}...")
	try:
		# with CollectionConfig(date=date) as config:
		config = CollectionConfig(date=date)
		config.log_manager.init(type="tmdb_collection")

		# Get the list of collection from TMDB and the database
		config.log_manager.fetching_data()
		tmdb_collections_set = get_tmdb_collections(config)
		db_collections_set = get_db_collections(config)

		# Compare the collections and process missing collections
		config.extra_collections = db_collections_set - tmdb_collections_set
		config.missing_collections = tmdb_collections_set - db_collections_set
		logger.info(f"Found {len(config.extra_collections)} extra collections and {len(config.missing_collections)} missing collections")
		config.log_manager.data_fetched()

		# Sync the collections to the database
		config.log_manager.syncing_to_db()
		config.prune()
		process_missing_collections(config=config)
		config.log_manager.success()
	except Exception as e:
		config.log_manager.failed()
		raise ValueError(f"Failed to sync collection: {e}")

