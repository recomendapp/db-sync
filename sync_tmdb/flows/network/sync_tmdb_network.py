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

from .config import NetworkConfig
from .mapper import Mapper
from ...models.csv_file import CSVFile

# ---------------------------------------------------------------------------- #

# ---------------------------------------------------------------------------- #
#                                    Getters                                   #
# ---------------------------------------------------------------------------- #

def get_tmdb_networks(config: NetworkConfig) -> set:
	try:
		tmdb_networks = config.tmdb_client.get_export_ids(type="tv_network", date=config.date)
		tmdb_networks_set = set([item["id"] for item in tmdb_networks])

		return tmdb_networks_set
	except Exception as e:
		raise ValueError(f"Failed to get TMDB networks: {e}")

def get_db_networks(config: NetworkConfig) -> set:
	conn = config.db_client.get_connection()
	try:
		with conn.cursor() as cursor:
			cursor.execute(f"SELECT id FROM {config.table_network}")
			db_networks = cursor.fetchall()
			db_networks_set = set([item[0] for item in db_networks])
			return db_networks_set
	except Exception as e:
		raise ValueError(f"Failed to get database networks: {e}")
	finally:
		config.db_client.return_connection(conn)

@task(cache_policy=None)
def get_tmdb_network_details(config: NetworkConfig, network_id: int) -> dict:
	try:
		network_details = config.tmdb_client.request(f"network/{network_id}", {"append_to_response": "alternative_names,images"})
		return network_details
	except Exception as e:
		config.logger.error(f"Failed to get network details for {network_id}: {e}")
		return None

# ---------------------------------------------------------------------------- #
	
def process_missing_networks(config: NetworkConfig):
	try:
		if len(config.missing_networks) > 0:
			chunks = list(chunked(config.missing_networks, 100))
			for chunk in chunks:
				network_csv = CSVFile(
					columns=config.network_columns,
					tmp_directory=config.tmp_directory,
					prefix=config.flow_name
				)
				network_image_csv = CSVFile(
					columns=config.network_image_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_image"
				)
				network_alternative_name_csv = CSVFile(
					columns=config.network_alternative_name_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_alternative_name"
				)

				networks_details_futures = get_tmdb_network_details.map(config=config, network_id=chunk)


				for network_details_response in networks_details_futures:
					network_data = network_details_response.result()
					if network_data is not None:
						network_csv.append(rows_data=Mapper.network(network=network_data))
						network_image_csv.append(rows_data=Mapper.network_image(network=network_data))
						network_alternative_name_csv.append(rows_data=Mapper.network_alternative_name(network=network_data))
				
				config.logger.info(f"Pushing networks to the database...")
				push_future = config.push.submit(network_csv=network_csv, network_image_csv=network_image_csv, network_alternative_name_csv=network_alternative_name_csv)
				push_future.result(raise_on_failure=True)
				config.logger.info(f"Succesfully submitted networks to the database")

	except Exception as e:
		raise ValueError(f"Failed to process missing networks: {e}")

# ---------------------------------------------------------------------------- #

@flow(name="sync_tmdb_network", log_prints=True)
def sync_tmdb_network(date: date = date.today()):
	logger = get_run_logger()
	logger.info(f"Syncing network for {date}...")
	try:
		config = NetworkConfig(date=date)
		config.log_manager.init(type="tmdb_network")

		# Get the list of networks from TMDB and the database
		config.log_manager.fetching_data()
		tmdb_networks_set = get_tmdb_networks(config)
		db_networks_set = get_db_networks(config)

		# Compare the networks
		config.extra_networks = db_networks_set - tmdb_networks_set
		config.missing_networks = tmdb_networks_set - db_networks_set
		logger.info(f"Found {len(config.extra_networks)} extra networks and {len(config.missing_networks)} missing networks")
		config.log_manager.data_fetched()

		# Process extra and missing networks
		config.log_manager.syncing_to_db()
		config.prune()
		process_missing_networks(config=config)
		config.log_manager.success()
	except Exception as e:
		config.log_manager.failed()
		raise ValueError(f"Failed to sync network: {e}")

