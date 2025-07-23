from datetime import date
from prefect import task
import uuid
from ...models.config import Config
from ...models.csv_file import CSVFile
from ...utils.db import insert_into

class NetworkConfig(Config):
	def __init__(self, date: date):
		super().__init__(date=date)
		self.flow_name: str = "network"

		# Tables
		self.table_network: str = self.config.get("db_tables", {}).get("network", "tmdb_network")
		self.table_network_image: str = self.config.get("db_tables", {}).get("network_image", "tmdb_network_image")
		self.table_network_alternative_name: str = self.config.get("db_tables", {}).get("network_alternative_name", "tmdb_network_alternative_name")

		# Ids
		self.extra_networks: set = {}
		self.missing_networks: set = {}

		# Columns
		self.network_columns: list[str] = ["id", "name", "headquarters", "homepage", "origin_country"]
		self.network_image_columns: list[str] = ["id", "network", "file_path", "file_type", "aspect_ratio", "height", "width", "vote_average", "vote_count"]
		self.network_alternative_name_columns: list[str] = ["network", "name", "type"]
		
		# On conflict
		self.network_on_conflict: list[str] = ["id"]
		self.network_image_on_conflict: list[str] = ["id"]
		self.network_alternative_name_on_conflict: list[str] = ["network", "name", "type"]

		# On conflict update
		self.network_on_conflict_update: list[str] = [col for col in self.network_columns if col not in self.network_on_conflict]
		self.network_image_on_conflict_update: list[str] = [col for col in self.network_image_columns if col not in self.network_image_on_conflict]
		self.network_alternative_name_on_conflict_update: list[str] = [col for col in self.network_alternative_name_columns if col not in self.network_alternative_name_on_conflict]
	
	@task(cache_policy=None)
	def prune(self):
		"""Prune the extra networks from the database"""
		conn = self.db_client.get_connection()
		try:
			if len(self.extra_networks) > 0:
				with conn.cursor() as cursor:
					conn.autocommit = False
					try:
						cursor.execute(f"DELETE FROM {self.table_network} WHERE id IN %s", (tuple(self.extra_networks),))
						conn.commit()
					except:
						conn.rollback()
						raise
		except Exception as e:
			raise ValueError(f"Failed to prune extra networks: {e}")
		finally:
			self.db_client.return_connection(conn)

	@task(cache_policy=None)
	def push(self, network_csv: CSVFile, network_image_csv: CSVFile, network_alternative_name_csv: CSVFile):
		"""Push the networks to the database"""
		conn = self.db_client.get_connection()
		try:
			# Clean duplicates from the CSV files
			network_csv.clean_duplicates(conflict_columns=self.network_on_conflict)
			network_image_csv.clean_duplicates(conflict_columns=self.network_image_on_conflict)
			network_alternative_name_csv.clean_duplicates(conflict_columns=self.network_alternative_name_on_conflict)

			with conn.cursor() as cursor:
				try:
					conn.autocommit = False
					temp_network = f"temp_{self.table_network}_{uuid.uuid4().hex}"
					temp_network_image = f"temp_{self.table_network_image}_{uuid.uuid4().hex}"
					temp_network_alternative_name = f"temp_{self.table_network_alternative_name}_{uuid.uuid4().hex}"
					cursor.execute(f"""
						CREATE TEMP TABLE {temp_network} (LIKE {self.table_network} INCLUDING ALL);
						CREATE TEMP TABLE {temp_network_image} (LIKE {self.table_network_image} INCLUDING ALL);
						CREATE TEMP TABLE {temp_network_alternative_name} (LIKE {self.table_network_alternative_name} INCLUDING ALL);
					""")

					with open(network_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_network} ({','.join(self.network_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(network_image_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_network_image} ({','.join(self.network_image_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(network_alternative_name_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_network_alternative_name} ({','.join(self.network_alternative_name_columns)}) FROM STDIN WITH CSV HEADER", f)

					# Delete all previous images
					cursor.execute(f"""
						DELETE FROM {self.table_network_image}
						WHERE {self.network_image_columns[1]} IN (
						SELECT id FROM {temp_network}
						)
					""")

					# Delete all previous alternative names
					cursor.execute(f"""
						DELETE FROM {self.table_network_alternative_name}
						WHERE {self.network_alternative_name_columns[0]} IN (
						SELECT id FROM {temp_network}
						)
					""")

					# Insert networks
					insert_into(
						cursor=cursor,
						table=self.table_network,
						temp_table=temp_network,
						columns=self.network_columns,
						on_conflict=self.network_on_conflict,
						on_conflict_update=self.network_on_conflict_update
					)

					# Insert network images
					insert_into(
						cursor=cursor,
						table=self.table_network_image,
						temp_table=temp_network_image,
						columns=self.network_image_columns,
						# on_conflict=self.network_image_on_conflict,
						# on_conflict_update=self.network_image_on_conflict_update
					)

					# Insert network alternative names
					insert_into(
						cursor=cursor,
						table=self.table_network_alternative_name,
						temp_table=temp_network_alternative_name,
						columns=self.network_alternative_name_columns,
						# on_conflict=self.network_alternative_name_on_conflict,
						# on_conflict_update=self.network_alternative_name_on_conflict_update
					)

					conn.commit()

					network_csv.delete()
					network_image_csv.delete()
					network_alternative_name_csv.delete()
				except Exception as e:
					conn.rollback()
					raise
				finally:
					conn.autocommit = True
		except Exception as e:
			raise ValueError(f"Failed to push networks to the database: {e}")
		finally:
			self.db_client.return_connection(conn)
