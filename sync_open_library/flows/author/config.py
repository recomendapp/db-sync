from datetime import date
from prefect import task
from prefect.cache_policies import NO_CACHE
import uuid
from ...models.config import Config
from ...models.csv_file import CSVFile
from ...utils.db import insert_into

class AuthorConfig(Config):
	def __init__(self, date: date):
		super().__init__(date=date)
		self.flow_name: str = "author"

		# Tables
		self.table_author: str = self.config.get("db_tables", {}).get("author", "book.author")
		self.table_author_alternate_name: str = self.config.get("db_tables", {}).get("author_alternate_name", "book.author_alternate_name")
		self.table_author_external_id: str = self.config.get("db_tables", {}).get("author_external_id", "book.author_external_id")
		self.table_author_photo: str = self.config.get("db_tables", {}).get("author_photo", "book.author_photo")

		# Columns
		self.author_columns: list[str] = [
			"key", "name", "personal_name", "title", "bio", "wikipedia",
			"birth_date_raw", "birth_date", "birth_year", "birth_precision",
			"death_date_raw", "death_date", "death_year", "death_precision",
			"revision", "last_modified",
		]
		self.author_alternate_name_columns: list[str] = ["author_key", "name"]
		self.author_external_id_columns: list[str] = ["author_key", "source", "value"]
		self.author_photo_columns: list[str] = ["author_key", "photo_id", "position"]

		# On conflict
		self.author_on_conflict: list[str] = ["key"]
		self.author_alternate_name_on_conflict: list[str] = ["author_key", "name"]
		self.author_external_id_on_conflict: list[str] = ["author_key", "source"]
		self.author_photo_on_conflict: list[str] = ["author_key", "photo_id"]

		# On conflict update
		self.author_on_conflict_update: list[str] = [c for c in self.author_columns if c not in self.author_on_conflict]
		self.author_alternate_name_on_conflict_update: list[str] = [c for c in self.author_alternate_name_columns if c not in self.author_alternate_name_on_conflict]
		self.author_external_id_on_conflict_update: list[str] = [c for c in self.author_external_id_columns if c not in self.author_external_id_on_conflict]
		self.author_photo_on_conflict_update: list[str] = [c for c in self.author_photo_columns if c not in self.author_photo_on_conflict]

	@task(cache_policy=NO_CACHE)
	def prune(self, extra_keys: set):
		"""Prune authors that no longer exist in the OL dump"""
		if not extra_keys:
			return
		conn = self.db_client.get_connection()
		try:
			with conn.cursor() as cursor:
				conn.autocommit = False
				try:
					cursor.execute(f"DELETE FROM {self.table_author} WHERE key IN %s", (tuple(extra_keys),))
					conn.commit()
				except Exception:
					conn.rollback()
					raise
		except Exception as e:
			raise ValueError(f"Failed to prune extra authors: {e}")
		finally:
			self.db_client.return_connection(conn)

	@task(cache_policy=NO_CACHE)
	def push(
		self,
		author_csv: CSVFile,
		author_alternate_name_csv: CSVFile,
		author_external_id_csv: CSVFile,
		author_photo_csv: CSVFile,
	):
		"""Push a batch of authors to the database"""
		conn = self.db_client.get_connection()
		try:
			author_csv.clean_duplicates(conflict_columns=self.author_on_conflict)
			author_alternate_name_csv.clean_duplicates(conflict_columns=self.author_alternate_name_on_conflict)
			author_external_id_csv.clean_duplicates(conflict_columns=self.author_external_id_on_conflict)
			author_photo_csv.clean_duplicates(conflict_columns=self.author_photo_on_conflict)

			with conn.cursor() as cursor:
				try:
					conn.autocommit = False
					temp_author = f"{self.table_author.replace('.', '_')}_temp_{uuid.uuid4().hex}"
					temp_author_alternate_name = f"{self.table_author_alternate_name.replace('.', '_')}_temp_{uuid.uuid4().hex}"
					temp_author_external_id = f"{self.table_author_external_id.replace('.', '_')}_temp_{uuid.uuid4().hex}"
					temp_author_photo = f"{self.table_author_photo.replace('.', '_')}_temp_{uuid.uuid4().hex}"

					cursor.execute(f"""
						CREATE TEMP TABLE {temp_author} (LIKE {self.table_author} INCLUDING ALL);
						CREATE TEMP TABLE {temp_author_alternate_name} (LIKE {self.table_author_alternate_name} INCLUDING ALL);
						CREATE TEMP TABLE {temp_author_external_id} (LIKE {self.table_author_external_id} INCLUDING ALL);
						CREATE TEMP TABLE {temp_author_photo} (LIKE {self.table_author_photo} INCLUDING ALL);
					""")

					with open(author_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_author} ({','.join(self.author_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(author_alternate_name_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_author_alternate_name} ({','.join(self.author_alternate_name_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(author_external_id_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_author_external_id} ({','.join(self.author_external_id_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(author_photo_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_author_photo} ({','.join(self.author_photo_columns)}) FROM STDIN WITH CSV HEADER", f)

					insert_into(cursor=cursor, table=self.table_author, temp_table=temp_author, columns=self.author_columns, on_conflict=self.author_on_conflict, on_conflict_update=self.author_on_conflict_update)
					insert_into(cursor=cursor, table=self.table_author_alternate_name, temp_table=temp_author_alternate_name, columns=self.author_alternate_name_columns, on_conflict=self.author_alternate_name_on_conflict, on_conflict_update=self.author_alternate_name_on_conflict_update)
					insert_into(cursor=cursor, table=self.table_author_external_id, temp_table=temp_author_external_id, columns=self.author_external_id_columns, on_conflict=self.author_external_id_on_conflict, on_conflict_update=self.author_external_id_on_conflict_update)
					insert_into(cursor=cursor, table=self.table_author_photo, temp_table=temp_author_photo, columns=self.author_photo_columns, on_conflict=self.author_photo_on_conflict, on_conflict_update=self.author_photo_on_conflict_update)

					# Delete outdated child rows: safe to scope to authors in this batch since
					# each dump line carries the complete, current state for that author.
					cursor.execute(f"""
						DELETE FROM {self.table_author_alternate_name}
						WHERE ({','.join(self.author_alternate_name_on_conflict)}) NOT IN (
							SELECT {','.join(self.author_alternate_name_on_conflict)} FROM {temp_author_alternate_name}
						)
						AND author_key IN (SELECT key FROM {temp_author});
					""")
					cursor.execute(f"""
						DELETE FROM {self.table_author_external_id}
						WHERE ({','.join(self.author_external_id_on_conflict)}) NOT IN (
							SELECT {','.join(self.author_external_id_on_conflict)} FROM {temp_author_external_id}
						)
						AND author_key IN (SELECT key FROM {temp_author});
					""")
					cursor.execute(f"""
						DELETE FROM {self.table_author_photo}
						WHERE ({','.join(self.author_photo_on_conflict)}) NOT IN (
							SELECT {','.join(self.author_photo_on_conflict)} FROM {temp_author_photo}
						)
						AND author_key IN (SELECT key FROM {temp_author});
					""")

					conn.commit()

					author_csv.delete()
					author_alternate_name_csv.delete()
					author_external_id_csv.delete()
					author_photo_csv.delete()
				except Exception:
					conn.rollback()
					raise
				finally:
					conn.autocommit = True
		except Exception as e:
			raise ValueError(f"Failed to push authors to the database: {e}")
		finally:
			self.db_client.return_connection(conn)
