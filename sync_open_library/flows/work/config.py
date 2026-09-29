from datetime import date
from prefect import task
from prefect.cache_policies import NO_CACHE
import uuid
from ...models.config import Config
from ...models.csv_file import CSVFile
from ...utils.db import insert_into

class WorkConfig(Config):
	def __init__(self, date: date):
		super().__init__(date=date)
		self.flow_name: str = "work"

		# Tables
		self.table_work: str = self.config.get("db_tables", {}).get("work", "book.work")
		self.table_work_author: str = self.config.get("db_tables", {}).get("work_author", "book.work_author")
		self.table_work_subject: str = self.config.get("db_tables", {}).get("work_subject", "book.work_subject")
		self.table_work_cover: str = self.config.get("db_tables", {}).get("work_cover", "book.work_cover")

		# Columns
		self.work_columns: list[str] = [
			"key", "title", "subtitle", "description", "first_sentence",
			"first_publish_date_raw", "first_publish_year", "cover_edition_key",
			"revision", "last_modified",
		]
		self.work_author_columns: list[str] = ["work_key", "author_key", "position"]
		self.work_subject_columns: list[str] = ["work_key", "subject"]
		self.work_cover_columns: list[str] = ["work_key", "cover_id", "position"]

		# On conflict
		self.work_on_conflict: list[str] = ["key"]
		self.work_author_on_conflict: list[str] = ["work_key", "author_key"]
		self.work_subject_on_conflict: list[str] = ["work_key", "subject"]
		self.work_cover_on_conflict: list[str] = ["work_key", "cover_id"]

		# On conflict update
		self.work_on_conflict_update: list[str] = [c for c in self.work_columns if c not in self.work_on_conflict]
		self.work_author_on_conflict_update: list[str] = [c for c in self.work_author_columns if c not in self.work_author_on_conflict]
		self.work_subject_on_conflict_update: list[str] = [c for c in self.work_subject_columns if c not in self.work_subject_on_conflict]
		self.work_cover_on_conflict_update: list[str] = [c for c in self.work_cover_columns if c not in self.work_cover_on_conflict]

	@task(cache_policy=NO_CACHE)
	def prune(self, extra_keys: set):
		"""Prune works that no longer exist in the OL dump"""
		if not extra_keys:
			return
		conn = self.db_client.get_connection()
		try:
			with conn.cursor() as cursor:
				conn.autocommit = False
				try:
					cursor.execute(f"DELETE FROM {self.table_work} WHERE key IN %s", (tuple(extra_keys),))
					conn.commit()
				except Exception:
					conn.rollback()
					raise
		except Exception as e:
			raise ValueError(f"Failed to prune extra works: {e}")
		finally:
			self.db_client.return_connection(conn)

	@task(cache_policy=NO_CACHE)
	def push(
		self,
		work_csv: CSVFile,
		work_author_csv: CSVFile,
		work_subject_csv: CSVFile,
		work_cover_csv: CSVFile,
	):
		"""Push a batch of works to the database"""
		conn = self.db_client.get_connection()
		try:
			work_csv.clean_duplicates(conflict_columns=self.work_on_conflict)
			work_author_csv.clean_duplicates(conflict_columns=self.work_author_on_conflict)
			work_subject_csv.clean_duplicates(conflict_columns=self.work_subject_on_conflict)
			work_cover_csv.clean_duplicates(conflict_columns=self.work_cover_on_conflict)

			with conn.cursor() as cursor:
				try:
					conn.autocommit = False
					temp_work = f"{self.table_work.replace('.', '_')}_temp_{uuid.uuid4().hex}"
					temp_work_author = f"{self.table_work_author.replace('.', '_')}_temp_{uuid.uuid4().hex}"
					temp_work_subject = f"{self.table_work_subject.replace('.', '_')}_temp_{uuid.uuid4().hex}"
					temp_work_cover = f"{self.table_work_cover.replace('.', '_')}_temp_{uuid.uuid4().hex}"

					cursor.execute(f"""
						CREATE TEMP TABLE {temp_work} (LIKE {self.table_work} INCLUDING ALL);
						CREATE TEMP TABLE {temp_work_author} (LIKE {self.table_work_author} INCLUDING ALL);
						CREATE TEMP TABLE {temp_work_subject} (LIKE {self.table_work_subject} INCLUDING ALL);
						CREATE TEMP TABLE {temp_work_cover} (LIKE {self.table_work_cover} INCLUDING ALL);
					""")

					with open(work_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_work} ({','.join(self.work_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(work_author_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_work_author} ({','.join(self.work_author_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(work_subject_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_work_subject} ({','.join(self.work_subject_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(work_cover_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_work_cover} ({','.join(self.work_cover_columns)}) FROM STDIN WITH CSV HEADER", f)

					insert_into(cursor=cursor, table=self.table_work, temp_table=temp_work, columns=self.work_columns, on_conflict=self.work_on_conflict, on_conflict_update=self.work_on_conflict_update)
					# A work can reference an author key that doesn't exist in book.author yet
					# (not synced, or a stale/merged key) — the JOIN silently drops those rows
					# instead of letting the whole batch fail on the FK constraint.
					work_author_select_columns = ",".join(f"t.{c}" for c in self.work_author_columns)
					cursor.execute(f"""
						INSERT INTO {self.table_work_author} ({','.join(self.work_author_columns)})
						SELECT {work_author_select_columns} FROM {temp_work_author} t
						JOIN book.author a ON a.key = t.author_key
						ON CONFLICT ({','.join(self.work_author_on_conflict)}) DO UPDATE SET
							{','.join([f'{c}=EXCLUDED.{c}' for c in self.work_author_on_conflict_update])};
					""")
					insert_into(cursor=cursor, table=self.table_work_subject, temp_table=temp_work_subject, columns=self.work_subject_columns, on_conflict=self.work_subject_on_conflict, on_conflict_update=self.work_subject_on_conflict_update)
					insert_into(cursor=cursor, table=self.table_work_cover, temp_table=temp_work_cover, columns=self.work_cover_columns, on_conflict=self.work_cover_on_conflict, on_conflict_update=self.work_cover_on_conflict_update)

					# Delete outdated child rows: safe to scope to works in this batch since
					# each dump line carries the complete, current state for that work.
					cursor.execute(f"""
						DELETE FROM {self.table_work_author}
						WHERE ({','.join(self.work_author_on_conflict)}) NOT IN (
							SELECT {','.join(self.work_author_on_conflict)} FROM {temp_work_author}
						)
						AND work_key IN (SELECT key FROM {temp_work});
					""")
					cursor.execute(f"""
						DELETE FROM {self.table_work_subject}
						WHERE ({','.join(self.work_subject_on_conflict)}) NOT IN (
							SELECT {','.join(self.work_subject_on_conflict)} FROM {temp_work_subject}
						)
						AND work_key IN (SELECT key FROM {temp_work});
					""")
					cursor.execute(f"""
						DELETE FROM {self.table_work_cover}
						WHERE ({','.join(self.work_cover_on_conflict)}) NOT IN (
							SELECT {','.join(self.work_cover_on_conflict)} FROM {temp_work_cover}
						)
						AND work_key IN (SELECT key FROM {temp_work});
					""")

					conn.commit()

					work_csv.delete()
					work_author_csv.delete()
					work_subject_csv.delete()
					work_cover_csv.delete()
				except Exception:
					conn.rollback()
					raise
				finally:
					conn.autocommit = True
		except Exception as e:
			raise ValueError(f"Failed to push works to the database: {e}")
		finally:
			self.db_client.return_connection(conn)
