from datetime import date
from prefect import task
from prefect.cache_policies import NO_CACHE
import uuid
from ...models.config import Config
from ...models.csv_file import CSVFile
from ...utils.db import insert_into

class EditionConfig(Config):
	def __init__(self, date: date):
		super().__init__(date=date)
		self.flow_name: str = "edition"

		# Tables
		self.table_edition: str = self.config.get("db_tables", {}).get("edition", "book.edition")
		self.table_edition_isbn: str = self.config.get("db_tables", {}).get("edition_isbn", "book.edition_isbn")
		self.table_edition_language: str = self.config.get("db_tables", {}).get("edition_language", "book.edition_language")
		self.table_edition_genre: str = self.config.get("db_tables", {}).get("edition_genre", "book.edition_genre")
		self.table_edition_cover: str = self.config.get("db_tables", {}).get("edition_cover", "book.edition_cover")

		# Columns
		self.edition_columns: list[str] = [
			"key", "work_key", "title", "subtitle", "edition_name",
			"publish_date_raw", "publish_date", "publish_year", "publish_precision",
			"number_of_pages", "physical_format", "translation_of", "ocaid",
			"revision", "last_modified",
		]
		self.edition_isbn_columns: list[str] = ["edition_key", "isbn", "type"]
		self.edition_language_columns: list[str] = ["edition_key", "language"]
		self.edition_genre_columns: list[str] = ["edition_key", "genre"]
		self.edition_cover_columns: list[str] = ["edition_key", "cover_id", "position"]

		# On conflict
		self.edition_on_conflict: list[str] = ["key"]
		self.edition_isbn_on_conflict: list[str] = ["isbn"]
		self.edition_language_on_conflict: list[str] = ["edition_key", "language"]
		self.edition_genre_on_conflict: list[str] = ["edition_key", "genre"]
		self.edition_cover_on_conflict: list[str] = ["edition_key", "cover_id"]

		# On conflict update
		self.edition_on_conflict_update: list[str] = [c for c in self.edition_columns if c not in self.edition_on_conflict]
		self.edition_isbn_on_conflict_update: list[str] = [c for c in self.edition_isbn_columns if c not in self.edition_isbn_on_conflict]
		self.edition_language_on_conflict_update: list[str] = [c for c in self.edition_language_columns if c not in self.edition_language_on_conflict]
		self.edition_genre_on_conflict_update: list[str] = [c for c in self.edition_genre_columns if c not in self.edition_genre_on_conflict]
		self.edition_cover_on_conflict_update: list[str] = [c for c in self.edition_cover_columns if c not in self.edition_cover_on_conflict]

	@task(cache_policy=NO_CACHE)
	def prune(self, extra_keys: set):
		"""Prune editions that no longer exist in the OL dump"""
		if not extra_keys:
			return
		conn = self.db_client.get_connection()
		try:
			with conn.cursor() as cursor:
				conn.autocommit = False
				try:
					cursor.execute(f"DELETE FROM {self.table_edition} WHERE key IN %s", (tuple(extra_keys),))
					conn.commit()
				except Exception:
					conn.rollback()
					raise
		except Exception as e:
			raise ValueError(f"Failed to prune extra editions: {e}")
		finally:
			self.db_client.return_connection(conn)

	@task(cache_policy=NO_CACHE)
	def push(
		self,
		edition_csv: CSVFile,
		edition_isbn_csv: CSVFile,
		edition_language_csv: CSVFile,
		edition_genre_csv: CSVFile,
		edition_cover_csv: CSVFile,
	):
		"""Push a batch of editions to the database"""
		conn = self.db_client.get_connection()
		try:
			edition_csv.clean_duplicates(conflict_columns=self.edition_on_conflict)
			edition_isbn_csv.clean_duplicates(conflict_columns=self.edition_isbn_on_conflict)
			edition_language_csv.clean_duplicates(conflict_columns=self.edition_language_on_conflict)
			edition_genre_csv.clean_duplicates(conflict_columns=self.edition_genre_on_conflict)
			edition_cover_csv.clean_duplicates(conflict_columns=self.edition_cover_on_conflict)

			with conn.cursor() as cursor:
				try:
					conn.autocommit = False
					temp_edition = f"{self.table_edition.replace('.', '_')}_temp_{uuid.uuid4().hex}"
					temp_edition_isbn = f"{self.table_edition_isbn.replace('.', '_')}_temp_{uuid.uuid4().hex}"
					temp_edition_language = f"{self.table_edition_language.replace('.', '_')}_temp_{uuid.uuid4().hex}"
					temp_edition_genre = f"{self.table_edition_genre.replace('.', '_')}_temp_{uuid.uuid4().hex}"
					temp_edition_cover = f"{self.table_edition_cover.replace('.', '_')}_temp_{uuid.uuid4().hex}"

					cursor.execute(f"""
						CREATE TEMP TABLE {temp_edition} (LIKE {self.table_edition} INCLUDING ALL);
						CREATE TEMP TABLE {temp_edition_isbn} (LIKE {self.table_edition_isbn} INCLUDING ALL);
						CREATE TEMP TABLE {temp_edition_language} (LIKE {self.table_edition_language} INCLUDING ALL);
						CREATE TEMP TABLE {temp_edition_genre} (LIKE {self.table_edition_genre} INCLUDING ALL);
						CREATE TEMP TABLE {temp_edition_cover} (LIKE {self.table_edition_cover} INCLUDING ALL);
					""")

					with open(edition_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_edition} ({','.join(self.edition_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(edition_isbn_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_edition_isbn} ({','.join(self.edition_isbn_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(edition_language_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_edition_language} ({','.join(self.edition_language_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(edition_genre_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_edition_genre} ({','.join(self.edition_genre_columns)}) FROM STDIN WITH CSV HEADER", f)
					with open(edition_cover_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY {temp_edition_cover} ({','.join(self.edition_cover_columns)}) FROM STDIN WITH CSV HEADER", f)

					# work_key is nulled out when it doesn't resolve to a known work (missing from
					# book.work, e.g. not yet synced or referencing a stale/merged key) instead of
					# letting the whole edition row fail on the FK constraint.
					edition_select_columns = ",".join(
						"CASE WHEN w.key IS NOT NULL THEN t.work_key ELSE NULL END" if c == "work_key" else f"t.{c}"
						for c in self.edition_columns
					)
					cursor.execute(f"""
						INSERT INTO {self.table_edition} ({','.join(self.edition_columns)})
						SELECT {edition_select_columns}
						FROM {temp_edition} t
						LEFT JOIN book.work w ON w.key = t.work_key
						ON CONFLICT ({','.join(self.edition_on_conflict)}) DO UPDATE SET
							{','.join([f'{c}=EXCLUDED.{c}' for c in self.edition_on_conflict_update])};
					""")
					insert_into(cursor=cursor, table=self.table_edition_isbn, temp_table=temp_edition_isbn, columns=self.edition_isbn_columns, on_conflict=self.edition_isbn_on_conflict, on_conflict_update=self.edition_isbn_on_conflict_update)
					insert_into(cursor=cursor, table=self.table_edition_language, temp_table=temp_edition_language, columns=self.edition_language_columns, on_conflict=self.edition_language_on_conflict, on_conflict_update=self.edition_language_on_conflict_update)
					insert_into(cursor=cursor, table=self.table_edition_genre, temp_table=temp_edition_genre, columns=self.edition_genre_columns, on_conflict=self.edition_genre_on_conflict, on_conflict_update=self.edition_genre_on_conflict_update)
					insert_into(cursor=cursor, table=self.table_edition_cover, temp_table=temp_edition_cover, columns=self.edition_cover_columns, on_conflict=self.edition_cover_on_conflict, on_conflict_update=self.edition_cover_on_conflict_update)

					# Delete outdated child rows: safe to scope to editions in this batch since
					# each dump line carries the complete, current state for that edition.
					cursor.execute(f"""
						DELETE FROM {self.table_edition_isbn}
						WHERE ({','.join(self.edition_isbn_on_conflict)}) NOT IN (
							SELECT {','.join(self.edition_isbn_on_conflict)} FROM {temp_edition_isbn}
						)
						AND edition_key IN (SELECT key FROM {temp_edition});
					""")
					cursor.execute(f"""
						DELETE FROM {self.table_edition_language}
						WHERE ({','.join(self.edition_language_on_conflict)}) NOT IN (
							SELECT {','.join(self.edition_language_on_conflict)} FROM {temp_edition_language}
						)
						AND edition_key IN (SELECT key FROM {temp_edition});
					""")
					cursor.execute(f"""
						DELETE FROM {self.table_edition_genre}
						WHERE ({','.join(self.edition_genre_on_conflict)}) NOT IN (
							SELECT {','.join(self.edition_genre_on_conflict)} FROM {temp_edition_genre}
						)
						AND edition_key IN (SELECT key FROM {temp_edition});
					""")
					cursor.execute(f"""
						DELETE FROM {self.table_edition_cover}
						WHERE ({','.join(self.edition_cover_on_conflict)}) NOT IN (
							SELECT {','.join(self.edition_cover_on_conflict)} FROM {temp_edition_cover}
						)
						AND edition_key IN (SELECT key FROM {temp_edition});
					""")

					conn.commit()

					edition_csv.delete()
					edition_isbn_csv.delete()
					edition_language_csv.delete()
					edition_genre_csv.delete()
					edition_cover_csv.delete()
				except Exception:
					conn.rollback()
					raise
				finally:
					conn.autocommit = True
		except Exception as e:
			raise ValueError(f"Failed to push editions to the database: {e}")
		finally:
			self.db_client.return_connection(conn)
