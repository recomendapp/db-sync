from datetime import date
import gc
import os

import pandas as pd
from prefect import flow
from prefect.logging import get_run_logger

from .config import EditionConfig
from .mapper import Mapper
from ...models.csv_file import CSVFile
from ...utils.file_manager import download_file
from ...utils.dump_reader import iter_dump_records
from ...utils.redirects import build_redirect_map, load_redirect_map, apply_redirects
from ...utils.dataframes import with_nullable_int_columns

# ---------------------------------------------------------------------------- #
#                                    Getters                                   #
# ---------------------------------------------------------------------------- #

def get_db_edition_keys(config: EditionConfig) -> set:
	conn = config.db_client.get_connection()
	try:
		with conn.cursor() as cursor:
			cursor.execute(f"SELECT key FROM {config.table_edition}")
			return {row[0] for row in cursor}
	except Exception as e:
		raise ValueError(f"Failed to get database editions: {e}")
	finally:
		config.db_client.return_connection(conn)

# ---------------------------------------------------------------------------- #

def push_batch(config: EditionConfig, batch: list[dict]):
	if not batch:
		return

	# One DataFrame per table for the whole batch: at OL's scale (tens of millions of
	# rows), writing per-record would mean hundreds of thousands of individual to_csv
	# calls per batch instead of 5.
	edition_rows = [Mapper.edition(r) for r in batch]
	isbn_rows = [row for r in batch for row in Mapper.edition_isbn(r)]
	language_rows = [row for r in batch for row in Mapper.edition_language(r)]
	genre_rows = [row for r in batch for row in Mapper.edition_genre(r)]
	cover_rows = [row for r in batch for row in Mapper.edition_cover(r)]

	edition_csv = CSVFile(columns=config.edition_columns, tmp_directory=config.tmp_directory, prefix=config.flow_name)
	edition_isbn_csv = CSVFile(columns=config.edition_isbn_columns, tmp_directory=config.tmp_directory, prefix=f"{config.flow_name}_isbn")
	edition_language_csv = CSVFile(columns=config.edition_language_columns, tmp_directory=config.tmp_directory, prefix=f"{config.flow_name}_language")
	edition_genre_csv = CSVFile(columns=config.edition_genre_columns, tmp_directory=config.tmp_directory, prefix=f"{config.flow_name}_genre")
	edition_cover_csv = CSVFile(columns=config.edition_cover_columns, tmp_directory=config.tmp_directory, prefix=f"{config.flow_name}_cover")

	edition_df = with_nullable_int_columns(
		pd.DataFrame(edition_rows, columns=config.edition_columns), ["publish_year", "number_of_pages"]
	)
	edition_csv.append(rows_data=edition_df)
	edition_isbn_csv.append(rows_data=pd.DataFrame(isbn_rows, columns=config.edition_isbn_columns))
	edition_language_csv.append(rows_data=pd.DataFrame(language_rows, columns=config.edition_language_columns))
	edition_genre_csv.append(rows_data=pd.DataFrame(genre_rows, columns=config.edition_genre_columns))
	edition_cover_csv.append(rows_data=pd.DataFrame(cover_rows, columns=config.edition_cover_columns))

	push_future = config.push.submit(
		edition_csv=edition_csv,
		edition_isbn_csv=edition_isbn_csv,
		edition_language_csv=edition_language_csv,
		edition_genre_csv=edition_genre_csv,
		edition_cover_csv=edition_cover_csv,
	)
	push_future.result(raise_on_failure=True)

def process_dump(config: EditionConfig, dump_path: str, max_records: int | None = None) -> set:
	"""
	Stream the edition dump and upsert it chunk by chunk. Returns every key seen.

	max_records stops the stream early after that many records — for smoke-testing
	against a real dump without processing it in full. The caller must not prune
	when it's set: seen_keys is then only a partial view of the dump, not the
	complete current state, so anything "missing" from it isn't really gone.
	"""
	seen_keys: set = set()
	batch: list[dict] = []

	for record in iter_dump_records(dump_path, record_type="/type/edition"):
		seen_keys.add(record["key"])
		batch.append(record)

		if len(batch) >= config.chunk_size:
			push_batch(config, batch)
			batch = []
			gc.collect()

		if max_records is not None and len(seen_keys) >= max_records:
			break

	push_batch(config, batch)
	return seen_keys

# ---------------------------------------------------------------------------- #

def apply_edition_redirects(config: EditionConfig, redirects_path: str, known_keys: set):
	"""
	Repoint every table referencing an edition that has since been merged into another
	one before that edition's now-empty row gets pruned. OL edition keys live under
	/books/ (a historical naming quirk, unrelated to our own book schema name).
	"""
	redirect_csv = build_redirect_map(config, redirects_path, collection="books", known_keys=known_keys)
	if not redirect_csv:
		return

	conn = config.db_client.get_connection()
	try:
		with conn.cursor() as cursor:
			conn.autocommit = False
			try:
				temp_table = load_redirect_map(cursor, redirect_csv)

				# isbn is globally unique on its own: remapping edition_key can't collide.
				apply_redirects(cursor, temp_table, config.table_edition_isbn, "edition_key", None)
				apply_redirects(cursor, temp_table, config.table_edition_language, "edition_key", ["language"])
				apply_redirects(cursor, temp_table, config.table_edition_genre, "edition_key", ["genre"])
				apply_redirects(cursor, temp_table, config.table_edition_cover, "edition_key", ["cover_id"])
				# Soft/unconstrained pointer, kept accurate on a best-effort basis.
				apply_redirects(cursor, temp_table, "book.work", "cover_edition_key", None)

				conn.commit()
			except Exception:
				conn.rollback()
				raise
			finally:
				conn.autocommit = True
	except Exception as e:
		raise ValueError(f"Failed to apply edition redirects: {e}")
	finally:
		config.db_client.return_connection(conn)
		redirect_csv.delete()

# ---------------------------------------------------------------------------- #

@flow(name="sync_open_library_edition", log_prints=True)
def sync_open_library_edition(
	date: date = date.today(),
	dump_path: str | None = None,
	redirects_path: str | None = None,
	max_records: int | None = None,
):
	"""
	Sync Open Library editions into book.edition (+ isbn, language, genre, covers).

	Should run after sync_open_library_work: an edition's work_key is nulled out at
	insert time when it doesn't resolve to a known work (see EditionConfig.push), so
	running before works exist just means fewer editions get their work link on the
	first pass — corrected automatically on the next monthly run.

	dump_path/redirects_path let the orchestrator (sync_open_library.py) pass already-
	downloaded dumps so this flow doesn't re-download them when run as part of the full sync.

	max_records is for smoke-testing against a real dump: it stops the stream early and,
	since seen_keys is then only a partial view, also skips pruning for this run.
	"""
	logger = get_run_logger()
	logger.info(f"Syncing Open Library editions for {date}...")
	config = EditionConfig(date=date)
	downloaded_dump_here = dump_path is None
	downloaded_redirects_here = redirects_path is None
	try:
		config.log_manager.init(type="edition")

		config.log_manager.fetching_data()
		local_dump_path = dump_path or download_file(
			config.dump_urls["edition"], tmp_directory=config.tmp_directory, prefix="ol_dump_editions"
		)
		local_redirects_path = redirects_path or download_file(
			config.dump_urls["redirects"], tmp_directory=config.tmp_directory, prefix="ol_dump_redirects"
		)
		config.log_manager.data_fetched()

		config.log_manager.syncing_to_db()
		seen_keys = process_dump(config, local_dump_path, max_records=max_records)

		db_keys = get_db_edition_keys(config)
		logger.info("Applying edition redirects...")
		apply_edition_redirects(config, local_redirects_path, known_keys=db_keys)

		if max_records is not None:
			logger.info(f"max_records set ({max_records}): skipping pruning for this run.")
		else:
			extra_keys = db_keys - seen_keys
			logger.info(f"Found {len(seen_keys)} editions in the dump, pruning {len(extra_keys)} that no longer exist")
			config.prune(extra_keys)

		if downloaded_dump_here and os.path.exists(local_dump_path):
			os.remove(local_dump_path)
		if downloaded_redirects_here and os.path.exists(local_redirects_path):
			os.remove(local_redirects_path)

		config.log_manager.success()
		logger.info(f"Successfully synced {len(seen_keys)} Open Library editions.")
	except Exception as e:
		config.log_manager.failed()
		raise ValueError(f"Failed to sync editions: {e}")
