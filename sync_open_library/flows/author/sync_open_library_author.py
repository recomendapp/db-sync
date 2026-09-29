from datetime import date
import gc
import os

import pandas as pd
from prefect import flow
from prefect.logging import get_run_logger

from .config import AuthorConfig
from .mapper import Mapper
from ...models.csv_file import CSVFile
from ...utils.file_manager import download_file
from ...utils.dump_reader import iter_dump_records
from ...utils.redirects import build_redirect_map, load_redirect_map, apply_redirects
from ...utils.dataframes import with_nullable_int_columns

# ---------------------------------------------------------------------------- #
#                                    Getters                                   #
# ---------------------------------------------------------------------------- #

def get_db_author_keys(config: AuthorConfig) -> set:
	conn = config.db_client.get_connection()
	try:
		with conn.cursor() as cursor:
			cursor.execute(f"SELECT key FROM {config.table_author}")
			return {row[0] for row in cursor}
	except Exception as e:
		raise ValueError(f"Failed to get database authors: {e}")
	finally:
		config.db_client.return_connection(conn)

# ---------------------------------------------------------------------------- #

def push_batch(config: AuthorConfig, batch: list[dict]):
	if not batch:
		return

	# One DataFrame per table for the whole batch: at OL's scale (tens of millions of
	# rows), writing per-record would mean hundreds of thousands of individual to_csv
	# calls per batch instead of 4.
	author_rows = [Mapper.author(r) for r in batch]
	alternate_name_rows = [row for r in batch for row in Mapper.author_alternate_name(r)]
	external_id_rows = [row for r in batch for row in Mapper.author_external_id(r)]
	photo_rows = [row for r in batch for row in Mapper.author_photo(r)]

	author_csv = CSVFile(columns=config.author_columns, tmp_directory=config.tmp_directory, prefix=config.flow_name)
	author_alternate_name_csv = CSVFile(columns=config.author_alternate_name_columns, tmp_directory=config.tmp_directory, prefix=f"{config.flow_name}_alternate_name")
	author_external_id_csv = CSVFile(columns=config.author_external_id_columns, tmp_directory=config.tmp_directory, prefix=f"{config.flow_name}_external_id")
	author_photo_csv = CSVFile(columns=config.author_photo_columns, tmp_directory=config.tmp_directory, prefix=f"{config.flow_name}_photo")

	author_df = with_nullable_int_columns(pd.DataFrame(author_rows, columns=config.author_columns), ["birth_year", "death_year"])
	author_csv.append(rows_data=author_df)
	author_alternate_name_csv.append(rows_data=pd.DataFrame(alternate_name_rows, columns=config.author_alternate_name_columns))
	author_external_id_csv.append(rows_data=pd.DataFrame(external_id_rows, columns=config.author_external_id_columns))
	author_photo_csv.append(rows_data=pd.DataFrame(photo_rows, columns=config.author_photo_columns))

	push_future = config.push.submit(
		author_csv=author_csv,
		author_alternate_name_csv=author_alternate_name_csv,
		author_external_id_csv=author_external_id_csv,
		author_photo_csv=author_photo_csv,
	)
	push_future.result(raise_on_failure=True)

def process_dump(config: AuthorConfig, dump_path: str, max_records: int | None = None) -> set:
	"""
	Stream the author dump and upsert it chunk by chunk. Returns every key seen.

	max_records stops the stream early after that many records — for smoke-testing
	against a real dump without processing it in full. The caller must not prune
	when it's set: seen_keys is then only a partial view of the dump, not the
	complete current state, so anything "missing" from it isn't really gone.
	"""
	seen_keys: set = set()
	batch: list[dict] = []

	for record in iter_dump_records(dump_path, record_type="/type/author"):
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

def apply_author_redirects(config: AuthorConfig, redirects_path: str, known_keys: set):
	"""
	Repoint every table referencing an author that has since been merged into another
	one (an OL /type/redirect) before that author's now-empty row gets pruned — otherwise
	the cascade delete on book.author would silently wipe those references instead of
	following them to the surviving key.
	"""
	redirect_csv = build_redirect_map(config, redirects_path, collection="authors", known_keys=known_keys)
	if not redirect_csv:
		return

	conn = config.db_client.get_connection()
	try:
		with conn.cursor() as cursor:
			conn.autocommit = False
			try:
				temp_table = load_redirect_map(cursor, redirect_csv)

				apply_redirects(cursor, temp_table, config.table_author_alternate_name, "author_key", ["name"])
				apply_redirects(cursor, temp_table, config.table_author_external_id, "author_key", ["source"])
				apply_redirects(cursor, temp_table, config.table_author_photo, "author_key", ["photo_id"])
				apply_redirects(cursor, temp_table, "book.work_author", "author_key", ["work_key"])
				apply_redirects(cursor, temp_table, "person_open_library_link", "author_key", [])

				conn.commit()
			except Exception:
				conn.rollback()
				raise
			finally:
				conn.autocommit = True
	except Exception as e:
		raise ValueError(f"Failed to apply author redirects: {e}")
	finally:
		config.db_client.return_connection(conn)
		redirect_csv.delete()

# ---------------------------------------------------------------------------- #

@flow(name="sync_open_library_author", log_prints=True)
def sync_open_library_author(
	date: date = date.today(),
	dump_path: str | None = None,
	redirects_path: str | None = None,
	max_records: int | None = None,
):
	"""
	Sync Open Library authors into book.author (+ alternate names, external ids, photos).

	dump_path/redirects_path let the orchestrator (sync_open_library.py) pass already-
	downloaded dumps so this flow doesn't re-download them when run as part of the full sync.

	max_records is for smoke-testing against a real dump: it stops the stream early and,
	since seen_keys is then only a partial view, also skips pruning for this run.
	"""
	logger = get_run_logger()
	logger.info(f"Syncing Open Library authors for {date}...")
	config = AuthorConfig(date=date)
	downloaded_dump_here = dump_path is None
	downloaded_redirects_here = redirects_path is None
	try:
		config.log_manager.init(type="author")

		config.log_manager.fetching_data()
		local_dump_path = dump_path or download_file(
			config.dump_urls["author"], tmp_directory=config.tmp_directory, prefix="ol_dump_authors"
		)
		local_redirects_path = redirects_path or download_file(
			config.dump_urls["redirects"], tmp_directory=config.tmp_directory, prefix="ol_dump_redirects"
		)
		config.log_manager.data_fetched()

		config.log_manager.syncing_to_db()
		seen_keys = process_dump(config, local_dump_path, max_records=max_records)

		db_keys = get_db_author_keys(config)
		logger.info("Applying author redirects...")
		apply_author_redirects(config, local_redirects_path, known_keys=db_keys)

		if max_records is not None:
			logger.info(f"max_records set ({max_records}): skipping pruning for this run.")
		else:
			extra_keys = db_keys - seen_keys
			logger.info(f"Found {len(seen_keys)} authors in the dump, pruning {len(extra_keys)} that no longer exist")
			config.prune(extra_keys)

		if downloaded_dump_here and os.path.exists(local_dump_path):
			os.remove(local_dump_path)
		if downloaded_redirects_here and os.path.exists(local_redirects_path):
			os.remove(local_redirects_path)

		config.log_manager.success()
		logger.info(f"Successfully synced {len(seen_keys)} Open Library authors.")
	except Exception as e:
		config.log_manager.failed()
		raise ValueError(f"Failed to sync authors: {e}")
