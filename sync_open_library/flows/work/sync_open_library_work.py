from datetime import date
import gc
import os

import pandas as pd
from prefect import flow
from prefect.logging import get_run_logger

from .config import WorkConfig
from .mapper import Mapper
from ...models.csv_file import CSVFile
from ...utils.file_manager import download_file
from ...utils.dump_reader import iter_dump_records
from ...utils.redirects import build_redirect_map, load_redirect_map, apply_redirects
from ...utils.dataframes import with_nullable_int_columns

# ---------------------------------------------------------------------------- #
#                                    Getters                                   #
# ---------------------------------------------------------------------------- #

def get_db_work_keys(config: WorkConfig) -> set:
	conn = config.db_client.get_connection()
	try:
		with conn.cursor() as cursor:
			cursor.execute(f"SELECT key FROM {config.table_work}")
			return {row[0] for row in cursor}
	except Exception as e:
		raise ValueError(f"Failed to get database works: {e}")
	finally:
		config.db_client.return_connection(conn)

# ---------------------------------------------------------------------------- #

def push_batch(config: WorkConfig, batch: list[dict]):
	if not batch:
		return

	# One DataFrame per table for the whole batch: at OL's scale (tens of millions of
	# rows), writing per-record would mean hundreds of thousands of individual to_csv
	# calls per batch instead of 4.
	work_rows = [Mapper.work(r) for r in batch]
	author_rows = [row for r in batch for row in Mapper.work_author(r)]
	subject_rows = [row for r in batch for row in Mapper.work_subject(r)]
	cover_rows = [row for r in batch for row in Mapper.work_cover(r)]

	work_csv = CSVFile(columns=config.work_columns, tmp_directory=config.tmp_directory, prefix=config.flow_name)
	work_author_csv = CSVFile(columns=config.work_author_columns, tmp_directory=config.tmp_directory, prefix=f"{config.flow_name}_author")
	work_subject_csv = CSVFile(columns=config.work_subject_columns, tmp_directory=config.tmp_directory, prefix=f"{config.flow_name}_subject")
	work_cover_csv = CSVFile(columns=config.work_cover_columns, tmp_directory=config.tmp_directory, prefix=f"{config.flow_name}_cover")

	work_df = with_nullable_int_columns(pd.DataFrame(work_rows, columns=config.work_columns), ["first_publish_year"])
	work_csv.append(rows_data=work_df)
	work_author_csv.append(rows_data=pd.DataFrame(author_rows, columns=config.work_author_columns))
	work_subject_csv.append(rows_data=pd.DataFrame(subject_rows, columns=config.work_subject_columns))
	work_cover_csv.append(rows_data=pd.DataFrame(cover_rows, columns=config.work_cover_columns))

	push_future = config.push.submit(
		work_csv=work_csv,
		work_author_csv=work_author_csv,
		work_subject_csv=work_subject_csv,
		work_cover_csv=work_cover_csv,
	)
	push_future.result(raise_on_failure=True)

def process_dump(config: WorkConfig, dump_path: str, max_records: int | None = None) -> set:
	"""
	Stream the work dump and upsert it chunk by chunk. Returns every key seen.

	max_records stops the stream early after that many records — for smoke-testing
	against a real dump without processing it in full. The caller must not prune
	when it's set: seen_keys is then only a partial view of the dump, not the
	complete current state, so anything "missing" from it isn't really gone.
	"""
	seen_keys: set = set()
	batch: list[dict] = []

	for record in iter_dump_records(dump_path, record_type="/type/work"):
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

def apply_work_redirects(config: WorkConfig, redirects_path: str, known_keys: set):
	"""
	Repoint every table referencing a work that has since been merged into another one
	before that work's now-empty row gets pruned.
	"""
	redirect_csv = build_redirect_map(config, redirects_path, collection="works", known_keys=known_keys)
	if not redirect_csv:
		return

	conn = config.db_client.get_connection()
	try:
		with conn.cursor() as cursor:
			conn.autocommit = False
			try:
				temp_table = load_redirect_map(cursor, redirect_csv)

				apply_redirects(cursor, temp_table, config.table_work_author, "work_key", ["author_key"])
				apply_redirects(cursor, temp_table, config.table_work_subject, "work_key", ["subject"])
				apply_redirects(cursor, temp_table, config.table_work_cover, "work_key", ["cover_id"])
				# Not unique: several editions legitimately share the same work_key.
				apply_redirects(cursor, temp_table, "book.edition", "work_key", None)

				conn.commit()
			except Exception:
				conn.rollback()
				raise
			finally:
				conn.autocommit = True
	except Exception as e:
		raise ValueError(f"Failed to apply work redirects: {e}")
	finally:
		config.db_client.return_connection(conn)
		redirect_csv.delete()

# ---------------------------------------------------------------------------- #

@flow(name="sync_open_library_work", log_prints=True)
def sync_open_library_work(
	date: date = date.today(),
	dump_path: str | None = None,
	redirects_path: str | None = None,
	max_records: int | None = None,
):
	"""
	Sync Open Library works into book.work (+ authors, subjects, covers).

	Must run after sync_open_library_author: work_author rows are only kept for
	authors that already exist in book.author (see WorkConfig.push).

	dump_path/redirects_path let the orchestrator (sync_open_library.py) pass already-
	downloaded dumps so this flow doesn't re-download them when run as part of the full sync.

	max_records is for smoke-testing against a real dump: it stops the stream early and,
	since seen_keys is then only a partial view, also skips pruning for this run.
	"""
	logger = get_run_logger()
	logger.info(f"Syncing Open Library works for {date}...")
	config = WorkConfig(date=date)
	downloaded_dump_here = dump_path is None
	downloaded_redirects_here = redirects_path is None
	try:
		config.log_manager.init(type="work")

		config.log_manager.fetching_data()
		local_dump_path = dump_path or download_file(
			config.dump_urls["work"], tmp_directory=config.tmp_directory, prefix="ol_dump_works"
		)
		local_redirects_path = redirects_path or download_file(
			config.dump_urls["redirects"], tmp_directory=config.tmp_directory, prefix="ol_dump_redirects"
		)
		config.log_manager.data_fetched()

		config.log_manager.syncing_to_db()
		seen_keys = process_dump(config, local_dump_path, max_records=max_records)

		db_keys = get_db_work_keys(config)
		logger.info("Applying work redirects...")
		apply_work_redirects(config, local_redirects_path, known_keys=db_keys)

		if max_records is not None:
			logger.info(f"max_records set ({max_records}): skipping pruning for this run.")
		else:
			extra_keys = db_keys - seen_keys
			logger.info(f"Found {len(seen_keys)} works in the dump, pruning {len(extra_keys)} that no longer exist")
			config.prune(extra_keys)

		if downloaded_dump_here and os.path.exists(local_dump_path):
			os.remove(local_dump_path)
		if downloaded_redirects_here and os.path.exists(local_redirects_path):
			os.remove(local_redirects_path)

		config.log_manager.success()
		logger.info(f"Successfully synced {len(seen_keys)} Open Library works.")
	except Exception as e:
		config.log_manager.failed()
		raise ValueError(f"Failed to sync works: {e}")
