import uuid
import pandas as pd
from ..models.csv_file import CSVFile
from .dump_reader import iter_redirects

def build_redirect_map(config, redirects_path: str, collection: str, known_keys: set) -> CSVFile | None:
	"""
	Stream the redirects dump and keep only the redirects for `collection`
	("authors" | "works" | "books") whose old key is one we actually have —
	most of OL's redirect history is irrelevant to whatever we've synced so far.

	Returns a CSVFile with columns (old_key, new_key) ready to be COPYed into a
	temp mapping table, or None if nothing in this dump concerns us.
	"""
	rows = []
	for coll, old_key, new_key in iter_redirects(redirects_path):
		if coll != collection or not new_key or old_key not in known_keys:
			continue
		rows.append({"old_key": old_key, "new_key": new_key})

	if not rows:
		return None

	csv = CSVFile(columns=["old_key", "new_key"], tmp_directory=config.tmp_directory, prefix=f"redirects_{collection}")
	csv.append(rows_data=pd.DataFrame(rows))
	return csv

def load_redirect_map(cursor, redirect_csv: CSVFile) -> str:
	"""Load a redirect CSV into a temp table and return its name."""
	temp_table = f"redirect_map_{uuid.uuid4().hex}"
	cursor.execute(f"CREATE TEMP TABLE {temp_table} (old_key text, new_key text);")
	with open(redirect_csv.file_path, "r") as f:
		cursor.copy_expert(f"COPY {temp_table} (old_key, new_key) FROM STDIN WITH CSV HEADER", f)
	return temp_table

def apply_redirects(
	cursor,
	redirect_map_table: str,
	table: str,
	key_column: str,
	conflict_columns: list[str] | None,
):
	"""
	Repoint every `key_column` value in `table` from a redirected (old) key to its
	new target, using the (old_key, new_key) mapping in `redirect_map_table`.

	`conflict_columns` lists the table's other unique-together columns besides
	key_column (e.g. ["subject"] for work_subject's unique(work_key, subject)):
	a row that would collide with one the new key already has is deleted instead
	of updated, since the new key's own row already covers that case. Pass an
	empty list when key_column is unique on its own (e.g. a link table keyed
	only by this column). Pass None when the column isn't unique at all (e.g. a
	plain FK like edition.work_key) — there's nothing that could conflict, so
	rows are just updated directly.
	"""
	if conflict_columns is not None:
		match_clause = " AND ".join(f"t2.{c} = t.{c}" for c in conflict_columns)
		where_extra = f" AND {match_clause}" if match_clause else ""
		cursor.execute(f"""
			DELETE FROM {table} t
			USING {redirect_map_table} rm
			WHERE t.{key_column} = rm.old_key
			AND EXISTS (
				SELECT 1 FROM {table} t2
				WHERE t2.{key_column} = rm.new_key{where_extra}
			);
		""")

	cursor.execute(f"""
		UPDATE {table} t
		SET {key_column} = rm.new_key
		FROM {redirect_map_table} rm
		WHERE t.{key_column} = rm.old_key;
	""")
