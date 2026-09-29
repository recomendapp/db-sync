import gzip
import json
from typing import Iterator, TypedDict
from .keys import strip_ol_key

class DumpRecord(TypedDict):
	key: str
	revision: int
	last_modified: str
	data: dict

def iter_dump_records(gz_file_path: str, record_type: str) -> Iterator[DumpRecord]:
	"""
	Stream an Open Library dump file (columns: type, key, revision, last_modified, JSON)
	line by line, yielding only the rows matching record_type (e.g. "/type/author").

	Shared by the author, work and edition flows: the file is read directly from its
	.gz form (gzip.open handles the decompression in memory, chunk by chunk) so even
	the ~9GB editions dump never needs to be decompressed to disk first.

	The dump's own `key` column (a full path like "/authors/OL23919A") is stripped down
	to its bare id before being yielded, matching how our own tables store keys.

	Args:
		gz_file_path (str): Path to the downloaded .gz dump file.
		record_type (str): The OL type to keep, e.g. "/type/author", "/type/work", "/type/edition".
	"""
	with gzip.open(gz_file_path, 'rt', encoding='utf-8') as f:
		for line in f:
			parts = line.rstrip('\n').split('\t')
			if len(parts) != 5:
				continue

			type_, key, revision, last_modified, json_blob = parts
			if type_ != record_type:
				continue

			try:
				data = json.loads(json_blob)
			except json.JSONDecodeError:
				continue

			yield DumpRecord(
				key=strip_ol_key(key),
				revision=int(revision),
				last_modified=last_modified,
				data=data,
			)

def iter_redirects(gz_file_path: str) -> Iterator[tuple[str, str, str]]:
	"""
	Stream the Open Library redirects dump, yielding (collection, old_key, new_key) for
	every redirect, e.g. ("authors", "OL999992A", "OL999991A").

	Every entity type's redirects share the same /type/redirect record type, so unlike
	iter_dump_records() there is no `record_type` filter -- the entity type is only
	distinguishable by the raw key's path segment ("authors", "works", "books"), which
	is why the raw key can't be pre-stripped for this one.
	"""
	with gzip.open(gz_file_path, 'rt', encoding='utf-8') as f:
		for line in f:
			parts = line.rstrip('\n').split('\t')
			if len(parts) != 5:
				continue

			type_, raw_key, _revision, _last_modified, json_blob = parts
			if type_ != '/type/redirect':
				continue

			try:
				data = json.loads(json_blob)
			except json.JSONDecodeError:
				continue

			location = data.get("location")
			if not location:
				continue

			segments = raw_key.strip('/').split('/')
			if len(segments) != 2:
				continue

			collection, _id = segments
			yield collection, strip_ol_key(raw_key), strip_ol_key(location)
