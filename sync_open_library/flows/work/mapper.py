from ...utils.nullify import nullify
from ...utils.dates import parse_ol_date
from ...utils.keys import strip_ol_key

def _as_text(value) -> str | None:
	"""OL text fields (description, first_sentence...) are sometimes a plain string, sometimes {'type': '/type/text', 'value': '...'}."""
	if isinstance(value, dict):
		value = value.get("value")
	return nullify(value, "")

class Mapper:
	"""
	Each method returns plain dict rows, not DataFrames: at OL's scale (tens of millions
	of rows) building — and later concatenating — one tiny DataFrame per record would
	dominate the whole run's cost. The caller collects rows across a full batch and
	builds exactly one DataFrame per table right before writing it out.
	"""

	@staticmethod
	def work(record: dict) -> dict:
		work = record["data"]
		first_publish = parse_ol_date(work.get("first_publish_date"))
		cover_edition = work.get("cover_edition") or {}

		return {
			"key": record["key"],
			"title": _as_text(work.get("title")),
			"subtitle": _as_text(work.get("subtitle")),
			"description": _as_text(work.get("description")),
			"first_sentence": _as_text(work.get("first_sentence")),
			"first_publish_date_raw": first_publish.raw,
			"first_publish_year": first_publish.year,
			"cover_edition_key": strip_ol_key(cover_edition.get("key")),
			"revision": record["revision"],
			"last_modified": record["last_modified"],
		}

	@staticmethod
	def work_author(record: dict) -> list[dict]:
		work = record["data"]
		authors = work.get("authors", []) or []
		rows = []
		for position, entry in enumerate(authors):
			author_ref = (entry or {}).get("author") or {}
			author_key = strip_ol_key(author_ref.get("key"))
			if author_key:
				rows.append({"work_key": record["key"], "author_key": author_key, "position": position})
		return rows

	@staticmethod
	def work_subject(record: dict) -> list[dict]:
		work = record["data"]
		subjects = work.get("subjects", []) or []
		return [
			{"work_key": record["key"], "subject": subject}
			for subject in subjects if subject
		]

	@staticmethod
	def work_cover(record: dict) -> list[dict]:
		work = record["data"]
		covers = work.get("covers", []) or []
		return [
			{"work_key": record["key"], "cover_id": cover_id, "position": position}
			for position, cover_id in enumerate(covers)
			if cover_id and cover_id > 0
		]
