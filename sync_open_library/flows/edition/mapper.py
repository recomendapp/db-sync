from ...utils.nullify import nullify
from ...utils.dates import parse_ol_date
from ...utils.keys import strip_ol_key

def _as_text(value) -> str | None:
	"""OL text fields (description...) are sometimes a plain string, sometimes {'type': '/type/text', 'value': '...'}."""
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
	def edition(record: dict) -> dict:
		edition = record["data"]
		publish = parse_ol_date(edition.get("publish_date"))

		# An edition officially belongs to a list (works[]), but in practice it's always
		# exactly one work; the first entry is used as the primary link.
		works = edition.get("works", []) or []
		work_key = strip_ol_key(works[0].get("key")) if works else None

		return {
			"key": record["key"],
			"work_key": work_key,
			"title": nullify(edition.get("title"), ""),
			"subtitle": nullify(edition.get("subtitle"), ""),
			"edition_name": nullify(edition.get("edition_name"), ""),
			"publish_date_raw": publish.raw,
			"publish_date": publish.date,
			"publish_year": publish.year,
			"publish_precision": publish.precision,
			"number_of_pages": edition.get("number_of_pages"),
			"physical_format": nullify(edition.get("physical_format"), ""),
			"translation_of": nullify(edition.get("translation_of"), ""),
			"ocaid": nullify(edition.get("ocaid"), ""),
			"revision": record["revision"],
			"last_modified": record["last_modified"],
		}

	@staticmethod
	def edition_isbn(record: dict) -> list[dict]:
		edition = record["data"]
		rows = [
			{"edition_key": record["key"], "isbn": isbn, "type": "isbn_10"}
			for isbn in (edition.get("isbn_10", []) or []) if isbn
		]
		rows += [
			{"edition_key": record["key"], "isbn": isbn, "type": "isbn_13"}
			for isbn in (edition.get("isbn_13", []) or []) if isbn
		]
		return rows

	@staticmethod
	def edition_language(record: dict) -> list[dict]:
		edition = record["data"]
		languages = edition.get("languages", []) or []
		rows = []
		for lang in languages:
			code = strip_ol_key((lang or {}).get("key"))
			if code:
				rows.append({"edition_key": record["key"], "language": code})
		return rows

	@staticmethod
	def edition_genre(record: dict) -> list[dict]:
		edition = record["data"]
		genres = edition.get("genres", []) or []
		return [
			{"edition_key": record["key"], "genre": genre}
			for genre in genres if genre
		]

	@staticmethod
	def edition_cover(record: dict) -> list[dict]:
		edition = record["data"]
		covers = edition.get("covers", []) or []
		return [
			{"edition_key": record["key"], "cover_id": cover_id, "position": position}
			for position, cover_id in enumerate(covers)
			if cover_id and cover_id > 0
		]
