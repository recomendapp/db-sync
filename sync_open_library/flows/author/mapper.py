from ...utils.nullify import nullify
from ...utils.dates import parse_ol_date

def _as_text(value) -> str | None:
	"""OL text fields (bio, name...) are sometimes a plain string, sometimes {'type': '/type/text', 'value': '...'}."""
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
	def author(record: dict) -> dict:
		author = record["data"]
		birth = parse_ol_date(author.get("birth_date"))
		death = parse_ol_date(author.get("death_date"))

		return {
			"key": record["key"],
			"name": _as_text(author.get("name")),
			"personal_name": _as_text(author.get("personal_name")),
			"title": _as_text(author.get("title")),
			"bio": _as_text(author.get("bio")),
			"wikipedia": _as_text(author.get("wikipedia")),
			"birth_date_raw": birth.raw,
			"birth_date": birth.date,
			"birth_year": birth.year,
			"birth_precision": birth.precision,
			"death_date_raw": death.raw,
			"death_date": death.date,
			"death_year": death.year,
			"death_precision": death.precision,
			"revision": record["revision"],
			"last_modified": record["last_modified"],
		}

	@staticmethod
	def author_alternate_name(record: dict) -> list[dict]:
		author = record["data"]
		names = author.get("alternate_names", []) or []
		return [
			{"author_key": record["key"], "name": name}
			for name in names if name
		]

	@staticmethod
	def author_external_id(record: dict) -> list[dict]:
		author = record["data"]
		remote_ids = author.get("remote_ids", {}) or {}
		return [
			{"author_key": record["key"], "source": source, "value": value}
			for source, value in remote_ids.items() if value
		]

	@staticmethod
	def author_photo(record: dict) -> list[dict]:
		author = record["data"]
		photos = author.get("photos", []) or []
		return [
			{"author_key": record["key"], "photo_id": photo_id, "position": position}
			for position, photo_id in enumerate(photos)
			if photo_id and photo_id > 0
		]
