import re
from typing import NamedTuple, Optional
import dateparser

_APPROXIMATE_PREFIXES = re.compile(r'^\s*(c\.|ca\.|circa|b\.|d\.)\s*', re.IGNORECASE)
_MONTH_NAME = re.compile(r'[A-Za-z]{3,}')
_DAY_NUMBER = re.compile(r'\b\d{1,2}\b')
_IGNORED_VALUES = {"", "unknown", "n/a", "9999"}

class ParsedDate(NamedTuple):
	raw: Optional[str]
	date: Optional[str]  # ISO date string, only set when day-precision is confident
	year: Optional[int]
	precision: Optional[str]  # 'day' | 'month' | 'year' | 'circa'

def parse_ol_date(raw: Optional[str]) -> ParsedDate:
	"""
	Parse an Open Library free-text date (birth_date, death_date, publish_date...).

	OL dates range from precise ("September 21, 1947") to vague ("c. 1600", "1830?",
	"18th century"). The raw string is always preserved; the structured `date` field
	is only filled when day-precision is confident, so a year-only value never gets
	silently turned into a fake January 1st. `year` is filled whenever a year can be
	extracted at all, regardless of precision, so sorting/filtering still works.
	"""
	if raw is None or raw.strip().lower() in _IGNORED_VALUES:
		return ParsedDate(raw=raw, date=None, year=None, precision=None)

	stripped = raw.strip()
	is_approximate = bool(_APPROXIMATE_PREFIXES.match(stripped)) or stripped.endswith('?')
	cleaned = _APPROXIMATE_PREFIXES.sub('', stripped).rstrip('?').strip()

	parsed = dateparser.parse(cleaned, settings={"REQUIRE_PARTS": ["year"]})
	if not parsed:
		return ParsedDate(raw=raw, date=None, year=None, precision=None)

	has_day = bool(_DAY_NUMBER.search(cleaned)) and bool(_MONTH_NAME.search(cleaned))
	has_month = not has_day and bool(_MONTH_NAME.search(cleaned))

	if is_approximate:
		precision = "circa"
	elif has_day:
		precision = "day"
	elif has_month:
		precision = "month"
	else:
		precision = "year"

	return ParsedDate(
		raw=raw,
		date=parsed.date().isoformat() if precision == "day" else None,
		year=parsed.year,
		precision=precision,
	)
