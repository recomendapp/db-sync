"""Coerce TMDB numbers into what an integer column can actually hold.

TMDB is community-edited, so any numeric field can carry a value no integer column
accepts: a date used as an episode number, a runtime in the billions. Postgres
rejects the entire COPY when one such row shows up, so a single bad value takes the
whole batch down. Each helper takes the default the caller would otherwise have
passed to .get(), so an unstorable value lands on the same fallback as a missing one.
"""

POSTGRES_SMALLINT_MIN = -32_768
POSTGRES_SMALLINT_MAX = 32_767
POSTGRES_INTEGER_MIN = -2_147_483_648
POSTGRES_INTEGER_MAX = 2_147_483_647


def _in_range(value, minimum: int, maximum: int, default):
	# bool is an int subclass, and TMDB sometimes types a whole number as a float.
	if isinstance(value, bool):
		return default
	if isinstance(value, float) and value.is_integer():
		value = int(value)
	if not isinstance(value, int):
		return default
	return value if minimum <= value <= maximum else default


def smallint(value, default=0):
	"""Return value if a smallint column can hold it, otherwise default."""
	return _in_range(value, POSTGRES_SMALLINT_MIN, POSTGRES_SMALLINT_MAX, default)


def integer(value, default=0):
	"""Return value if an integer column can hold it, otherwise default."""
	return _in_range(value, POSTGRES_INTEGER_MIN, POSTGRES_INTEGER_MAX, default)
