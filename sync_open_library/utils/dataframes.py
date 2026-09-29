import pandas as pd

def with_nullable_int_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
	"""
	Cast columns to pandas' nullable Int64 dtype (capital I) so a None mixed into an
	otherwise all-integer column doesn't silently upcast the whole column to float64
	— which then writes values like "1952.0" to CSV instead of "1952", and Postgres's
	integer columns reject that outright.
	"""
	for column in columns:
		df[column] = df[column].astype("Int64")
	return df
