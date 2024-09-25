import os
import uuid
import pandas as pd
from typing import IO

def create_csv(data: pd.DataFrame, tmp_directory: str = None, prefix: str = "data") -> str:
	"""
    Create a CSV file with the given data.

    Args:
        data (pd.DataFrame): The data to save in the CSV file.
        tmp_directory (str, optionnel): The directory where to save the CSV file. Default: None.
        prefix (str, optionnel): The prefix of the CSV file. Default: "data".

    Returns:
        str: The path to the CSV file.
    """

	if tmp_directory:
		if not os.path.exists(tmp_directory):
			os.makedirs(tmp_directory)
	
	file_name = f"{prefix}_{uuid.uuid4().hex}.csv"
	file_path = os.path.join(tmp_directory, file_name) if tmp_directory else file_name

	if not isinstance(data, pd.DataFrame):
		data = pd.DataFrame(data)

	data.to_csv(file_path, index=False)

	return file_path

def get_csv_header(file: IO) -> list:
	"""
	Get the header of a CSV file.

	Args:
		file: The file to get the header from.

	Returns:
		list: The header of the CSV file.
	"""

	# Backup actual cursor position
	cursor_position = file.tell()
	# Get the header
	header = pd.read_csv(file, nrows=0).columns.tolist()
	# Reset cursor position
	file.seek(cursor_position)
	return header
