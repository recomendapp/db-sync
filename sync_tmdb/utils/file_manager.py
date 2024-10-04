import os
import uuid
import pandas as pd
from typing import IO
import requests
import gzip
import shutil

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

def download_file(url: str, tmp_directory: str = None, prefix: str = "file") -> str:
	"""
	Download a file from the given URL.

	Args:
		url (str): The URL of the file to download.
		tmp_directory (str, optionnel): The directory where to save the file. Default: None.
		prefix (str, optionnel): The prefix of the file. Default: "file".

	Returns:
		str: The path to the downloaded file.
	"""

	if tmp_directory:
		if not os.path.exists(tmp_directory):
			os.makedirs(tmp_directory)

	extension = url.split('.')[-1]
	file_name = f"{prefix}_{uuid.uuid4().hex}{f'.{extension}' if extension else ''}"
	file_path = os.path.join(tmp_directory, file_name) if tmp_directory else file_name

	response = requests.get(url)

	if response.status_code != 200:
		raise ValueError(f"Failed to download {url}: {response.text}")
	
	with open(file_path, 'wb') as file:
		file.write(response.content)

	return file_path

def decompress_file(file_path: str, deleteCompressedFile: bool = True) -> str:
	"""
	Decompress a file.

	Args:
		file_path (str): The path to the file to decompress.
		deleteCompressedFile (bool, optionnel): Whether to delete the compressed file after decompression. Default: False.
	Returns:
		str: The path to the decompressed file.
	"""

	if not file_path.endswith(".gz"):
		raise ValueError(f"File {file_path} is not a compressed file")

	decompressed_file_path = file_path[:-3]

	with gzip.open(file_path, 'rb') as compressed_file:
		with open(decompressed_file_path, 'wb') as decompressed_file:
			shutil.copyfileobj(compressed_file, decompressed_file)

	if deleteCompressedFile:
		os.remove(file_path)

	return decompressed_file_path


