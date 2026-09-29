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
	Download a file from the given URL, streaming it to disk.

	Open Library dumps range from a few hundred MB to several GB (editions):
	unlike sync_tmdb's version of this helper, the response is never buffered
	in memory (requests.get(..., stream=True) + chunked reads).

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

	with requests.get(url, stream=True, timeout=(10, 300)) as response:
		if response.status_code != 200:
			raise ValueError(f"Failed to download {url}: HTTP {response.status_code}")

		with open(file_path, 'wb') as file:
			for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
				if chunk:
					file.write(chunk)

	return file_path

def decompress_file(file_path: str, deleteCompressedFile: bool = True) -> str:
	"""
	Decompress a file. Only safe for the small OL dumps (redirects, deletes, covers
	metadata...) — the big ones (authors, works, editions) must be streamed directly
	with gzip.open() instead, see utils/dump_reader.py, since decompressing them to
	disk first would need tens of GB of free space.

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

def remove_duplicates(input_file: str, output_file: str, conflict_columns: list):
	"""
	Delete duplicates from a CSV file.

	:param input_file: The path to the input CSV file.
	:param output_file: The path to the output CSV file.
	:param conflict_columns: The columns to use to detect duplicates.
	"""
	try:
		# Read every column as plain text: convert_dtypes() would otherwise infer an
		# all-digit string column (e.g. an ISBN starting with "0") as an integer and
		# silently drop the leading zero on write-back. Duplicate detection only needs
		# string equality, never numeric typing.
		df = pd.read_csv(input_file, na_filter=False, dtype=str)

		# Delete duplicates
		df_cleaned = df.drop_duplicates(subset=conflict_columns, keep='first')
		# Save the cleaned CSV file
		df_cleaned.to_csv(output_file, index=False)
	except Exception as e:
		raise ValueError(f"Failed to remove duplicates: {e}")
