import os
import pandas as pd
import uuid

from ..utils.file_manager import remove_duplicates

class CSVFile:
	def __init__(self, columns: list, tmp_directory: str = None, prefix: str = "data"):
		"""
		Initialize the CSVFile instance and create a new CSV file with the given columns.

		Args:
			columns (list): The column names for the CSV file.
			tmp_directory (str, optionnel): The directory where to save the CSV file. Default: None.
			prefix (str, optionnel): The prefix of the CSV file. Default: "data".
		"""
		if not columns:
			raise ValueError("Columns must be provided")
		if not all(isinstance(col, str) for col in columns):
			raise ValueError("Columns must be a list of strings")

		self.columns: list[str] = columns
		self.tmp_directory: str = tmp_directory
		self.prefix: str = prefix

		# Create directory if it doesn't exist
		if tmp_directory and not os.path.exists(tmp_directory):
			os.makedirs(tmp_directory)

		self.file_name = f"{prefix}_{uuid.uuid4().hex}.csv"
		self.file_path = os.path.join(tmp_directory, self.file_name) if tmp_directory else self.file_name

		# Create the file with the header
		self._create_csv()
        
	def _create_csv(self):
		"""
		Create a new CSV file with the specified columns as header.
		"""
		df = pd.DataFrame(columns=self.columns)
		df.to_csv(self.file_path, mode='w', index=False, header=True)
	
	def append(self, rows_data: pd.DataFrame):
		"""
		Append the given rows data to the CSV file.

		Args:
			rows_data (pd.DataFrame): A DataFrame containing the rows data to append. Columns should match the column names.
		"""
		if not isinstance(rows_data, pd.DataFrame):
			raise ValueError("Rows data must be a DataFrame")
		
		rows_data.to_csv(self.file_path, mode='a', index=False, header=False)
	
	def get_file_path(self) -> str:
		"""
		Return the file path of the CSV file.

		Returns:
			str: The path to the CSV file.
		"""
		return self.file_path
	
	def delete(self):
		"""
		Delete the CSV file.
		"""
		if os.path.exists(self.file_path):
			os.remove(self.file_path)
	
	def is_empty(self) -> bool:
		"""
		Check if the CSV file is empty.

		Returns:
			bool: True if the file is empty, False otherwise.
		"""
		return os.stat(self.file_path).st_size == 0

	def clean_duplicates(self, conflict_columns: list[str]):
		"""
		Clean the duplicates in the CSV file.
		"""
		remove_duplicates(input_file=self.file_path, output_file=self.file_path, conflict_columns=conflict_columns)
