from datetime import date
from prefect import task
from ...models.config import Config
from ...models.csv_file import CSVFile
from ...utils.db import insert_into

class CompanyConfig(Config):
	def __init__(self, date: date):
		super().__init__(date=date)
		self.flow_name: str = "company"

		# Tables
		self.table_company: str = self.config.get("db_tables", {}).get("company", "tmdb_company")
		self.table_company_image: str = self.config.get("db_tables", {}).get("company_image", "tmdb_company_image")
		self.table_company_alternative_name: str = self.config.get("db_tables", {}).get("company_alternative_name", "tmdb_company_alternative_name")

		# Ids
		self.extra_companies: set = {}
		self.missing_companies: set = {}

		# Columns
		self.company_columns: list[str] = ["id", "name", "description", "headquarters", "homepage", "origin_country", "parent_company"]
		self.company_image_columns: list[str] = ["id", "company", "file_path", "file_type", "aspect_ratio", "height", "width", "vote_average", "vote_count"]
		self.company_alternative_name_columns: list[str] = ["company", "name"]
		
		# On conflict
		self.company_on_conflict: list[str] = ["id"]
		self.company_image_on_conflict: list[str] = ["id"]
		self.company_alternative_name_on_conflict: list[str] = ["company", "name"]

		# On conflict update
		self.company_on_conflict_update: list[str] = [col for col in self.company_columns if col not in self.company_on_conflict]
		self.company_image_on_conflict_update: list[str] = [col for col in self.company_image_columns if col not in self.company_image_on_conflict]
		self.company_alternative_name_on_conflict_update: list[str] = [col for col in self.company_alternative_name_columns if col not in self.company_alternative_name_on_conflict]
	
	@task
	def prune(self):
		"""Prune the extra companies from the database"""
		try:
			if len(self.extra_companies) > 0:
				with self.db_client.get_connection() as conn:
					with conn.cursor() as cursor:
						conn.autocommit = False
						try:
							cursor.execute(f"DELETE FROM {self.table_company} WHERE id IN %s", (tuple(self.extra_companies),))
							conn.commit()
						except:
							conn.rollback()
							raise
		except Exception as e:
			raise ValueError(f"Failed to prune extra companies: {e}")

	@task
	def push(self, company_csv: CSVFile, company_image_csv: CSVFile, company_alternative_name_csv: CSVFile):
		"""Push the companies to the database"""
		try:
			# Clean duplicates from the CSV files
			company_csv.clean_duplicates(conflict_columns=self.company_on_conflict)
			company_image_csv.clean_duplicates(conflict_columns=self.company_image_on_conflict)
			company_alternative_name_csv.clean_duplicates(conflict_columns=self.company_alternative_name_on_conflict)

			with self.db_client.get_connection() as conn:
				with conn.cursor() as cursor:
					try:
						conn.autocommit = False
						cursor.execute(f"""
							CREATE TEMP TABLE temp_{self.table_company} (LIKE {self.table_company} INCLUDING ALL);
							CREATE TEMP TABLE temp_{self.table_company_image} (LIKE {self.table_company_image} INCLUDING ALL);
							CREATE TEMP TABLE temp_{self.table_company_alternative_name} (LIKE {self.table_company_alternative_name} INCLUDING ALL);
						""")

						with open(company_csv.file_path, "r") as f:
							cursor.copy_expert(f"COPY temp_{self.table_company} ({','.join(self.company_columns)}) FROM STDIN WITH CSV HEADER", f)
						with open(company_image_csv.file_path, "r") as f:
							cursor.copy_expert(f"COPY temp_{self.table_company_image} ({','.join(self.company_image_columns)}) FROM STDIN WITH CSV HEADER", f)
						with open(company_alternative_name_csv.file_path, "r") as f:
							cursor.copy_expert(f"COPY temp_{self.table_company_alternative_name} ({','.join(self.company_alternative_name_columns)}) FROM STDIN WITH CSV HEADER", f)

						# Insert companies
						insert_into(
							cursor=cursor,
							table=self.table_company,
							temp_table=f"temp_{self.table_company}",
							columns=self.company_columns,
							on_conflict=self.company_on_conflict,
							on_conflict_update=self.company_on_conflict_update
						)

						# Insert company images
						insert_into(
							cursor=cursor,
							table=self.table_company_image,
							temp_table=f"temp_{self.table_company_image}",
							columns=self.company_image_columns,
							on_conflict=self.company_image_on_conflict,
							on_conflict_update=self.company_image_on_conflict_update
						)

						# Insert company alternative names
						insert_into(
							cursor=cursor,
							table=self.table_company_alternative_name,
							temp_table=f"temp_{self.table_company_alternative_name}",
							columns=self.company_alternative_name_columns,
							on_conflict=self.company_alternative_name_on_conflict,
							on_conflict_update=self.company_alternative_name_on_conflict_update
						)

						# Delete outdated images
						cursor.execute(f"""
							DELETE FROM {self.table_company_image}
							WHERE ({','.join(self.company_image_on_conflict)}) NOT IN (
								SELECT {','.join(self.company_image_on_conflict)}
								FROM temp_{self.table_company_image}
							)
							AND company IN (
								SELECT id FROM temp_{self.table_company}
							);
						""")

						# Delete outdated alternative names
						cursor.execute(f"""
							DELETE FROM {self.table_company_alternative_name}
							WHERE ({','.join(self.company_alternative_name_on_conflict)}) NOT IN (
								SELECT {','.join(self.company_alternative_name_on_conflict)}
								FROM temp_{self.table_company_alternative_name}
							)
							AND company IN (
								SELECT id FROM temp_{self.table_company}
							);
						""")

						conn.commit()

						company_csv.delete()
						company_image_csv.delete()
						company_alternative_name_csv.delete()
					except:
						conn.rollback()
						raise
					finally:
						conn.autocommit = True
		except Exception as e:
			raise ValueError(f"Failed to push companies to the database: {e}")
