import pandas as pd
from prefect import task
from ...models.csv_file import CSVFile
from .config import CompanyConfig as Config

class Mapper:
	# Columns
	company_columns: list[str] = ["id", "name", "description", "headquarters", "homepage", "logo_path", "origin_country", "parent_company"]

	# On conflict
	company_on_conflict: list[str] = ["id"]
	company_on_conflict_update: list[str] = ["name", "description", "headquarters", "homepage", "logo_path", "origin_country", "parent_company"]

	@staticmethod
	def company(config: Config, company: dict) -> pd.DataFrame:
		company_data = [
			{
				"id": company.get("id"),
				"name": company.get("name", None),
				"description": company.get("description", None),
				"headquarters": company.get("headquarters", None),
				"homepage": company.get("homepage", None),
				"logo_path": company.get("logo_path", None),
				"origin_country": company.get("origin_country", None),
				"parent_company": company.get("parent_company", {}).get("id", None) if company.get("parent_company") else None
			}
		]
		return pd.DataFrame(company_data)
	
	@staticmethod
	@task
	def push(config: Config, company_csv: CSVFile):
		try:
			with config.db_client.get_connection() as conn:
				with conn.cursor() as cursor:
					conn.autocommit = False
					cursor.execute(f"""
						CREATE TEMP TABLE temp_{config.table_company} (LIKE {config.table_company} INCLUDING ALL);
					""")

					with open(company_csv.file_path, "r") as f:
						cursor.copy_expert(f"COPY temp_{config.table_company} ({','.join(Mapper.company_columns)}) FROM STDIN WITH CSV HEADER", f)

					cursor.execute(f"""
						INSERT INTO {config.table_company} ({','.join(Mapper.company_columns)})
						SELECT {','.join(Mapper.company_columns)} FROM temp_{config.table_company}
						ON CONFLICT ({','.join(Mapper.company_on_conflict)}) DO UPDATE
						SET {','.join([f"{column}=EXCLUDED.{column}" for column in Mapper.company_on_conflict_update])};
					""")
					
					conn.commit()

					company_csv.delete()
		except Exception as e:
			raise ValueError(f"Failed to push companies to the database: {e}")	
		
	