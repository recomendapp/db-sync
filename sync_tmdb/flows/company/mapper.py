import pandas as pd
from .config import CompanyConfig as Config

class Mapper:
	@staticmethod
	def company(company: dict) -> pd.DataFrame:
		company_data = [
			{
				"id": company.get("id"),
				"name": company.get("name", None),
				"description": company.get("description", None),
				"headquarters": company.get("headquarters", None),
				"homepage": company.get("homepage", None),
				"origin_country": company.get("origin_country", None),
				"parent_company": company.get("parent_company", {}).get("id", None) if company.get("parent_company") else None
			}
		]
		return pd.DataFrame(company_data)
	
	@staticmethod
	def company_image(company: dict) -> pd.DataFrame:
		companyId = company["id"]
		images = company.get("images", {}).get("logos", [])
		company_image_data = [
			{
				"id": image["id"],
				"company": companyId,
				"file_path": image["file_path"],
				"file_type": image["file_type"],
				"aspect_ratio": image["aspect_ratio"],
				"height": image["height"],
				"width": image["width"],
				"vote_average": image["vote_average"],
				"vote_count": image["vote_count"],
			}
			for image in images
		]

		return pd.DataFrame(company_image_data)
	
	@staticmethod
	def company_alternative_name(company: dict) -> pd.DataFrame:
		companyId = company["id"]
		alternative_names = company.get("alternative_names", {}).get("results", [])
		company_alternative_name_data = [
			{
				"company": companyId,
				"name": alternative_name["name"]
			}
			for alternative_name in alternative_names
		]

		return pd.DataFrame(company_alternative_name_data)