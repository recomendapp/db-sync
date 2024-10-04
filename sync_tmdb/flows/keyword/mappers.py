import pandas as pd

class Mappers:
	def __init__(self, keywords: list):
		self.keyword = self._map_keyword(keywords)
	
	def _map_keyword(self, keywords: list) -> pd.DataFrame:
		keyword_data = [
			{
				"id": item["id"],
				"name": item["name"]
			}
			for item in keywords
		]
		return pd.DataFrame(keyword_data)
		
