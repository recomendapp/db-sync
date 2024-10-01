import pandas as pd
from ...models.language import Language
from ...models.extra_languages import ExtraLanguages

class Mappers:
	def __init__(self, country: list, default_language: Language, extra_languages: ExtraLanguages):
		self.default_language = default_language
		self.extra_languages = extra_languages.languages
		self.country = self._map_country(country)
		self.country_translation = self._map_country_translation(country)
	
	def _map_country(self, country: list) -> pd.DataFrame:
		country_data = [
			{
				"iso_3166_1": item["iso_3166_1"],
			}
			for item in country
		]
		return pd.DataFrame(country_data)
	
	def _map_country_translation(self, country: list) -> pd.DataFrame:
		country_translation_data = [
			{
				"iso_3166_1": item["iso_3166_1"],
				"name": item["english_name"],
				"language": self.default_language.code
			}
			for item in country
		]
		# Add the extra languages
		for extra_language in self.extra_languages:
			country_translation_data.extend([
				{
					"iso_3166_1": item["iso_3166_1"],
					"name": item["native_name"],
					"language": extra_language.code
				}
				for item in country
			])
		return pd.DataFrame(country_translation_data)
		
