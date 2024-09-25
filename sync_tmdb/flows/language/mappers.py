import pandas as pd
from ...models.language import Language
from ...models.extra_languages import ExtraLanguages

class Mappers:
	def __init__(self, language: list, default_language: Language, extra_languages: ExtraLanguages):
		self.default_language = default_language
		self.extra_languages = extra_languages.languages
		self.language = self._map_language(language)
		self.language_translation = self._map_language_translation(language)
	
	def _map_language(self, language: list) -> pd.DataFrame:
		language_data = [
			{
				"iso_639_1": lang["iso_639_1"],
				"name_in_native_language": lang["name"]
			}
			for lang in language
		]
		return pd.DataFrame(language_data)
	
	def _map_language_translation(self, language: list) -> pd.DataFrame:
		language_translation_data = [
			{
				"iso_639_1": lang["iso_639_1"],
				"name": lang["english_name"],
				"language": self.default_language.code
			}
			for lang in language
		]
		# Add the extra languages -> NOT SUPPORTED
		return pd.DataFrame(language_translation_data)
		
