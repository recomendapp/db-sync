import pandas as pd
from ...models.language import Language
from ...models.extra_languages import ExtraLanguages

class Mappers:
	def __init__(self, language: list, default_language: Language, extra_languages: ExtraLanguages):
		self.language = language
		self.default_language = default_language
		self.extra_languages = extra_languages.languages
	
	def map_language(self) -> pd.DataFrame:
		language_data = [
			{
				"iso_639_1": lang["iso_639_1"],
				"name_in_native_language": lang["name"]
			}
			for lang in self.language
		]
		return pd.DataFrame(language_data)
	
	def map_language_translation(self) -> pd.DataFrame:
		language_translation_data = [
			{
				"iso_639_1": lang["iso_639_1"],
				"name": lang["english_name"],
				"language": self.default_language.code
			}
			for lang in self.language
		]
		# Add the extra languages -> NOT SUPPORTED

		return pd.DataFrame(language_translation_data)
		
