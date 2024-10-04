import pandas as pd
from ...models.language import Language
from ...models.extra_languages import ExtraLanguages

class Mappers:
	def __init__(self, genres: dict, default_language: Language, extra_languages: ExtraLanguages):
		self.default_language = default_language
		self.extra_languages = extra_languages.languages
		self.genre = self._map_genre(genres)
		self.genre_translation = self._map_genre_translation(genres)
	
	def _map_genre(self, genres: dict) -> pd.DataFrame:
		genre_data = [
			{
				"id": item["id"],
			}
			for item in genres[self.default_language.code]
		]
		return pd.DataFrame(genre_data)
	
	def _map_genre_translation(self, genres: dict) -> pd.DataFrame:
		genre_translation_data = [
			{
				"genre": item["id"],
				"name": item["name"],
				"language": self.default_language.code
			}
			for item in genres[self.default_language.code]
		]
		# Add the extra languages
		for extra_language in self.extra_languages:
			genre_translation_data.extend([
				{
					"genre": item["id"],
					"name": item["name"],
					"language": extra_language.code
				}
				for item in genres[extra_language.code]
			])
		return pd.DataFrame(genre_translation_data)
		
