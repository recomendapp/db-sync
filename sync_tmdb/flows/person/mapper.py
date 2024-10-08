import pandas as pd
from .config import PersonConfig as Config

class Mapper:
	@staticmethod
	def person(config: Config, person: dict) -> pd.DataFrame:
		person_data = [
			{
				"id": person[config.default_language.code]["id"],
				"adult": person[config.default_language.code].get("adult", False),
				"also_known_as": person[config.default_language.code].get("also_known_as", []),
				"birthday": person[config.default_language.code].get("birthday", None),
				"deathday": person[config.default_language.code].get("deathday", None),
				"gender": person[config.default_language.code].get("gender", None),
				"homepage": person[config.default_language.code].get("homepage", None),
				"imdb_id": person[config.default_language.code].get("imdb_id", None),
				"known_for_department": person[config.default_language.code].get("known_for_department", None),
				"name": person[config.default_language.code].get("name", None),
				"place_of_birth": person[config.default_language.code].get("place_of_birth", None),
				"popularity": person[config.default_language.code].get("popularity", None),
				"profile_path": person[config.default_language.code].get("profile_path", None)
			}
		]
		return pd.DataFrame(person_data)

	@staticmethod
	def person_translation(person: dict) -> pd.DataFrame:
		person_translation_data = [
			{
				"person": person[language]["id"],
				"biography": person[language].get("biography", None),
				"language": language
			}
			for language in person.keys()
		]

		return pd.DataFrame(person_translation_data)

	