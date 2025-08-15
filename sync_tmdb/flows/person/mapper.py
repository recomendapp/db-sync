import pandas as pd
from .config import PersonConfig as Config

class Mapper:
	@staticmethod
	def person(person: dict) -> pd.DataFrame:
		person_data = [
			{
				"id": person["id"],
				"adult": person.get("adult", False),
				"birthday": person.get("birthday", None),
				"deathday": person.get("deathday", None),
				"gender": person.get("gender", None),
				"homepage": person.get("homepage", None),
				"imdb_id": person.get("imdb_id", None),
				"known_for_department": person.get("known_for_department", None),
				"name": person.get("name", None),
				"place_of_birth": person.get("place_of_birth", None),
				"popularity": person.get("popularity", None)
			}
		]
		return pd.DataFrame(person_data)

	@staticmethod
	def person_translation(person: dict) -> pd.DataFrame:
		person_translation_data = [
			{
				"person": person["id"],
				"biography": translation["data"].get("biography", None),
				"iso_639_1": translation["iso_639_1"],
				"iso_3166_1": translation["iso_3166_1"]
			}
			for translation in person.get("translations", {}).get("translations", [])
			if translation["data"].get("biography")
		]

		return pd.DataFrame(person_translation_data)

	@staticmethod
	def person_image(person: dict) -> pd.DataFrame:
		personId = person["id"]
		images = person.get("images", {}).get("profiles", [])
		person_image_data = [
			{
				"person": personId,
				"file_path": image["file_path"],
				"aspect_ratio": image.get("aspect_ratio", None),
				"height": image.get("height", None),
				"width": image.get("width", None),
				"vote_average": image.get("vote_average", None),
				"vote_count": image.get("vote_count", None)
			}
			for image in images
		]

		return pd.DataFrame(person_image_data)
	
	@staticmethod
	def person_external_id(person: dict) -> pd.DataFrame:
		personId = person["id"]
		external_ids = person.get("external_ids", {})
		person_external_id_data = [
			{
				"person": personId,
				"source": source.replace("_id", "") if source.endswith("_id") else source,
				"value": external_ids[source]
			}
			for source in external_ids
			if external_ids[source]
		]

		return pd.DataFrame(person_external_id_data)
	
	@staticmethod
	def person_also_known_as(person: dict) -> pd.DataFrame:
		personId = person["id"]
		also_known_as = person.get("also_known_as", [])
		person_also_known_as_data = [
			{
				"person": personId,
				"name": name
			}
			for name in also_known_as if name
		]

		return pd.DataFrame(person_also_known_as_data)

	@staticmethod
	def typesense(person: dict) -> dict:
		also_known_as_list = person.get("also_known_as", [])
		all_names_set = set(also_known_as_list)
		name = person.get("name")
		if name and name.strip():
			all_names_set.add(name.strip())
		
		also_known_as_final = list(all_names_set)
		doc = {
			"id": str(person["id"]),
			"name": name or "",
			"also_known_as": also_known_as_final,
			"popularity": float(person.get("popularity", 0.0)),
		}
		known_for_department = person.get("known_for_department")
		if known_for_department:
			doc["known_for_department"] = known_for_department
			
		return doc