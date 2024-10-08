import pandas as pd
from .config import CollectionConfig as Config

class Mapper:
	@staticmethod
	def collection(collection: dict) -> pd.DataFrame:
		collection_data = [
			{
				"id": collection["id"],
				"name": collection["name"],
			}
		]
		return pd.DataFrame(collection_data)

	@staticmethod
	def collection_translation(collection: dict) -> pd.DataFrame:
		collectionId = collection["id"]
		translations = collection["translations"]
		collection_translation_data = [
			{
				"collection": collectionId,
				"title": translation["data"].get("title", None),
				"overview": translation["data"].get("overview", None),
				"homepage": translation["data"].get("homepage", None),
				"iso_639_1": translation["iso_639_1"],
				"iso_3166_1": translation["iso_3166_1"]
			}
			for translation in translations
			if translation["data"].get("title") or translation["data"].get("overview") or translation["data"].get("homepage")
		]

		return pd.DataFrame(collection_translation_data)
	
	@staticmethod
	def collection_image(collection: dict) -> pd.DataFrame:
		collectionId = collection["id"]
		
		# Adding backdrops and posters
		collection_image_data = [
			{
				"collection": collectionId,
				"file_path": image["file_path"],
				"type": "backdrop",
				"aspect_ratio": image["aspect_ratio"],
				"height": image["height"],
				"width": image["width"],
				"vote_average": image["vote_average"],
				"vote_count": image["vote_count"],
				"iso_639_1": image["iso_639_1"]
			}
			for image in collection["backdrops"]
		] + [
			{
				"collection": collectionId,
				"file_path": image["file_path"],
				"type": "poster",
				"aspect_ratio": image["aspect_ratio"],
				"height": image["height"],
				"width": image["width"],
				"vote_average": image["vote_average"],
				"vote_count": image["vote_count"],
				"iso_639_1": image["iso_639_1"]
			}
			for image in collection["posters"]
		]

		return pd.DataFrame(collection_image_data)

	