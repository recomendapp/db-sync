import pandas as pd
import numpy as np
from .config import SerieConfig as Config
from ...utils.nullify import nullify

class Mapper:
	@staticmethod
	def serie(config: Config, serie: dict) -> pd.DataFrame:
		serie_data = [
			{
				"id": serie["id"],
				"adult": serie.get("adult", False),
				"in_production": serie.get("in_production", False),
				"original_language": nullify(serie.get("original_language", None), ""),
				"original_name": nullify(serie.get("original_name", None), ""),
				"popularity": serie.get("popularity", 0),
				"status": nullify(serie.get("status", None), ""),
				"type": nullify(serie.get("type", None), ""),
				"vote_average": serie.get("vote_average", 0),
				"vote_count": serie.get("vote_count", 0),
			}
		]
		df = pd.DataFrame(serie_data)
		df = df.convert_dtypes()
		return df

	@staticmethod
	def serie_alternative_titles(config: Config, serie: dict) -> pd.DataFrame:
		serieId = serie["id"]
		alternative_titles = serie.get("alternative_titles", {}).get("results", [])
		serie_alternative_titles_data = [
			{
				"serie_id": serieId,
				"iso_3166_1": alternative_title["iso_3166_1"],
				"title": alternative_title["title"],
				"type": nullify(alternative_title["type"], "")
			}
			for alternative_title in alternative_titles
		]

		return pd.DataFrame(serie_alternative_titles_data)

	@staticmethod
	def serie_content_ratings(config: Config, serie: dict) -> pd.DataFrame:
		serieId = serie["id"]
		content_ratings = serie.get("content_ratings", {}).get("results", [])
		serie_content_ratings_data = [
			{
				"serie_id": serieId,
				"iso_3166_1": content_rating["iso_3166_1"],
				"rating": content_rating["rating"],
				"descriptors": (
					"{" + ",".join(f'"{descriptor}"' for descriptor in content_rating.get("descriptors")) + "}"
					if content_rating.get("descriptors")
					else None
				),
			}
			for content_rating in content_ratings
		]

		return pd.DataFrame(serie_content_ratings_data)
	
	@staticmethod
	def serie_external_ids(config: Config, serie: dict) -> pd.DataFrame:
		serieId = serie["id"]
		external_ids = serie.get("external_ids", {})
		serie_external_ids_data = [
			{
				"serie_id": serieId,
				"source": source.replace("_id", "") if source.endswith("_id") else source,
				"value": external_ids[source]
			}
			for source in external_ids
			if external_ids.get(source)
		]

		return pd.DataFrame(serie_external_ids_data)
	
	@staticmethod
	def serie_genres(config: Config, serie: dict) -> pd.DataFrame:
		serieId = serie["id"]
		genres = serie.get("genres", [])
		serie_genres_data = [
			{
				"serie_id": serieId,
				"genre_id": genre["id"]
			}
			for genre in genres
			if genre["id"] in config.db_genres
		]

		return pd.DataFrame(serie_genres_data)
	
	@staticmethod
	def serie_images(config: Config, serie: dict) -> pd.DataFrame:
		serieId = serie["id"]
		images = serie.get("images", {})
		serie_image_data = [
			{
				"serie_id": serieId,
				"file_path": image["file_path"],
				"type": imageType,
				"aspect_ratio": image.get("aspect_ratio", 0),
				"height": image.get("height", 0),
				"width": image.get("width", 0),
				"vote_average": image.get("vote_average", 0),
				"vote_count": image.get("vote_count", 0),
				"iso_639_1": nullify(image.get("iso_639_1", None), "")
			}
			for imageType in ["backdrop", "poster", "logo"]
			for image in images.get(imageType + "s", [])
		]

		return pd.DataFrame(serie_image_data)
	
	@staticmethod
	def serie_keywords(config: Config, serie: dict) -> pd.DataFrame:
		serieId = serie["id"]
		keywords = serie.get("keywords", {}).get("results", [])
		serie_keywords_data = [
			{
				"serie_id": serieId,
				"keyword_id": keyword["id"]
			}
			for keyword in keywords
			if keyword["id"] in config.db_keywords
		]

		return pd.DataFrame(serie_keywords_data)
	
	@staticmethod
	def serie_languages(config: Config, serie: dict) -> pd.DataFrame:
		serieId = serie["id"]
		languages = serie.get("languages", [])
		serie_languages_data = [
			{
				"serie_id": serieId,
				"iso_639_1": language
			}
			for language in languages
			if language in config.db_languages
		]

		return pd.DataFrame(serie_languages_data)
	
	@staticmethod
	def serie_networks(config: Config, serie: dict) -> pd.DataFrame:
		serieId = serie["id"]
		networks = serie.get("networks", [])
		serie_networks_data = [
			{
				"serie_id": serieId,
				"network_id": network["id"]
			}
			for network in networks
			if network["id"] in config.db_networks
		]

		return pd.DataFrame(serie_networks_data)
	
	@staticmethod
	def serie_origin_country(config: Config, serie: dict) -> pd.DataFrame:
		serieId = serie["id"]
		origin_country = serie.get("origin_country", [])
		serie_origin_country_data = [
			{
				"serie_id": serieId,
				"iso_3166_1": country
			}
			for country in origin_country
			if country in config.db_countries
		]

		return pd.DataFrame(serie_origin_country_data)
	
	@staticmethod
	def serie_production_companies(config: Config, serie: dict) -> pd.DataFrame:
		serieId = serie["id"]
		production_companies = serie.get("production_companies", [])
		serie_production_companies_data = [
			{
				"serie_id": serieId,
				"company_id": company["id"]
			}
			for company in production_companies
			if company["id"] in config.db_companies
		]

		return pd.DataFrame(serie_production_companies_data)
	
	@staticmethod
	def serie_production_countries(config: Config, serie: dict) -> pd.DataFrame:
		serieId = serie["id"]
		production_countries = serie.get("production_countries", [])
		serie_production_countries_data = [
			{
				"serie_id": serieId,
				"iso_3166_1": country["iso_3166_1"]
			}
			for country in production_countries
			if country["iso_3166_1"] in config.db_countries
		]

		return pd.DataFrame(serie_production_countries_data)
	
	@staticmethod
	def serie_spoken_languages(config: Config, serie: dict) -> pd.DataFrame:
		serieId = serie["id"]
		spoken_languages = serie.get("spoken_languages", [])
		serie_spoken_languages_data = [
			{
				"serie_id": serieId,
				"iso_639_1": language["iso_639_1"]
			}
			for language in spoken_languages
			if language["iso_639_1"] in config.db_languages
		]

		return pd.DataFrame(serie_spoken_languages_data)
	
	@staticmethod
	def serie_translations(config: Config, serie: dict) -> pd.DataFrame:
		serie_translation_data = [
			{
				"serie_id": serie["id"],
				"name": nullify(translation["data"].get("name", None), ""),
				"overview": nullify(translation["data"].get("overview", None), ""),
				"homepage": nullify(translation["data"].get("homepage", None), ""),
				"tagline": nullify(translation["data"].get("tagline", None), ""),
				"iso_639_1": translation["iso_639_1"],
				"iso_3166_1": translation["iso_3166_1"]
			}
			for translation in serie.get("translations", {}).get("translations", [])
			if nullify(translation["data"].get("name", None), "") or nullify(translation["data"].get("overview", None), "") or nullify(translation["data"].get("homepage", None), "") or nullify(translation["data"].get("tagline", None), "")
		]

		return pd.DataFrame(serie_translation_data)
	
	@staticmethod
	def serie_videos(config: Config, serie: dict) -> pd.DataFrame:
		serieId = serie["id"]
		videos = serie.get("videos", {}).get("results", [])
		serie_videos_data = [
			{
				"id": video["id"],
				"serie_id": serieId,
				"iso_639_1": video.get("iso_639_1", None),
				"iso_3166_1": video.get("iso_3166_1", None),
				"name": video.get("name", None),
				"key": video.get("key", None),
				"site": video.get("site", None),
				"size": video.get("size", None),
				"type": video.get("type", None),
				"official": video.get("official", False),
				"published_at": video.get("published_at", None)
			}
			for video in videos
		]

		return pd.DataFrame(serie_videos_data)
	
	@staticmethod
	def serie_credits(config: Config, serie: dict) -> pd.DataFrame:
		serieId = serie["id"]
		credits = serie.get("aggregate_credits", {})
		movie_credits_data = []

		for credit in credits.get("cast", []):
			if credit["id"] in config.db_persons:
				for role in credit.get("roles", []):
					movie_credits_data.append(
						{
							"id": role["credit_id"],
							"serie_id": serieId,
							"person_id": credit["id"],
							"department": "Acting",
							"job": "Actor",
							"character": role["character"],
							"episode_count": role["episode_count"]
						}
					)
		
		for credit in credits.get("crew", []):
			if credit["id"] in config.db_persons:
				for role in credit.get("jobs", []):
					movie_credits_data.append(
						{
							"id": role["credit_id"],
							"serie_id": serieId,
							"person_id": credit["id"],
							"department": credit["department"],
							"job": role["job"],
							"character": None,
							"episode_count": role["episode_count"]
						}
					)
		
		return pd.DataFrame(movie_credits_data)