import pandas as pd
import numpy as np
from .config import MovieConfig as Config
from ...utils.nullify import nullify

class Mapper:
	@staticmethod
	def movie(config: Config, movie: dict) -> pd.DataFrame:
		# ["id", "adult", "budget", "original_language", "original_title", "popularity", "revenue", "status", "vote_average", "vote_count", "belongs_to_collection", "updated_at"]
		movie_data = [
			{
				"id": movie["id"],
				"adult": movie.get("adult", False),
				"budget": movie.get("budget", 0),
				"original_language": nullify(movie.get("original_language", None), ""),
				"original_title": nullify(movie.get("original_title", None), ""),
				"popularity": movie.get("popularity", 0),
				"revenue": movie.get("revenue", 0),
				"status": nullify(movie.get("status", None), ""),
				"vote_average": movie.get("vote_average", 0),
				"vote_count": movie.get("vote_count", 0),
				"belongs_to_collection": movie.get("belongs_to_collection", {}).get("id") if movie.get("belongs_to_collection") and movie.get("belongs_to_collection").get("id") in config.db_collections else None,
			}
		]
		df = pd.DataFrame(movie_data)
		df = df.convert_dtypes()
		return df
	
	@staticmethod
	def movies(config: Config, movies: list[dict]) -> pd.DataFrame:
		# ["id", "adult", "budget", "original_language", "original_title", "popularity", "revenue", "status", "vote_average", "vote_count", "belongs_to_collection", "updated_at"]
		movies_data = [
			{
				"id": movie["id"],
				"adult": movie.get("adult", False),
				"budget": movie.get("budget", 0),
				"original_language": nullify(movie.get("original_language", None), ""),
				"original_title": nullify(movie.get("original_title", None), ""),
				"popularity": movie.get("popularity", 0),
				"revenue": movie.get("revenue", 0),
				"status": nullify(movie.get("status", None), ""),
				"vote_average": movie.get("vote_average", 0),
				"vote_count": movie.get("vote_count", 0),
				"belongs_to_collection": movie.get("belongs_to_collection", {}).get("id") if movie.get("belongs_to_collection") and movie.get("belongs_to_collection").get("id") in config.db_collections else None,
			}
			for movie in movies
		]
		df = pd.DataFrame(movies_data)
		df = df.convert_dtypes()
		return df

	@staticmethod
	def movie_alternative_titles(config: Config, movie: dict) -> pd.DataFrame:
		# ["movie_id", "iso_3166_1", "title", "type"]
		movieId = movie["id"]
		alternative_titles = movie.get("alternative_titles", {}).get("titles", {})
		movie_alternative_titles_data = [
			{
				"movie_id": movieId,
				"iso_3166_1": alternative_title["iso_3166_1"],
				"title": alternative_title["title"],
				"type": nullify(alternative_title["type"], "")
			}
			for alternative_title in alternative_titles
		]

		return pd.DataFrame(movie_alternative_titles_data)
	
	@staticmethod
	def movies_alternative_titles(config: Config, movies: list[dict]) -> pd.DataFrame:
		# ["movie_id", "iso_3166_1", "title", "type"]
		movie_alternative_titles_data = [
			{
				"movie_id": movie["id"],
				"iso_3166_1": alternative_title["iso_3166_1"],
				"title": alternative_title["title"],
				"type": nullify(alternative_title["type"], "")
			}
			for movie in movies
			for alternative_title in movie.get("alternative_titles", {}).get("titles", [])
		]

		return pd.DataFrame(movie_alternative_titles_data)
	
	@staticmethod
	def movie_credits(config: Config, movie: dict) -> pd.DataFrame:
		# ["id", "movie_id", "person_id", "department", "job"]
		movieId = movie["id"]
		credits = movie.get("credits", {})
		movie_credits_data = []
		movie_roles_data = []

		for credit in credits.get("cast", []) + credits.get("crew", []):
			if credit["id"] in config.db_persons:
				movie_credits_data.append({
					"id": credit["credit_id"],
					"movie_id": movieId,
					"person_id": credit["id"],
					"department": credit["department"] if "department" in credit else "Acting",
					"job": credit["job"] if "job" in credit else "Actor"
				})
				if "character" in credit:
					movie_roles_data.append({
						"credit_id": credit["credit_id"],
						"character": nullify(credit["character"], ""),
						"order": credit.get("order", 0),
					})
		
		return pd.DataFrame(movie_credits_data), pd.DataFrame(movie_roles_data)
	
	@staticmethod
	def movies_credits(config: Config, movies: list[dict]) -> pd.DataFrame:
		# ["id", "movie_id", "person_id", "department", "job"]
		movies_credits_data = []
		movies_roles_data = []

		for movie in movies:
			credits = movie.get("credits", {})
			for credit in credits.get("cast", []) + credits.get("crew", []):
				if credit["id"] in config.db_persons:
					movies_credits_data.append({
						"id": credit["credit_id"],
						"movie_id": movie["id"],
						"person_id": credit["id"],
						"department": credit["department"] if "department" in credit else "Acting",
						"job": credit["job"] if "job" in credit else "Actor"
					})
					if "character" in credit:
						movies_roles_data.append({
							"credit_id": credit["credit_id"],
							"character": nullify(credit["character"], ""),
							"order": credit.get("order", 0),
						})
		
		return pd.DataFrame(movies_credits_data), pd.DataFrame(movies_roles_data)
	

	@staticmethod
	def movie_external_ids(config: Config, movie: dict) -> pd.DataFrame:
		# ["movie_id", "source", "value"]
		movieId = movie["id"]
		external_ids = movie.get("external_ids", {})
		movie_external_ids_data = [
			{
				"movie_id": movieId,
				"source": source.replace("_id", "") if source.endswith("_id") else source,
				"value": external_ids[source]
			}
			for source in external_ids
			if external_ids.get(source)
		]

		return pd.DataFrame(movie_external_ids_data)

	@staticmethod
	def movies_external_ids(config: Config, movies: list[dict]) -> pd.DataFrame:
		# ["movie_id", "source", "value"]
		movies_external_ids_data = [
			{
				"movie_id": movie["id"],
				"source": source.replace("_id", "") if source.endswith("_id") else source,
				"value": external_ids[source]
			}
			for movie in movies
			for external_ids in movie.get("external_ids", {})
			if isinstance(external_ids, dict)
			for source in external_ids
			if external_ids.get(source)
		]

		return pd.DataFrame(movies_external_ids_data)

	@staticmethod
	def movie_genres(config: Config, movie: dict) -> pd.DataFrame:
		# ["movie_id", "genre_id"]
		movieId = movie["id"]
		genres = movie.get("genres", [])
		movie_genres_data = [
			{
				"movie_id": movieId,
				"genre_id": genre["id"]
			}
			for genre in genres
			if genre["id"] in config.db_genres
		]

		return pd.DataFrame(movie_genres_data)
	
	@staticmethod
	def movies_genres(config: Config, movies: list[dict]) -> pd.DataFrame:
		# ["movie_id", "genre_id"]
		movies_genres_data = [
			{
				"movie_id": movie["id"],
				"genre_id": genre["id"]
			}
			for movie in movies
			for genre in movie.get("genres", [])
			if genre["id"] in config.db_genres
		]

		return pd.DataFrame(movies_genres_data)
	
	@staticmethod
	def movie_images(config: Config, movie: dict) -> pd.DataFrame:
		# ["movie_id", "file_path", "type", "aspect_ratio", "height", "width", "vote_average", "vote_count", "iso_639_1"]
		movieId = movie["id"]
		images = movie.get("images", {})
		movie_image_data = [
			{
				"movie_id": movieId,
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

		return pd.DataFrame(movie_image_data)
	
	@staticmethod
	def movies_images(config: Config, movies: list[dict]) -> pd.DataFrame:
		# ["movie_id", "file_path", "type", "aspect_ratio", "height", "width", "vote_average", "vote_count", "iso_639_1"]
		movies_image_data = [
			{
				"movie_id": movie["id"],
				"file_path": image["file_path"],
				"type": imageType,
				"aspect_ratio": image.get("aspect_ratio", 0),
				"height": image.get("height", 0),
				"width": image.get("width", 0),
				"vote_average": image.get("vote_average", 0),
				"vote_count": image.get("vote_count", 0),
				"iso_639_1": nullify(image.get("iso_639_1", None), "")
			}
			for movie in movies
			for imageType in ["backdrop", "poster", "logo"]
			for image in movie.get("images", {}).get(imageType + "s", [])
		]

		return pd.DataFrame(movies_image_data)
	
	@staticmethod
	def movie_keywords(config: Config, movie: dict) -> pd.DataFrame:
		# ["movie_id", "keyword_id"]
		movieId = movie["id"]
		keywords = movie.get("keywords", {}).get("keywords", [])
		movie_keywords_data = [
			{
				"movie_id": movieId,
				"keyword_id": keyword["id"]
			}
			for keyword in keywords
			if keyword["id"] in config.db_keywords
		]

		return pd.DataFrame(movie_keywords_data)

	@staticmethod
	def movies_keywords(config: Config, movies: list[dict]) -> pd.DataFrame:
		# ["movie_id", "keyword_id"]
		movies_keywords_data = [
			{
				"movie_id": movie["id"],
				"keyword_id": keyword["id"]
			}
			for movie in movies
			for keyword in movie.get("keywords", {}).get("keywords", [])
			if keyword["id"] in config.db_keywords
		]

		return pd.DataFrame(movies_keywords_data)
	
	@staticmethod
	def movie_origin_country(config: Config, movie: dict) -> pd.DataFrame:
		# ["movie_id", "iso_3166_1"]
		movieId = movie["id"]
		origin_country = movie.get("origin_country", [])
		movie_origin_country_data = [
			{
				"movie_id": movieId,
				"iso_3166_1": country
			}
			for country in origin_country
			if country in config.db_countries
		]

		return pd.DataFrame(movie_origin_country_data)
	
	@staticmethod
	def movies_origin_country(config: Config, movies: list[dict]) -> pd.DataFrame:
		# ["movie_id", "iso_3166_1"]
		movies_origin_country_data = [
			{
				"movie_id": movie["id"],
				"iso_3166_1": country
			}
			for movie in movies
			for country in movie.get("origin_country", [])
			if country in config.db_countries
		]

		return pd.DataFrame(movies_origin_country_data)
	
	@staticmethod
	def movie_production_companies(config: Config, movie: dict) -> pd.DataFrame:
		# ["movie_id", "company_id"]
		movieId = movie["id"]
		production_companies = movie.get("production_companies", [])
		movie_production_companies_data = [
			{
				"movie_id": movieId,
				"company_id": company["id"]
			}
			for company in production_companies
			if company["id"] in config.db_companies
		]

		return pd.DataFrame(movie_production_companies_data)
	
	@staticmethod
	def movies_production_companies(config: Config, movies: list[dict]) -> pd.DataFrame:
		# ["movie_id", "company_id"]
		movies_production_companies_data = [
			{
				"movie_id": movie["id"],
				"company_id": company["id"]
			}
			for movie in movies
			for company in movie.get("production_companies", [])
			if company["id"] in config.db_companies
		]

		return pd.DataFrame(movies_production_companies_data)
	
	@staticmethod
	def movie_production_countries(config: Config, movie: dict) -> pd.DataFrame:
		# ["movie_id", "iso_3166_1"]
		movieId = movie["id"]
		production_countries = movie.get("production_countries", [])
		movie_production_countries_data = [
			{
				"movie_id": movieId,
				"iso_3166_1": country["iso_3166_1"]
			}
			for country in production_countries
			if country["iso_3166_1"] in config.db_countries
		]

		return pd.DataFrame(movie_production_countries_data)
	
	@staticmethod
	def movies_production_countries(config: Config, movies: list[dict]) -> pd.DataFrame:
		# ["movie_id", "iso_3166_1"]
		movies_production_countries_data = [
			{
				"movie_id": movie["id"],
				"iso_3166_1": country["iso_3166_1"]
			}
			for movie in movies
			for country in movie.get("production_countries", [])
			if country["iso_3166_1"] in config.db_countries
		]

		return pd.DataFrame(movies_production_countries_data)
	
	@staticmethod
	def movie_release_dates(config: Config, movie: dict) -> pd.DataFrame:
		# ["movie_id", "iso_3166_1", "release_date", "certification", "iso_639_1", "note", "release_type", "descriptors"]
		movieId = movie["id"]
		release_dates = movie.get("release_dates", {}).get("results", [])
		movie_release_dates_data = [
			{
				"movie_id": movieId,
				"iso_3166_1": release_iso_3166_1.get("iso_3166_1"),
				"release_date": release_date.get("release_date"),
				"certification": nullify(release_date.get("certification"), ""),
				"iso_639_1": nullify(release_date.get("iso_639_1"), "") if release_date.get("iso_639_1") and release_date.get("iso_639_1") in config.db_languages else None,
				"note": nullify(release_date.get("note"), ""),
				"release_type": release_date.get("type"),
				"descriptors": (
					"{" + ",".join(f'"{descriptor}"' for descriptor in release_date.get("descriptors")) + "}"
					if nullify(release_date.get("descriptors"), []) else None
				)
			}
			for release_iso_3166_1 in release_dates
			for release_date in release_iso_3166_1.get("release_dates", [])
			if release_iso_3166_1.get("iso_3166_1") in config.db_countries
		]

		return pd.DataFrame(movie_release_dates_data)
	
	@staticmethod
	def movies_release_dates(config: Config, movies: list[dict]) -> pd.DataFrame:
		# ["movie_id", "iso_3166_1", "release_date", "certification", "iso_639_1", "note", "release_type", "descriptors"]
		movies_release_dates_data = [
			{
				"movie_id": movie["id"],
				"iso_3166_1": release_iso_3166_1.get("iso_3166_1"),
				"release_date": release_date.get("release_date"),
				"certification": nullify(release_date.get("certification"), ""),
				"iso_639_1": nullify(release_date.get("iso_639_1"), "") if release_date.get("iso_639_1") and release_date.get("iso_639_1") in config.db_languages else None,
				"note": nullify(release_date.get("note"), ""),
				"release_type": release_date.get("type"),
				"descriptors": (
					"{" + ",".join(f'"{descriptor}"' for descriptor in release_date.get("descriptors")) + "}"
					if nullify(release_date.get("descriptors"), []) else None
				)
			}
			for movie in movies
			for release_iso_3166_1 in movie.get("release_dates", {}).get("results", [])
			for release_date in release_iso_3166_1.get("release_dates", [])
			if release_iso_3166_1.get("iso_3166_1") in config.db_countries
		]

		return pd.DataFrame(movies_release_dates_data)

	@staticmethod
	def movie_spoken_languages(config: Config, movie: dict) -> pd.DataFrame:
		# ["movie_id", "iso_639_1"]
		movieId = movie["id"]
		spoken_languages = movie.get("spoken_languages", [])
		movie_spoken_languages_data = [
			{
				"movie_id": movieId,
				"iso_639_1": language["iso_639_1"]
			}
			for language in spoken_languages
			if language["iso_639_1"] in config.db_languages
		]

		return pd.DataFrame(movie_spoken_languages_data)
	
	@staticmethod
	def movies_spoken_languages(config: Config, movies: list[dict]) -> pd.DataFrame:
		# ["movie_id", "iso_639_1"]
		movies_spoken_languages_data = [
			{
				"movie_id": movie["id"],
				"iso_639_1": language["iso_639_1"]
			}
			for movie in movies
			for language in movie.get("spoken_languages", [])
			if language["iso_639_1"] in config.db_languages
		]

		return pd.DataFrame(movies_spoken_languages_data)
	
	@staticmethod
	def movie_translations(config: Config, movie: dict) -> pd.DataFrame:
		# ["movie_id", "overview", "tagline", "title", "homepage", "runtime", "iso_639_1", "iso_3166_1"]
		movie_translation_data = [
			{
				"movie_id": movie["id"],
				"overview": nullify(translation.get("overview", None), ""),
				"tagline": nullify(translation.get("tagline", None), ""),
				"title": nullify(translation.get("title", None), ""),
				"homepage": nullify(translation.get("homepage", None), ""),
				"runtime": translation.get("runtime", 0),
				"iso_639_1": translation["iso_639_1"],
				"iso_3166_1": translation["iso_3166_1"]
			}
			for translation in movie.get("translations", {}).get("translations", [])
			# only add if there is something to translate (overview, tagline, title, homepage, runtime)
			# check if there are empty strings or 0
			if nullify(translation.get("overview", None), "") or nullify(translation.get("tagline", None), "") or nullify(translation.get("title", None), "") or nullify(translation.get("homepage", None), "") or nullify(translation.get("runtime", 0), 0)
		]

		return pd.DataFrame(movie_translation_data)
	
	@staticmethod
	def movies_translations(config: Config, movies: list[dict]) -> pd.DataFrame:
		# ["movie_id", "overview", "tagline", "title", "homepage", "runtime", "iso_639_1", "iso_3166_1"]
		movies_translation_data = [
			{
				"movie_id": movie["id"],
				"overview": nullify(translation.get("overview", None), ""),
				"tagline": nullify(translation.get("tagline", None), ""),
				"title": nullify(translation.get("title", None), ""),
				"homepage": nullify(translation.get("homepage", None), ""),
				"runtime": translation.get("runtime", 0),
				"iso_639_1": translation["iso_639_1"],
				"iso_3166_1": translation["iso_3166_1"]
			}
			for movie in movies
			for translation in movie.get("translations", {}).get("translations", [])
			# only add if there is something to translate (overview, tagline, title, homepage, runtime)
			# check if there are empty strings or 0
			if nullify(translation.get("overview", None), "") or nullify(translation.get("tagline", None), "") or nullify(translation.get("title", None), "") or nullify(translation.get("homepage", None), "") or nullify(translation.get("runtime", 0), 0)
		]

		return pd.DataFrame(movies_translation_data)
	
	@staticmethod
	def movie_videos(config: Config, movie: dict) -> pd.DataFrame:
		# ["id", "movie_id", "iso_639_1", "iso_3166_1", "name", "key", "site", "size", "type", "official", "published_at"]
		movieId = movie["id"]
		videos = movie.get("videos", {}).get("results", [])
		movie_videos_data = [
			{
				"id": video["id"],
				"movie_id": movieId,
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

		return pd.DataFrame(movie_videos_data)
	
	@staticmethod
	def movies_videos(config: Config, movies: list[dict]) -> pd.DataFrame:
		# ["id", "movie_id", "iso_639_1", "iso_3166_1", "name", "key", "site", "size", "type", "official", "published_at"]
		movies_videos_data = [
			{
				"id": video["id"],
				"movie_id": movie["id"],
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
			for movie in movies
			for video in movie.get("videos", {}).get("results", [])
		]

		return pd.DataFrame(movies_videos_data)