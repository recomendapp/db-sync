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
		# df = df.replace({np.nan: None})
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
	def movie_translations(config: Config, movie: dict) -> pd.DataFrame:
		# ["movie_id", "overview", "tagline", "title", "homepage", "runtime", "iso_639_1", "iso_3166_1"]
		movie_translation_data = [
			{
				"movie_id": movie["id"],
				"overview": nullify(translation["data"].get("overview", None), ""),
				"tagline": nullify(translation["data"].get("tagline", None), ""),
				"title": nullify(translation["data"].get("title", None), ""),
				"homepage": nullify(translation["data"].get("homepage", None), ""),
				"runtime": translation["data"].get("runtime", 0),
				"iso_639_1": translation["iso_639_1"],
				"iso_3166_1": translation["iso_3166_1"]
			}
			for translation in movie.get("translations", {}).get("translations", [])
			if nullify(translation["data"].get("overview", None), "") or nullify(translation["data"].get("tagline", None), "") or nullify(translation["data"].get("title", None), "") or nullify(translation["data"].get("homepage", None), "") or nullify(translation["data"].get("runtime", 0), 0)
		]

		return pd.DataFrame(movie_translation_data)

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
	def typesense(config: Config, movie: dict) -> dict:
		original_title = movie.get("original_title")
		translated_titles = [
			t["data"]["title"]
			for t in movie.get("translations", {}).get("translations", [])
			if t["data"].get("title", "").strip()
		]

		all_titles_set = set(translated_titles)
		if original_title and original_title.strip():
			all_titles_set.add(original_title.strip())

		titles_list = list(all_titles_set)
		runtime = None
		for t in movie.get("translations", {}).get("translations", []):
			if t["iso_639_1"] == movie.get("original_language"):
				runtime = t["data"].get("runtime")
				if runtime is not None and runtime > 0:
					break

		if not runtime:
			for t in movie.get("translations", {}).get("translations", []):
				runtime = t["data"].get("runtime")
				if runtime is not None and runtime > 0:
					break
	
		if not runtime and movie.get("runtime") is not None and movie.get("runtime") > 0:
			runtime = movie.get("runtime")
		
		release_dates_by_type = [
			rd["release_dates"]
			for rd in movie.get("release_dates", {}).get("results", [])
			if rd.get("iso_3166_1") in config.db_countries
		]
		
		filtered_dates = []
		for rd_list in release_dates_by_type:
			for date_info in rd_list:
				if date_info.get("type") in [2, 3] and date_info.get("release_date"):
					filtered_dates.append(pd.to_datetime(date_info.get("release_date")))
		
		release_date = min(filtered_dates).timestamp() if filtered_dates else None

		doc = {
			"id": str(movie["id"]),
			"original_title": original_title or "",
			"titles": titles_list,
			"popularity": float(movie.get("popularity", 0.0)),
			"genre_ids": [g["id"] for g in movie.get("genres", [])]
		}

		if runtime is not None:
			doc["runtime"] = int(runtime)
		
		if release_date is not None:
			doc["release_date"] = int(release_date)

		return doc