import pandas as pd
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
				"budget": nullify(movie.get("budget", None), 0),
				"original_language": nullify(movie.get("original_language", None), ""),
				"original_title": nullify(movie.get("original_title", None), ""),
				"popularity": nullify(movie.get("popularity", None), 0),
				"revenue": nullify(movie.get("revenue", None), 0),
				"status": nullify(movie.get("status", None), ""),
				"vote_average": nullify(movie.get("vote_average", None), 0),
				"vote_count": nullify(movie.get("vote_count", None), 0),
				"belongs_to_collection": movie.get("belongs_to_collection", {}).get("id") if movie.get("belongs_to_collection") and movie.get("belongs_to_collection").get("id") in config.db_collections else None,
			}
		]
		return pd.DataFrame(movie_data)

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
					"id": credit["id"],
					"movie_id": movieId,
					"person_id": credit["id"],
					"department": credit["department"] if "department" in credit else "Acting",
					"job": credit["job"] if "job" in credit else "Actor"
				})
				if "character" in credit:
					movie_roles_data.append({
						"movie_id": movieId,
						"character": nullify(credit["character"], ""),
						"order": nullify(credit["order"], 0),
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
				"aspect_ratio": nullify(image.get("aspect_ratio", None), 0),
				"height": nullify(image.get("height", None), 0),
				"width": nullify(image.get("width", None), 0),
				"vote_average": nullify(image.get("vote_average", None), 0),
				"vote_count": nullify(image.get("vote_count", None), 0),
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
				"descriptors": nullify(release_date.get("descriptors"), [])
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
				"overview": nullify(translation.get("overview", None), ""),
				"tagline": nullify(translation.get("tagline", None), ""),
				"title": nullify(translation.get("title", None), ""),
				"homepage": nullify(translation.get("homepage", None), ""),
				"runtime": nullify(translation.get("runtime", None), 0),
				"iso_639_1": translation["iso_639_1"],
				"iso_3166_1": translation["iso_3166_1"]
			}
			for translation in movie.get("translations", {}).get("translations", [])
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