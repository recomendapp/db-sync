import pandas as pd
from .config import MovieConfig as Config

class Mapper:
	@staticmethod
	def movie(movie: dict) -> pd.DataFrame:
		movie_data = [
			{
				"id": movie["id"],
				"adult": movie.get("adult", False),
				"budget": movie.get("budget", None),
				"collection": movie.get("belongs_to_collection", {}).get("id", None),
				"original_language": movie.get("original_language", None),
				"original_title": movie.get("original_title", None),
				"popularity": movie.get("popularity", None),
				"release_date": movie.get("release_date", None),
				"revenue": movie.get("revenue", None),
				# "runtime": movie.get("runtime", None), (inside translations)
				"status": movie.get("status", None),
				# "title": movie.get("title", None), (inside translations)
				# "video": movie.get("video", False), (inside videos)
				"vote_average": movie.get("vote_average", None),
				"vote_count": movie.get("vote_count", None),
			}
		]
		return pd.DataFrame(movie_data)

	@staticmethod
	def movie_translation(movie: dict) -> pd.DataFrame:
		movie_translation_data = [
			{
				"movie": movie["id"],
				"homepage": translation["data"].get("homepage", None),
				"overview": translation["data"].get("overview", None),
				"runtime": translation["data"].get("runtime", None) if translation["data"].get("runtime", None) != 0 else None,
				"tagline": translation["data"].get("tagline", None),
				"title": translation["data"].get("title", None),
				"iso_639_1": translation["iso_639_1"],
				"iso_3166_1": translation["iso_3166_1"]
			}
			for translation in movie.get("translations", {}).get("translations", [])
			if translation["data"].get("homepage", None) or translation["data"].get("overview", None) or translation["data"].get("runtime", None) or translation["data"].get("tagline", None) or translation["data"].get("title", None)
		]

		return pd.DataFrame(movie_translation_data)

	@staticmethod
	def movie_image(movie: dict) -> pd.DataFrame:
		movieId = movie["id"]
		images = movie.get("images", {})
		movie_image_data = [
			{
				"movie": movieId,
				"file_path": image["file_path"],
				"type": "backdrop",
				"aspect_ratio": image["aspect_ratio"],
				"height": image["height"],
				"width": image["width"],
				"vote_average": image["vote_average"],
				"vote_count": image["vote_count"],
				"iso_639_1": image["iso_639_1"]
			}
			for image in images["backdrops"]
		] + [
			{
				"movie": movieId,
				"file_path": image["file_path"],
				"type": "poster",
				"aspect_ratio": image["aspect_ratio"],
				"height": image["height"],
				"width": image["width"],
				"vote_average": image["vote_average"],
				"vote_count": image["vote_count"],
				"iso_639_1": image["iso_639_1"]
			}
			for image in images["posters"]
		] + [
			{
				"movie": movieId,
				"file_path": image["file_path"],
				"type": "logo",
				"aspect_ratio": image["aspect_ratio"],
				"height": image["height"],
				"width": image["width"],
				"vote_average": image["vote_average"],
				"vote_count": image["vote_count"],
				"iso_639_1": image["iso_639_1"]
			}
			for image in images["logos"]
		]

		return pd.DataFrame(movie_image_data)
	
	@staticmethod
	def movie_alternative_titles(movie: dict) -> pd.DataFrame:
		movieId = movie["id"]
		alternative_titles = movie.get("alternative_titles", {}).get("titles", {})
		movie_alternative_titles_data = [
			{
				"movie": movieId,
				"iso_3166_1": alternative_title["iso_3166_1"],
				"title": alternative_title["title"],
				"type": alternative_title["type"]
			}
			for alternative_title in alternative_titles
		]

		return pd.DataFrame(movie_alternative_titles_data)

	
	