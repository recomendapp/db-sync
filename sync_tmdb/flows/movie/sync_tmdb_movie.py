# ---------------------------------------------------------------------------- #
#                                    Imports                                   #
# ---------------------------------------------------------------------------- #

from datetime import date
import pandas as pd
from more_itertools import chunked
import time
import ast

# ---------------------------------- Prefect --------------------------------- #
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.futures import wait

from .config import MovieConfig
from .mapper import Mapper
from ...models.csv_file import CSVFile

# ---------------------------------------------------------------------------- #

# ---------------------------------------------------------------------------- #
#                                    Getters                                   #
# ---------------------------------------------------------------------------- #

def get_tmdb_movies(config: MovieConfig) -> set:
	try:
		tmdb_movies = config.tmdb_client.get_export_ids(type="movie", date=config.date)
		tmdb_movies_set = set([item["id"] for item in tmdb_movies])
		return tmdb_movies_set
	except Exception as e:
		raise ValueError(f"Failed to get TMDB movies: {e}")

def get_db_movies(config: MovieConfig) -> set:
	conn = config.db_client.get_connection()
	try:
		with conn.cursor() as cursor:
			cursor.execute(f"SELECT id FROM {config.table_movie}")
			db_movies = cursor.fetchall()
			db_movies_set = set([item[0] for item in db_movies])
			return db_movies_set
	except Exception as e:
		raise ValueError(f"Failed to get database movies: {e}")
	finally:
		config.db_client.return_connection(conn)
	
def get_tmdb_movies_changed(config: MovieConfig):
	try:
		config.logger.info("Getting changed movies...")
		changed_movies = config.tmdb_client.get_changed_ids(type="movie", start_date=config.log_manager.last_success_log.date, end_date=config.date)
		config.missing_movies |= changed_movies
	except Exception as e:
		raise ValueError(f"Failed to get changed movies: {e}")

@task
def get_tmdb_movie_details(config: MovieConfig, movie_id: int) -> dict:
	try:
		main_video_languages = "en,fr,es,ja,de"
		# TMDB limit the number of languages to 5 
		movie = config.tmdb_client.request(f"movie/{movie_id}", {"append_to_response": "alternative_titles,credits,external_ids,keywords,release_dates,translations,videos", "include_video_language": main_video_languages})
		images = config.tmdb_client.request(f"movie/{movie_id}/images")
		# Add images to the movie details
		movie["images"] = images
		return movie
	except Exception as e:
		config.logger.error(f"Failed to get movie details for {movie_id}: {e}")
		return None

# ---------------------------------------------------------------------------- #
	
def process_missing_movies(config: MovieConfig):
	try:
		if len(config.missing_movies) > 0:
			config.get_db_data()
			chunks = list(chunked(config.missing_movies, 100))
			for chunk in chunks:
				csv: dict[str, CSVFile] = {}
				csv["movie"] = CSVFile(
					columns=config.movie_columns,
					tmp_directory=config.tmp_directory,
					prefix=config.flow_name
				)
				csv["movie_alternative_titles"] = CSVFile(
					columns=config.movie_alternative_titles_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_alternative_titles"
				)
				csv["movie_credits"] = CSVFile(
					columns=config.movie_credits_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_credits"
				)
				csv["movie_external_ids"] = CSVFile(
					columns=config.movie_external_ids_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_external_ids"
				)
				csv["movie_genres"] = CSVFile(
					columns=config.movie_genres_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_genres"
				)
				csv["movie_images"] = CSVFile(
					columns=config.movie_images_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_images"
				)
				csv["movie_keywords"] = CSVFile(
					columns=config.movie_keywords_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_keywords"
				)
				csv["movie_origin_country"] = CSVFile(
					columns=config.movie_origin_country_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_origin_country"
				)
				csv["movie_production_companies"] = CSVFile(
					columns=config.movie_production_companies_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_production_companies"
				)
				csv["movie_production_countries"] = CSVFile(
					columns=config.movie_production_countries_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_production_countries"
				)
				csv["movie_release_dates"] = CSVFile(
					columns=config.movie_release_dates_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_release_dates"
				)
				csv["movie_roles"] = CSVFile(
					columns=config.movie_roles_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_roles"
				)
				csv["movie_spoken_languages"] = CSVFile(
					columns=config.movie_spoken_languages_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_spoken_languages"
				)
				csv["movie_translations"] = CSVFile(
					columns=config.movie_translations_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_translations"
				)
				csv["movie_videos"] = CSVFile(
					columns=config.movie_videos_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_videos"
				)

				movies_details_futures = get_tmdb_movie_details.map(config=config, movie_id=chunk)

				movies_details_results = []
				for movie_details_response in movies_details_futures:
					movie_details = movie_details_response.result()
					if movie_details is not None:
						movies_details_results.append(movie_details)

				# config.logger.info(f"Processing movies details...")
				csv["movie"].append(rows_data=Mapper.movies(config=config,movies=movies_details_results))
				# config.logger.info(f"Processing movies alternative titles...")
				csv["movie_alternative_titles"].append(rows_data=Mapper.movies_alternative_titles(config=config,movies=movies_details_results))
				# config.logger.info(f"Processing movies credits...")
				movies_credits_df, movies_roles_df = Mapper.movies_credits(config=config,movies=movies_details_results)
				csv["movie_credits"].append(rows_data=movies_credits_df)
				# config.logger.info(f"Processing movies roles...")
				csv["movie_roles"].append(rows_data=movies_roles_df)
				# config.logger.info(f"Processing movies external ids...")
				csv["movie_external_ids"].append(rows_data=Mapper.movies_external_ids(config=config,movies=movies_details_results))
				# config.logger.info(f"Processing movies genres...")
				csv["movie_genres"].append(rows_data=Mapper.movies_genres(config=config,movies=movies_details_results))
				# config.logger.info(f"Processing movies images...")
				csv["movie_images"].append(rows_data=Mapper.movies_images(config=config,movies=movies_details_results))
				# config.logger.info(f"Processing movies keywords...")
				csv["movie_keywords"].append(rows_data=Mapper.movies_keywords(config=config,movies=movies_details_results))
				# config.logger.info(f"Processing movies origin country...")
				csv["movie_origin_country"].append(rows_data=Mapper.movies_origin_country(config=config,movies=movies_details_results))
				# config.logger.info(f"Processing movies production companies...")
				csv["movie_production_companies"].append(rows_data=Mapper.movies_production_companies(config=config,movies=movies_details_results))
				# config.logger.info(f"Processing movies production countries...")
				csv["movie_production_countries"].append(rows_data=Mapper.movies_production_countries(config=config,movies=movies_details_results))
				# config.logger.info(f"Processing movies release dates...")
				csv["movie_release_dates"].append(rows_data=Mapper.movies_release_dates(config=config,movies=movies_details_results))
				# config.logger.info(f"Processing movies spoken languages...")
				csv["movie_spoken_languages"].append(rows_data=Mapper.movies_spoken_languages(config=config,movies=movies_details_results))
				# config.logger.info(f"Processing movies translations...")
				csv["movie_translations"].append(rows_data=Mapper.movies_translations(config=config,movies=movies_details_results))
				# config.logger.info(f"Processing movies videos...")
				csv["movie_videos"].append(rows_data=Mapper.movies_videos(config=config,movies=movies_details_results))

				config.logger.info(f"Pushing movies to the database...")
				push_future = config.push.submit(csv=csv)
				push_future.result(raise_on_failure=True)
				config.logger.info(f"Successfully pushed movies to the database")
			
			# Wait for all the submits to finish
			# wait(submits)
	except Exception as e:
		raise ValueError(f"Failed to process missing movies: {e}")
	
# ---------------------------------------------------------------------------- #

@flow(name="sync_tmdb_movie", log_prints=True)
def sync_tmdb_movie(date: date = date.today()):
	logger = get_run_logger()
	logger.info(f"Syncing movie for {date}...")
	try:
		config = MovieConfig(date=date)
		config.log_manager.init(type="tmdb_movie")

		# Get the list of movie from TMDB and the database
		config.log_manager.fetching_data()
		tmdb_movies_set = get_tmdb_movies(config)
		db_movies_set = get_db_movies(config)

		# Compare the movies and process missing movies
		config.extra_movies = db_movies_set - tmdb_movies_set
		config.missing_movies = tmdb_movies_set - db_movies_set
		get_tmdb_movies_changed(config)
		logger.info(f"Found {len(config.extra_movies)} extra movies and {len(config.missing_movies)} missing movies")
		config.log_manager.data_fetched()

		# Sync the movies to the database
		config.log_manager.syncing_to_db()
		config.prune()
		process_missing_movies(config=config)
		config.log_manager.success()
	except Exception as e:
		config.log_manager.failed()
		raise ValueError(f"Failed to sync movie: {e}")


