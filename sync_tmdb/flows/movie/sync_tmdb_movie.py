# ---------------------------------------------------------------------------- #
#                                    Imports                                   #
# ---------------------------------------------------------------------------- #

from datetime import date
import pandas as pd
from more_itertools import chunked
import time

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
		movie = config.tmdb_client.request(f"movie/{movie_id}", {"append_to_response": "alternative_titles,credits,external_ids,keywords,release_dates,translations,images,videos", "include_image_language": "null", "include_video_language": "null"})

		return movie
	except Exception as e:
		config.logger.error(f"Failed to get movie details for {movie_id}: {e}")
		return None

# ---------------------------------------------------------------------------- #
	
def process_missing_movies(config: MovieConfig):
	try:
		if len(config.missing_movies) > 0:
			config.get_db_data()
			chunks = list(chunked(config.missing_movies, 500))
			submits = []
			for chunk in chunks:
				csv: dict[str, CSVFile] = {}
				csv["movie"] = CSVFile(
					columns=config.movie_columns,
					tmp_directory=config.tmp_directory,
					prefix=config.flow_name
				)
				

				movies_details_futures = get_tmdb_movie_details.map(config=config, movie_id=chunk)

				for movie_details_response in movies_details_futures:
					movie_details = movie_details_response.result()
					if movie_details is not None:
						person_csv.append(rows_data=Mapper.person(person=movie_details))
						person_translation_csv.append(rows_data=Mapper.person_translation(person=movie_details))
						person_image_csv.append(rows_data=Mapper.person_image(person=movie_details))
						person_external_id_csv.append(rows_data=Mapper.person_external_id(person=movie_details))
						person_also_known_as_csv.append(rows_data=Mapper.person_also_known_as(person=movie_details))
				
				submits.append(config.push.submit(csv=csv))
			
			# Wait for all the submits to finish
			wait(submits)
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

