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

from .config import SerieConfig
from .mapper import Mapper
from ...models.csv_file import CSVFile

# ---------------------------------------------------------------------------- #

# ---------------------------------------------------------------------------- #
#                                    Getters                                   #
# ---------------------------------------------------------------------------- #

def get_tmdb_series(config: SerieConfig) -> set:
	try:
		tmdb_series = config.tmdb_client.get_export_ids(type="tv_series", date=config.date)
		tmdb_series_set = set([item["id"] for item in tmdb_series])
		return tmdb_series_set
	except Exception as e:
		raise ValueError(f"Failed to get TMDB series: {e}")

def get_db_series(config: SerieConfig) -> set:
	conn = config.db_client.get_connection()
	try:
		with conn.cursor() as cursor:
			cursor.execute(f"SELECT id FROM {config.table_serie}")
			db_series = cursor.fetchall()
			db_series_set = set([item[0] for item in db_series])
			return db_series_set
	except Exception as e:
		raise ValueError(f"Failed to get database series: {e}")
	finally:
		config.db_client.return_connection(conn)
	
def get_tmdb_series_changed(config: SerieConfig):
	try:
		config.logger.info("Getting changed series...")
		changed_series = config.tmdb_client.get_changed_ids(type="tv", start_date=config.log_manager.last_success_log.date, end_date=config.date)
		config.missing_series |= changed_series
	except Exception as e:
		raise ValueError(f"Failed to get changed series: {e}")

@task
def get_tmdb_movie_details(config: SerieConfig, movie_id: int) -> dict:
	try:
		main_video_languages = "en,fr,es,ja,de"
		# TMDB limit the number of languages to 5 
		movie = config.tmdb_client.request(f"movie/{movie_id}", {"append_to_response": "alternative_titles,credits,external_ids,keywords,release_dates,translations,videos", "include_video_language": main_video_languages})

		# Protect against adult content
		if movie["adult"]:
			return None
		images = config.tmdb_client.request(f"movie/{movie_id}/images")
		# Add images to the movie details
		movie["images"] = images
		return movie
	except Exception as e:
		config.logger.error(f"Failed to get movie details for {movie_id}: {e}")
		return None

# ---------------------------------------------------------------------------- #
	
def process_missing_series(config: SerieConfig):
	try:
		if len(config.missing_series) > 0:
			config.get_db_data()
			chunks = list(chunked(config.missing_series, 500))
			for chunk in chunks:
				csv: dict[str, CSVFile] = {}
				# csv["movie"] = CSVFile(
				# 	columns=config.movie_columns,
				# 	tmp_directory=config.tmp_directory,
				# 	prefix=config.flow_name
				# )
				# csv["movie_alternative_titles"] = CSVFile(
				# 	columns=config.movie_alternative_titles_columns,
				# 	tmp_directory=config.tmp_directory,
				# 	prefix=f"{config.flow_name}_alternative_titles"
				# )
				# csv["movie_credits"] = CSVFile(
				# 	columns=config.movie_credits_columns,
				# 	tmp_directory=config.tmp_directory,
				# 	prefix=f"{config.flow_name}_credits"
				# )
				# csv["movie_external_ids"] = CSVFile(
				# 	columns=config.movie_external_ids_columns,
				# 	tmp_directory=config.tmp_directory,
				# 	prefix=f"{config.flow_name}_external_ids"
				# )
				# csv["movie_genres"] = CSVFile(
				# 	columns=config.movie_genres_columns,
				# 	tmp_directory=config.tmp_directory,
				# 	prefix=f"{config.flow_name}_genres"
				# )
				# csv["movie_images"] = CSVFile(
				# 	columns=config.movie_images_columns,
				# 	tmp_directory=config.tmp_directory,
				# 	prefix=f"{config.flow_name}_images"
				# )
				# csv["movie_keywords"] = CSVFile(
				# 	columns=config.movie_keywords_columns,
				# 	tmp_directory=config.tmp_directory,
				# 	prefix=f"{config.flow_name}_keywords"
				# )
				# csv["movie_origin_country"] = CSVFile(
				# 	columns=config.movie_origin_country_columns,
				# 	tmp_directory=config.tmp_directory,
				# 	prefix=f"{config.flow_name}_origin_country"
				# )
				# csv["movie_production_companies"] = CSVFile(
				# 	columns=config.movie_production_companies_columns,
				# 	tmp_directory=config.tmp_directory,
				# 	prefix=f"{config.flow_name}_production_companies"
				# )
				# csv["movie_production_countries"] = CSVFile(
				# 	columns=config.movie_production_countries_columns,
				# 	tmp_directory=config.tmp_directory,
				# 	prefix=f"{config.flow_name}_production_countries"
				# )
				# csv["movie_release_dates"] = CSVFile(
				# 	columns=config.movie_release_dates_columns,
				# 	tmp_directory=config.tmp_directory,
				# 	prefix=f"{config.flow_name}_release_dates"
				# )
				# csv["movie_roles"] = CSVFile(
				# 	columns=config.movie_roles_columns,
				# 	tmp_directory=config.tmp_directory,
				# 	prefix=f"{config.flow_name}_roles"
				# )
				# csv["movie_spoken_languages"] = CSVFile(
				# 	columns=config.movie_spoken_languages_columns,
				# 	tmp_directory=config.tmp_directory,
				# 	prefix=f"{config.flow_name}_spoken_languages"
				# )
				# csv["movie_translations"] = CSVFile(
				# 	columns=config.movie_translations_columns,
				# 	tmp_directory=config.tmp_directory,
				# 	prefix=f"{config.flow_name}_translations"
				# )
				# csv["movie_videos"] = CSVFile(
				# 	columns=config.movie_videos_columns,
				# 	tmp_directory=config.tmp_directory,
				# 	prefix=f"{config.flow_name}_videos"
				# )

				csv["serie"] = CSVFile(
					columns=config.serie_columns,
					tmp_directory=config.tmp_directory,
					prefix=config.flow_name
				)

				csv["serie_alternative_titles"] = CSVFile(
					columns=config.serie_alternative_titles_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_alternative_titles"
				)

				csv["serie_content_ratings"] = CSVFile(
					columns=config.serie_content_ratings_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_content_ratings"
				)

				csv["serie_external_ids"] = CSVFile(
					columns=config.serie_external_ids_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_external_ids"
				)

				csv["serie_genres"] = CSVFile(
					columns=config.serie_genres_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_genres"
				)

				csv["serie_images"] = CSVFile(
					columns=config.serie_images_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_images"
				)

				csv["serie_keywords"] = CSVFile(
					columns=config.serie_keywords_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_keywords"
				)

				csv["serie_languages"] = CSVFile(
					columns=config.serie_languages_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_languages"
				)

				csv["serie_networks"] = CSVFile(
					columns=config.serie_networks_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_networks"
				)

				csv["serie_origin_country"] = CSVFile(
					columns=config.serie_origin_country_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_origin_country"
				)

				csv["serie_production_companies"] = CSVFile(
					columns=config.serie_production_companies_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_production_companies"
				)

				csv["serie_production_countries"] = CSVFile(
					columns=config.serie_production_countries_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_production_countries"
				)

				csv["serie_spoken_languages"] = CSVFile(
					columns=config.serie_spoken_languages_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_spoken_languages"
				)

				csv["serie_translations"] = CSVFile(
					columns=config.serie_translations_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_translations"
				)

				csv["serie_videos"] = CSVFile(
					columns=config.serie_videos_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_videos"
				)

				series_details_futures = get_tmdb_movie_details.map(config=config, movie_id=chunk)
				for serie_details_response in series_details_futures:
					serie_details = serie_details_response.result()
					if serie_details is not None:
						print(serie_details)
						# csv["movie"].append(rows_data=Mapper.movie(config=config,movie=movie_details))
						# csv["movie_alternative_titles"].append(rows_data=Mapper.movie_alternative_titles(config=config,movie=movie_details))
						# movie_credits_df, movie_roles_df = Mapper.movie_credits(config=config,movie=movie_details)
						# csv["movie_credits"].append(rows_data=movie_credits_df)
						# csv["movie_roles"].append(rows_data=movie_roles_df)
						# csv["movie_external_ids"].append(rows_data=Mapper.movie_external_ids(config=config,movie=movie_details))
						# csv["movie_genres"].append(rows_data=Mapper.movie_genres(config=config,movie=movie_details))
						# csv["movie_images"].append(rows_data=Mapper.movie_images(config=config,movie=movie_details))
						# csv["movie_keywords"].append(rows_data=Mapper.movie_keywords(config=config,movie=movie_details))
						# csv["movie_origin_country"].append(rows_data=Mapper.movie_origin_country(config=config,movie=movie_details))
						# csv["movie_production_companies"].append(rows_data=Mapper.movie_production_companies(config=config,movie=movie_details))
						# csv["movie_production_countries"].append(rows_data=Mapper.movie_production_countries(config=config,movie=movie_details))
						# csv["movie_release_dates"].append(rows_data=Mapper.movie_release_dates(config=config,movie=movie_details))
						# csv["movie_spoken_languages"].append(rows_data=Mapper.movie_spoken_languages(config=config,movie=movie_details))
						# csv["movie_translations"].append(rows_data=Mapper.movie_translations(config=config,movie=movie_details))
						# csv["movie_videos"].append(rows_data=Mapper.movie_videos(config=config,movie=movie_details))

				config.logger.info(f"Pushing series to the database...")
				push_future = config.push.submit(csv=csv)
				push_future.result(raise_on_failure=True)
				config.logger.info(f"Successfully pushed series to the database")
			
			# Wait for all the submits to finish
			# wait(submits)
	except Exception as e:
		raise ValueError(f"Failed to process missing series: {e}")
	
# ---------------------------------------------------------------------------- #

@flow(name="sync_tmdb_serie", log_prints=True)
def sync_tmdb_serie(date: date = date.today()):
	logger = get_run_logger()
	logger.info(f"Syncing serie for {date}...")
	try:
		config = SerieConfig(date=date)
		config.log_manager.init(type="tmdb_serie")

		# Get the list of series from TMDB and the database
		config.log_manager.fetching_data()
		tmdb_series_set = get_tmdb_series(config)
		db_series_set = get_db_series(config)

		# Compare the series and process missing serries
		config.extra_series = db_series_set - tmdb_series_set
		config.missing_series = tmdb_series_set - db_series_set
		get_tmdb_series_changed(config)
		logger.info(f"Found {len(config.extra_series)} extra series and {len(config.missing_series)} missing series")
		config.log_manager.data_fetched()

		# Sync the series to the database
		config.log_manager.syncing_to_db()
		config.prune()
		process_missing_series(config=config)
		config.log_manager.success()
	except Exception as e:
		config.log_manager.failed()
		raise ValueError(f"Failed to sync series: {e}")


