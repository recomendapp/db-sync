# ---------------------------------------------------------------------------- #
#                                    Imports                                   #
# ---------------------------------------------------------------------------- #

from datetime import date
from more_itertools import chunked
import gc

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

def get_db_series(config: SerieConfig) -> set:
	conn = config.db_client.get_connection()
	try:
		with conn.cursor() as cursor:
			cursor.execute(f"SELECT id FROM {config.table_serie}")
			return {item[0] for item in cursor}
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

@task(cache_policy=None, log_prints=False)
def get_tmdb_serie_details(config: SerieConfig, serie_id: int) -> dict:
	try:
		main_video_languages = "en,fr,es,ja,de"
		# TMDB limit the number of languages to 5 
		serie = config.tmdb_client.request(f"tv/{serie_id}", {"append_to_response": "alternative_titles,content_ratings,external_ids,images,keywords,videos,aggregate_credits,translations", "include_video_language": main_video_languages})

		# Protect against adult content
		if serie["adult"]:
			return None
		
		# Get the each season details
		seasons = []
		for season in serie["seasons"]:
			try:
				season_details = config.tmdb_client.request(f"tv/{serie_id}/season/{season['season_number']}", {"append_to_response": "credits,translations"})
				seasons.append(season_details)
			except Exception as e:
				config.logger.error(f"Failed to get season details for {serie_id} season {season['season_number']}: {e}")

		serie["seasons"] = seasons
		return serie
	except Exception as e:
		config.logger.error(f"Failed to get serie details for {serie_id}: {e}")
		return None

# ---------------------------------------------------------------------------- #
	
def process_missing_series(config: SerieConfig):
	try:
		if len(config.missing_series) > 0:
			config.get_db_data()
			chunks = list(chunked(config.missing_series, 500))
			for chunk in chunks:
				csv: dict[str, CSVFile] = {}

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

				csv["serie_credits"] = CSVFile(
					columns=config.serie_credits_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_credits"
				)

				# Seasons
				csv["serie_season"] = CSVFile(
					columns=config.serie_season_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_season"
				)

				csv["serie_season_credits"] = CSVFile(
					columns=config.serie_season_credits_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_season_credits"
				)

				csv["serie_season_translations"] = CSVFile(
					columns=config.serie_season_translations_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_season_translations"
				)

				# Episodes
				csv["serie_episode"] = CSVFile(
					columns=config.serie_episode_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_episode"
				)

				csv["serie_episode_credits"] = CSVFile(
					columns=config.serie_episode_credits_columns,
					tmp_directory=config.tmp_directory,
					prefix=f"{config.flow_name}_episode_credits"
				)

				series_details_futures = get_tmdb_serie_details.map(config=config, serie_id=chunk)
				for serie_details_response in series_details_futures:
					serie_details = serie_details_response.result()
					if serie_details is not None:
						csv["serie"].append(rows_data=Mapper.serie(config=config,serie=serie_details))
						csv["serie_alternative_titles"].append(rows_data=Mapper.serie_alternative_titles(config=config,serie=serie_details))
						csv["serie_content_ratings"].append(rows_data=Mapper.serie_content_ratings(config=config,serie=serie_details))
						csv["serie_external_ids"].append(rows_data=Mapper.serie_external_ids(config=config,serie=serie_details))
						csv["serie_genres"].append(rows_data=Mapper.serie_genres(config=config,serie=serie_details))
						csv["serie_images"].append(rows_data=Mapper.serie_images(config=config,serie=serie_details))
						csv["serie_keywords"].append(rows_data=Mapper.serie_keywords(config=config,serie=serie_details))
						csv["serie_languages"].append(rows_data=Mapper.serie_languages(config=config,serie=serie_details))
						csv["serie_networks"].append(rows_data=Mapper.serie_networks(config=config,serie=serie_details))
						csv["serie_origin_country"].append(rows_data=Mapper.serie_origin_country(config=config,serie=serie_details))
						csv["serie_production_companies"].append(rows_data=Mapper.serie_production_companies(config=config,serie=serie_details))
						csv["serie_production_countries"].append(rows_data=Mapper.serie_production_countries(config=config,serie=serie_details))
						csv["serie_spoken_languages"].append(rows_data=Mapper.serie_spoken_languages(config=config,serie=serie_details))
						csv["serie_translations"].append(rows_data=Mapper.serie_translations(config=config,serie=serie_details))
						csv["serie_videos"].append(rows_data=Mapper.serie_videos(config=config,serie=serie_details))
						csv["serie_credits"].append(rows_data=Mapper.serie_credits(config=config,serie=serie_details))

						# Seasons
						csv["serie_season"].append(rows_data=Mapper.serie_season(config=config,serie=serie_details))
						csv["serie_season_credits"].append(rows_data=Mapper.serie_season_credits(config=config,serie=serie_details))
						csv["serie_season_translations"].append(rows_data=Mapper.serie_season_translations(config=config,serie=serie_details))

						# Episodes
						csv["serie_episode"].append(rows_data=Mapper.serie_episode(config=config,serie=serie_details))
						csv["serie_episode_credits"].append(rows_data=Mapper.serie_episode_credits(config=config,serie=serie_details))


				config.logger.info(f"Pushing series to the database...")
				push_future = config.push.submit(csv=csv)
				push_future.result(raise_on_failure=True)
				config.logger.info(f"Successfully pushed series to the database")
			
	except Exception as e:
		raise ValueError(f"Failed to process missing series: {e}")
	
# ---------------------------------------------------------------------------- #

@flow(name="sync_tmdb_serie", log_prints=True)
def sync_tmdb_serie(date: date = date.today(), update_popularity: bool = True):
	logger = get_run_logger()
	logger.info(f"Syncing serie for {date}...")
	config = SerieConfig(date=date)
	try:
		config.log_manager.init(type="tmdb_tv_serie")

		# Get the list of series from TMDB and the database
		config.log_manager.fetching_data()
		tmdb_series_df = config.tmdb_client.get_export_ids(type="tv_series", date=config.date)
		tmdb_series_set = set(tmdb_series_df["id"])
		if update_popularity and 'popularity' in tmdb_series_df.columns:
			tmdb_series_popularity = tmdb_series_df.set_index('id')['popularity'].to_dict()
		else:
			tmdb_series_popularity = {}
		db_series_set = get_db_series(config)

		del tmdb_series_df
		gc.collect()

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

		if update_popularity:
			config.log_manager.updating_popularity()
			config.update_popularity(
				tmdb_popularity_data=tmdb_series_popularity,
				table_name=config.table_serie,
				content_type=config.flow_name,
			)
		
		config.log_manager.success()
	except Exception as e:
		config.log_manager.failed()
		raise ValueError(f"Failed to sync series: {e}")


