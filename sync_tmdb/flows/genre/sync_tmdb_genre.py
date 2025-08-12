# ---------------------------------------------------------------------------- #
#                                    Imports                                   #
# ---------------------------------------------------------------------------- #

from datetime import date

# ---------------------------------- Prefect --------------------------------- #
from prefect import flow
from prefect.logging import get_run_logger

from ...utils.file_manager import create_csv, get_csv_header
from .config import GenreConfig
from .mappers import Mappers

# ---------------------------------------------------------------------------- #

# ---------------------------------------------------------------------------- #
#                                    Getters                                   #
# ---------------------------------------------------------------------------- #

def get_tmdb_genre(config: GenreConfig, params: dict) -> dict:
	try:
		tmdb_genres_movie_response = config.tmdb_client.request("genre/movie/list", params=params)
		tmdb_genres_tv_response = config.tmdb_client.request("genre/tv/list", params=params)

		tmdb_genres_movie: list = tmdb_genres_movie_response.get("genres", [])
		tmdb_genres_tv: list = tmdb_genres_tv_response.get("genres", [])

		# Merge the two lists deleting duplicates
		tmdb_genres = {genre["id"]: genre for genre in tmdb_genres_movie + tmdb_genres_tv}.values()
		return tmdb_genres
	except Exception as e:
		raise ValueError(f"Failed to get TMDB genre: {e}")


def get_tmdb_genres(config: GenreConfig) -> tuple:
	try:
		tmdb_genres = {}
		tmdb_genres[config.default_language.code] = get_tmdb_genre(config, {"language": config.default_language.tmdb_language})
		tmdb_genres_set: set = {genre["id"] for genre in tmdb_genres[config.default_language.code]}

		for language in config.extra_languages:
			tmdb_genres[language.code] = get_tmdb_genre(config, {"language": language.tmdb_language})

		return tmdb_genres, tmdb_genres_set

	except Exception as e:
		raise ValueError(f"Failed to get TMDB genres: {e}")

def get_db_genres(config: GenreConfig) -> set:
	try:
		with config.db_client.connection() as conn:
			with conn.cursor() as cursor:
				cursor.execute(f"SELECT id FROM {config.table_genre}")
				return {item[0] for item in cursor}
	except Exception as e:
		raise ValueError(f"Failed to get database genres: {e}")

# ---------------------------------------------------------------------------- #

def process_extra_genres(config: GenreConfig, extra_genres: set):
	try:
		if len(extra_genres) > 0:
			config.logger.warning(f"Found {len(extra_genres)} extra genres in the database")
			with config.db_client.connection() as conn:
				with conn.cursor() as cursor:
					conn.autocommit = False
					try:
						cursor.execute(f"DELETE FROM {config.table_genre} WHERE id IN %s", (tuple(extra_genres),))
						conn.commit()
					except Exception as e:
						conn.rollback()
						raise
	except Exception as e:
		raise ValueError(f"Failed to process extra genres: {e}")
	
def process_missing_genres(config: GenreConfig, tmdb_genres: list, missing_genres_set: set):
	try:
		if len(missing_genres_set) > 0:
			config.logger.warning(f"Found {len(missing_genres_set)} missing genres in the database")
		
		# Initialize the mappers
		mappers = Mappers(genres=tmdb_genres, default_language=config.default_language, extra_languages=config.extra_languages)

		# Generate the CSV files
		config.genre = create_csv(data=mappers.genre, tmp_directory=config.tmp_directory, prefix="genre")
		config.genre_translation = create_csv(data=mappers.genre_translation, tmp_directory=config.tmp_directory, prefix="genre_translation")

		# Load the CSV files into the database using copy
		with config.db_client.connection() as conn:
			with conn.cursor() as cursor:
				conn.autocommit = False
				try:
					cursor.execute(f"""
						CREATE TEMP TABLE temp_{config.table_genre} (LIKE {config.table_genre} INCLUDING ALL);
						CREATE TEMP TABLE temp_{config.table_genre_translation} (LIKE {config.table_genre_translation} INCLUDING ALL);
					""")

					with open(config.genre, "r") as f:
						cursor.copy_expert(f"COPY temp_{config.table_genre} ({','.join(get_csv_header(f))}) FROM STDIN WITH CSV HEADER", f)
					with open(config.genre_translation, "r") as f:
						cursor.copy_expert(f"COPY temp_{config.table_genre_translation} ({','.join(get_csv_header(f))}) FROM STDIN WITH CSV HEADER", f)

					cursor.execute(f"""
						INSERT INTO {config.table_genre} (id)
						SELECT id FROM temp_{config.table_genre}
						ON CONFLICT (id) DO NOTHING;
					""")

					cursor.execute(f"""
						INSERT INTO {config.table_genre_translation} (genre, language, name)
						SELECT genre, language, name FROM temp_{config.table_genre_translation}
						ON CONFLICT (genre, language) DO UPDATE
						SET name = EXCLUDED.name;
					""")
					
					conn.commit()
				except Exception as e:
					conn.rollback()
					raise
	except Exception as e:
		raise ValueError(f"Failed to process missing genres: {e}")
			

@flow(name="sync_tmdb_genre", log_prints=True)
def sync_tmdb_genre(date: date = date.today()):
	logger = get_run_logger()
	logger.info(f"Syncing genre for {date}...")
	config = GenreConfig(date=date)
	try:
		config.log_manager.init(type="tmdb_genre")

		# Get the list of genre from TMDB and the database
		config.log_manager.fetching_data()
		tmdb_genres, tmdb_genres_set = get_tmdb_genres(config)
		db_genres_set = get_db_genres(config)
		config.log_manager.data_fetched()

		# Compare the genres
		extra_genres: set = db_genres_set - tmdb_genres_set
		missing_genres: set = tmdb_genres_set - db_genres_set

		# Process extra and missing genres
		config.log_manager.syncing_to_db()
		process_extra_genres(config, extra_genres)
		process_missing_genres(config, tmdb_genres, missing_genres)

		config.log_manager.success()
	except Exception as e:
		config.log_manager.failed()
		raise ValueError(f"Failed to sync genre: {e}")

