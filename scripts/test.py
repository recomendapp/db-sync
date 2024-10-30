from datetime import date
from logger import logger
import os

def sync_tmdb(date: date = date.today()):
	logger.fatal(f"Starting synchronization with TMDb for {date}...")

	tmdb_api_key = os.environ.get("TMDB_API_KEY")
	if tmdb_api_key:
		logger.fatal(f"TMDB API key: {tmdb_api_key}")
	else:
		logger.fatal("TMDB API key not found")



# call sync_tmdb function
if __name__ == '__main__':
	sync_tmdb()