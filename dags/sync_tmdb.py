from airflow.decorators import dag
from datetime import date, datetime
from scripts.tmdb.sync_tmdb import sync_tmdb as starting

start_date = datetime.now()
default_args = {
	'start_date': start_date,
}

@dag(schedule="@daily", default_args=default_args, catchup=False)
def sync_tmdb(start_time = date.today()):
	starting(start_time)

run = sync_tmdb()
