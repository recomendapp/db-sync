# ---------------------------------------------------------------------------- #
#                                    Imports                                   #
# ---------------------------------------------------------------------------- #

from datetime import date
import os
import shutil

# ---------------------------------- Prefect --------------------------------- #

from prefect import flow
from prefect.logging import get_run_logger

# ----------------------------------- Flows ---------------------------------- #
from . import flows
# ---------------------------------------------------------------------------- #

@flow(name="sync_tmdb", log_prints=True)
def sync_tmdb(
    current_date: date = date.today(),
    language: bool = True,
    country: bool = True,
    genre: bool = True,
    keyword: bool = True,
    collection: bool = True,
    company: bool = True,
    network: bool = True,
    person: bool = True,
    movie: bool = True,
    serie: bool = True,
):
    logger = get_run_logger()
    logger.info(f"Starting synchronization with TMDb for {current_date}...")

    try:
        if language:
            flows.sync_tmdb_language(date=current_date)
        if country:
            flows.sync_tmdb_country(date=current_date)
        if genre:
            flows.sync_tmdb_genre(date=current_date)
        if keyword:
            flows.sync_tmdb_keyword(date=current_date)
        if collection:
            flows.sync_tmdb_collection(date=current_date)
        if company:
            flows.sync_tmdb_company(date=current_date)
        if network:
            flows.sync_tmdb_network(date=current_date)
        if person:
            flows.sync_tmdb_person(date=current_date)
        if movie:
            flows.sync_tmdb_movie(date=current_date)
        if serie:
            flows.sync_tmdb_serie(date=current_date)

        logger.info(f"Successfully synchronized with TMDb for {current_date}.")

    except Exception as e:
        logger.error(f"Syncing with TMDb failed: {e}")
        if os.path.exists(".tmp"):
            shutil.rmtree(".tmp")
        raise