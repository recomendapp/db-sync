from prefect import flow, get_run_logger
from .flows import (
    generate_movie_sitemaps,
    generate_tv_series_sitemaps,
    generate_user_sitemaps,
    generate_playlist_sitemaps,
    generate_review_sitemaps
)

@flow(name="generate_sitemaps", log_prints=True)
def generate_sitemaps():
    logger = get_run_logger()
    logger.info("Starting sitemap generation...")
    
    generate_user_sitemaps()
    generate_playlist_sitemaps()
    generate_review_sitemaps()
    generate_tv_series_sitemaps()
    generate_movie_sitemaps()

    logger.info("All sitemaps generated successfully.")