from prefect import flow, get_run_logger
from .flows import (
    generate_movie_sitemaps,
    generate_tv_series_sitemaps,
    generate_user_sitemaps,
    generate_playlist_sitemaps,
    generate_review_sitemaps
)

@flow(name="generate_sitemaps", log_prints=True)
def generate_sitemaps(
    users: bool = True,
    playlists: bool = True,
    reviews: bool = True,
    tv: bool = True,
    movies: bool = True
):
    logger = get_run_logger()
    logger.info("Starting sitemap generation...")

    if users:
        generate_user_sitemaps()
    if playlists:
        generate_playlist_sitemaps()
    if reviews:
        generate_review_sitemaps()
    if tv:
        generate_tv_series_sitemaps()
    if movies:
        generate_movie_sitemaps()

    logger.info("All selected sitemaps generated.")
