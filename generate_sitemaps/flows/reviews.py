from prefect import flow, task
from ..models.config import Config
from ..utils.sitemap import build_sitemap, build_sitemap_index, gzip_encode
import math

REVIEW_PER_PAGE = 10000

@task(cache_policy=None)
def get_sitemap_review_movie_count(config: Config) -> int:
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(id) as count FROM user_reviews_movie")
            count = cursor.fetchone()[0]
            return math.ceil(count / REVIEW_PER_PAGE) if count else 0

@task(cache_policy=None)
def get_sitemap_review_tv_series_count(config: Config) -> int:
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(id) as count FROM user_reviews_tv_series")
            count = cursor.fetchone()[0]
            return math.ceil(count / REVIEW_PER_PAGE) if count else 0

@task(cache_policy=None)
def get_sitemap_reviews_movie(config: Config, page: int) -> list:
    offset = page * REVIEW_PER_PAGE
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT
                    r.id,
                    r.updated_at,
                    (
                        SELECT json_build_object('movie_id', a.movie_id)
                        FROM user_activities_movie a
                        WHERE a.id = r.id
                    ) as activity
                FROM user_reviews_movie r
                ORDER BY r.created_at ASC
                LIMIT {REVIEW_PER_PAGE} OFFSET {offset}
            """)
            return cursor.fetchall()

@task(cache_policy=None)
def get_sitemap_reviews_tv_series(config: Config, page: int) -> list:
    offset = page * REVIEW_PER_PAGE
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT
                    r.id,
                    r.updated_at,
                    (
                        SELECT json_build_object('tv_series_id', a.tv_series_id)
                        FROM user_activities_tv_series a
                        WHERE a.id = r.id
                    ) as activity
                FROM user_reviews_tv_series r
                ORDER BY r.created_at ASC
                LIMIT {REVIEW_PER_PAGE} OFFSET {offset}
            """)
            return cursor.fetchall()

@flow(name="generate_review_sitemaps", log_prints=True)
def generate_review_sitemaps():
    config = Config()
    logger = config.logger
    logger.info("Generating review sitemaps...")

    movie_review_count = get_sitemap_review_movie_count(config)
    tv_review_count = get_sitemap_review_tv_series_count(config)

    movie_review_sitemaps = [f"{config.sitemap_base_url}/reviews/movie/{i}" for i in range(movie_review_count)]
    tv_review_sitemaps = [f"{config.sitemap_base_url}/reviews/tv-series/{i}" for i in range(tv_review_count)]

    review_index_xml = build_sitemap_index(movie_review_sitemaps + tv_review_sitemaps)
    gzipped_index = gzip_encode(review_index_xml)
    config.storage_client.upload("reviews/index.xml.gz", gzipped_index)
    logger.info("  - Uploaded reviews/index.xml.gz")

    # Movie Reviews
    for i in range(movie_review_count):
        reviews = get_sitemap_reviews_movie(config, i)
        sitemap_entries = []
        for review_data in reviews:
            review_id, updated_at, activity = review_data
            if activity and 'movie_id' in activity:
                sitemap_entries.append({
                    "url": f"{config.site_url}/film/{activity['movie_id']}/review/{review_id}",
                    "lastModified": updated_at.isoformat(),
                    "changeFrequency": "daily",
                    "priority": 0.8,
                })
        
        sitemap_xml = build_sitemap(sitemap_entries)
        gzipped_sitemap = gzip_encode(sitemap_xml)
        config.storage_client.upload(f"reviews/movie/{i}.xml.gz", gzipped_sitemap)
        logger.info(f"  - Uploaded reviews/movie/{i}.xml.gz")

    # TV Series Reviews
    for i in range(tv_review_count):
        reviews = get_sitemap_reviews_tv_series(config, i)
        sitemap_entries = []
        for review_data in reviews:
            review_id, updated_at, activity = review_data
            if activity and 'tv_series_id' in activity:
                sitemap_entries.append({
                    "url": f"{config.site_url}/tv-series/{activity['tv_series_id']}/review/{review_id}",
                    "lastModified": updated_at.isoformat(),
                    "changeFrequency": "daily",
                    "priority": 0.8,
                })
        
        sitemap_xml = build_sitemap(sitemap_entries)
        gzipped_sitemap = gzip_encode(sitemap_xml)
        config.storage_client.upload(f"reviews/tv-series/{i}.xml.gz", gzipped_sitemap)
        logger.info(f"  - Uploaded reviews/tv-series/{i}.xml.gz")

    logger.info("Finished review sitemaps.")
