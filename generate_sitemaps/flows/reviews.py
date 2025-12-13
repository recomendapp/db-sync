from prefect import flow, task
from prefect.futures import wait
from prefect.task_runners import ThreadPoolTaskRunner
from ..models.config import Config
from ..utils.sitemap import build_sitemap, build_sitemap_index, gzip_encode
from ..utils.slugify import slugify
from ..utils.locales import DEFAULT_LOCALE
import math
import json

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
    iso_639_1, iso_3166_1 = DEFAULT_LOCALE.split("-")
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT
                    r.id,
                    r.updated_at,
                    a.movie_id,
                    COALESCE(
                        (
                            SELECT NULLIF(t.title, '')
                            FROM tmdb_movie_translations t
                            WHERE t.movie_id = a.movie_id
                                AND t.iso_639_1 = '{iso_639_1}'
                                AND t.iso_3166_1 = '{iso_3166_1}'
                        ),
                        m.original_title
                    ) as movie_title
                FROM user_reviews_movie r
                JOIN user_activities_movie a ON r.id = a.id
                LEFT JOIN tmdb_movie m ON m.id = a.movie_id
                ORDER BY r.created_at ASC
                LIMIT {REVIEW_PER_PAGE} OFFSET {offset}
            """)
            return cursor.fetchall()

@task(cache_policy=None)
def get_sitemap_reviews_tv_series(config: Config, page: int) -> list:
    offset = page * REVIEW_PER_PAGE
    iso_639_1, iso_3166_1 = DEFAULT_LOCALE.split("-")
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT
                    r.id,
                    r.updated_at,
                    a.tv_series_id,
                    COALESCE(
                        (
                            SELECT NULLIF(t.name, '')
                            FROM tmdb_tv_series_translations t
                            WHERE t.serie_id = a.tv_series_id
                                AND t.iso_639_1 = '{iso_639_1}'
                                AND t.iso_3166_1 = '{iso_3166_1}'
                        ),
                        tv.original_name
                    ) as tv_series_name
                FROM user_reviews_tv_series r
                JOIN user_activities_tv_series a ON r.id = a.id
                LEFT JOIN tmdb_tv_series tv ON tv.id = a.tv_series_id
                ORDER BY r.created_at ASC
                LIMIT {REVIEW_PER_PAGE} OFFSET {offset}
            """)
            return cursor.fetchall()

@task(cache_policy=None)
def process_movie_review_page(page_index: int):
    config = Config()
    logger = config.logger
    reviews = get_sitemap_reviews_movie(config, page_index)
    sitemap_entries = []
    for review_data in reviews:
        review_id, updated_at, movie_id, movie_title = review_data
        if movie_id:
            slug_val = slugify(movie_title)
            slug_url = f"{movie_id}{f'-{slug_val}' if slug_val else ''}"
            sitemap_entries.append({
                "url": f"{config.site_url}/film/{slug_url}/review/{review_id}",
                "lastModified": updated_at.isoformat(),
                "changeFrequency": "daily",
                "priority": 0.8,
            })
    
    sitemap_xml = build_sitemap(sitemap_entries)
    gzipped_sitemap = gzip_encode(sitemap_xml)
    config.storage_client.upload(f"reviews/movie/{page_index}.xml.gz", gzipped_sitemap)
    logger.info(f"  - Uploaded reviews/movie/{page_index}.xml.gz")

@task(cache_policy=None)
def process_tv_series_review_page(page_index: int):
    config = Config()
    logger = config.logger
    reviews = get_sitemap_reviews_tv_series(config, page_index)
    sitemap_entries = []
    for review_data in reviews:
        review_id, updated_at, tv_series_id, tv_series_name = review_data
        if tv_series_id:
            slug_val = slugify(tv_series_name)
            slug_url = f"{tv_series_id}{f'-{slug_val}' if slug_val else ''}"
            sitemap_entries.append({
                "url": f"{config.site_url}/tv-series/{slug_url}/review/{review_id}",
                "lastModified": updated_at.isoformat(),
                "changeFrequency": "daily",
                "priority": 0.8,
            })
    
    sitemap_xml = build_sitemap(sitemap_entries)
    gzipped_sitemap = gzip_encode(sitemap_xml)
    config.storage_client.upload(f"reviews/tv-series/{page_index}.xml.gz", gzipped_sitemap)
    logger.info(f"  - Uploaded reviews/tv-series/{page_index}.xml.gz")

@flow(name="generate_review_sitemaps", log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=5))
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

    futures = []
    if movie_review_count > 0:
        futures.extend(process_movie_review_page.map(range(movie_review_count)))

    if tv_review_count > 0:
        futures.extend(process_tv_series_review_page.map(range(tv_review_count)))

    if futures:
        wait(futures)

    logger.info("Finished review sitemaps.")
