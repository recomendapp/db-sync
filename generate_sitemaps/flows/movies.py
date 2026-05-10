from prefect import flow, task
from prefect.futures import wait
from prefect.task_runners import ThreadPoolTaskRunner
from ..models.config import Config
from ..utils.sitemap import build_sitemap, build_sitemap_index, gzip_encode
from ..utils.slugify import slugify
from ..utils.locales import DEFAULT_LOCALE
import math

MOVIE_PER_PAGE = 10000

@task(name="cleanup_excess_movie_sitemaps", log_prints=True)
def cleanup_excess_movie_sitemaps(config: Config, prefix: str, current_count: int):
    config.storage_client.clean_excess_sitemaps(prefix, current_count)
    config.logger.info(f"Cleaned up {prefix} sitemaps from index {current_count} onwards.")

@task(cache_policy=None)
def get_sitemap_movie_count(config: Config) -> int:
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT COUNT(id) as count FROM tmdb."movie"')
            count = cursor.fetchone()[0]
            return math.ceil(count / MOVIE_PER_PAGE) if count else 0

@task(cache_policy=None)
def get_sitemap_movies(config: Config, page: int) -> list:
    offset = page * MOVIE_PER_PAGE
    
    lang, country = DEFAULT_LOCALE.split('-')
    
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT 
                    m.id, 
                    m.original_title, 
                    m.updated_at,
                    (
                        SELECT t.title
                        FROM tmdb."movie_translation" t
                        WHERE t.movie_id = m.id
                          AND t.iso_639_1 = '{lang}'
                          AND t.iso_3166_1 = '{country}'
                        LIMIT 1
                    ) as default_title
                FROM tmdb."movie" m
                ORDER BY m.id ASC
                LIMIT {MOVIE_PER_PAGE} OFFSET {offset}
            """)
            return cursor.fetchall()

@task(cache_policy=None)
def process_sitemap_page(page_index: int):
    config = Config()
    logger = config.logger
    movies = get_sitemap_movies(config, page_index)
    sitemap_entries = []
    
    for movie_data in movies:
        movie_id, original_title, updated_at, default_title = movie_data
        
        final_title = default_title if default_title else original_title
        
        slug_val = slugify(final_title) if final_title else ""
        slug = f"{movie_id}-{slug_val}" if slug_val else str(movie_id)

        sitemap_entries.append({
            "url": f"{config.site_url}/film/{slug}",
            "lastModified": updated_at.isoformat() if updated_at else None,
            "priority": 0.8,
        })

    sitemap_xml = build_sitemap(sitemap_entries)
    gzipped_sitemap = gzip_encode(sitemap_xml)
    config.storage_client.upload(f"movies/{page_index}.xml.gz", gzipped_sitemap)
    logger.info(f"  - Uploaded movies/{page_index}.xml.gz")

@flow(name="generate_movie_sitemaps", log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=5))
def generate_movie_sitemaps():
    config = Config()
    logger = config.logger
    logger.info("Generating movie sitemaps (Zero-Downtime)...")

    count = get_sitemap_movie_count(config)

    sitemap_indexes = [f"{config.sitemap_base_url}/movies/{i}.xml.gz" for i in range(count)]
    sitemap_index_xml = build_sitemap_index(sitemap_indexes)
    gzipped_index = gzip_encode(sitemap_index_xml)
    config.storage_client.upload("movies/index.xml.gz", gzipped_index)
    logger.info("Uploaded new movies/index.xml.gz")

    cleanup_excess_movie_sitemaps(config, "movies/", count)

    if count > 0:
        futures = process_sitemap_page.map(range(count))
        wait(futures)

    logger.info("Finished movie sitemaps.")