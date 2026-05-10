from prefect import flow, task
from prefect.futures import wait
from prefect.task_runners import ThreadPoolTaskRunner
from ..models.config import Config
from ..utils.sitemap import build_sitemap, build_sitemap_index, gzip_encode
from ..utils.slugify import slugify
from ..utils.locales import DEFAULT_LOCALE
import math

TV_SERIES_PER_PAGE = 10000

@task(name="cleanup_excess_tv_series_sitemaps", log_prints=True)
def cleanup_excess_tv_series_sitemaps(config: Config, prefix: str, current_count: int):
    """Supprime les fichiers XML obsolètes si le nombre de pages a diminué."""
    config.storage_client.clean_excess_sitemaps(prefix, current_count)
    config.logger.info(f"Cleaned up {prefix} sitemaps from index {current_count} onwards.")

@task(cache_policy=None)
def get_sitemap_tv_series_count(config: Config) -> int:
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT COUNT(id) as count FROM tmdb."tv_series"')
            count = cursor.fetchone()[0]
            return math.ceil(count / TV_SERIES_PER_PAGE) if count else 0

@task(cache_policy=None)
def get_sitemap_tv_series(config: Config, page: int) -> list:
    offset = page * TV_SERIES_PER_PAGE
    
    lang, country = DEFAULT_LOCALE.split('-')
    
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT 
                    tv.id, 
                    tv.original_name,
                    tv.updated_at,
                    (
                        SELECT t.name
                        FROM tmdb."tv_series_translation" t
                        WHERE t.tv_series_id = tv.id
                          AND t.iso_639_1 = '{lang}'
                          AND t.iso_3166_1 = '{country}'
                        LIMIT 1
                    ) as default_name
                FROM tmdb."tv_series" tv
                ORDER BY tv.id ASC
                LIMIT {TV_SERIES_PER_PAGE} OFFSET {offset}
            """)
            return cursor.fetchall()

@task(cache_policy=None)
def process_sitemap_page(page_index: int):
    config = Config()
    logger = config.logger
    series = get_sitemap_tv_series(config, page_index)
    sitemap_entries = []
    
    for serie_data in series:
        serie_id, original_name, updated_at, default_name = serie_data
        
        final_name = default_name if default_name else original_name
        
        slug_val = slugify(final_name) if final_name else ""
        slug = f"{serie_id}-{slug_val}" if slug_val else str(serie_id)

        sitemap_entries.append({
            "url": f"{config.site_url}/tv-series/{slug}",
            "lastModified": updated_at.isoformat() if updated_at else None,
            "priority": 0.8,
        })

    sitemap_xml = build_sitemap(sitemap_entries)
    gzipped_sitemap = gzip_encode(sitemap_xml)
    
    config.storage_client.upload(f"tv-series/{page_index}.xml.gz", gzipped_sitemap)
    logger.info(f"  - Uploaded tv-series/{page_index}.xml.gz")

@flow(name="generate_tv_series_sitemaps", log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=5))
def generate_tv_series_sitemaps():
    config = Config()
    logger = config.logger
    logger.info("Generating TV series sitemaps (Zero-Downtime)...")

    count = get_sitemap_tv_series_count(config)

    sitemap_indexes = [f"{config.sitemap_base_url}/tv-series/{i}.xml.gz" for i in range(count)]
    sitemap_index_xml = build_sitemap_index(sitemap_indexes)
    gzipped_index = gzip_encode(sitemap_index_xml)
    config.storage_client.upload("tv-series/index.xml.gz", gzipped_index)
    logger.info("Uploaded new tv-series/index.xml.gz")

    cleanup_excess_tv_series_sitemaps(config, "tv-series/", count)

    if count > 0:
        futures = process_sitemap_page.map(range(count))
        wait(futures)

    logger.info("Finished TV series sitemaps.")