from prefect import flow, task
from prefect.futures import wait
from prefect.task_runners import ThreadPoolTaskRunner
from ..models.config import Config
from ..utils.sitemap import build_sitemap, build_sitemap_index, gzip_encode
import math

PLAYLIST_PER_PAGE = 10000

@task(name="cleanup_excess_playlist_sitemaps", log_prints=True)
def cleanup_excess_playlist_sitemaps(config: Config, prefix: str, current_count: int):
    config.storage_client.clean_excess_sitemaps(prefix, current_count)
    config.logger.info(f"Cleaned up {prefix} sitemaps from index {current_count} onwards.")

@task(cache_policy=None)
def get_sitemap_playlist_count(config: Config) -> int:
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(id) as count FROM playlist WHERE visibility = 'public'")
            count = cursor.fetchone()[0]
            return math.ceil(count / PLAYLIST_PER_PAGE) if count else 0

@task(cache_policy=None)
def get_sitemap_playlists(config: Config, page: int) -> list:
    offset = page * PLAYLIST_PER_PAGE
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT id, updated_at FROM playlist
                WHERE visibility = 'public'
                ORDER BY id ASC
                LIMIT {PLAYLIST_PER_PAGE} OFFSET {offset}
            """)
            return cursor.fetchall()

@task(cache_policy=None)
def process_sitemap_page(page_index: int):
    config = Config()
    logger = config.logger
    playlists = get_sitemap_playlists(config, page_index)
    sitemap_entries = []
    
    for playlist_data in playlists:
        playlist_id, updated_at = playlist_data
        sitemap_entries.append({
            "url": f"{config.site_url}/playlist/{playlist_id}",
            "lastModified": updated_at.isoformat() if updated_at else None,
            "priority": 0.7,
        })
    
    sitemap_xml = build_sitemap(sitemap_entries)
    gzipped_sitemap = gzip_encode(sitemap_xml)
    config.storage_client.upload(f"playlists/{page_index}.xml.gz", gzipped_sitemap)
    logger.info(f"  - Uploaded playlists/{page_index}.xml.gz")

@flow(name="generate_playlist_sitemaps", log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=5))
def generate_playlist_sitemaps():
    config = Config()
    logger = config.logger
    logger.info("Generating playlist sitemaps (Zero-Downtime)...")

    count = get_sitemap_playlist_count(config)

    sitemap_indexes = [f"{config.sitemap_base_url}/playlists/{i}.xml.gz" for i in range(count)]
    sitemap_index_xml = build_sitemap_index(sitemap_indexes)
    gzipped_index = gzip_encode(sitemap_index_xml)
    config.storage_client.upload("playlists/index.xml.gz", gzipped_index)
    logger.info("Uploaded new playlists/index.xml.gz")

    cleanup_excess_playlist_sitemaps(config, "playlists/", count)

    if count > 0:
        futures = process_sitemap_page.map(range(count))
        wait(futures)

    logger.info("Finished playlist sitemaps.")