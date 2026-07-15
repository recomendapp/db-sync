from prefect import flow, task, unmapped
from prefect.futures import wait
from prefect.task_runners import ThreadPoolTaskRunner
from ..models.config import Config
from ..utils.sitemap import build_sitemap, build_sitemap_index, gzip_encode
from ..utils.priority import compute_priority
import math

PLAYLIST_PER_PAGE = 10000

def compute_playlist_score(likes_count: int, saved_count: int) -> float:
    """Score composite pour une playlist, basé uniquement sur l'engagement
    (likes/saves) — le volume d'items (items_count) est volontairement exclu
    car il ne reflète pas la qualité perçue de la playlist."""
    return (likes_count or 0) * 3 + (saved_count or 0) * 2

@task(name="cleanup_excess_playlist_sitemaps", log_prints=True)
def cleanup_excess_playlist_sitemaps(config: Config, prefix: str, current_count: int):
    config.storage_client.clean_excess_sitemaps(prefix, current_count)
    config.logger.info(f"Cleaned up {prefix} sitemaps from index {current_count} onwards.")

@task(cache_policy=None)
def get_sitemap_playlist_count(config: Config) -> int:
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(id) as count
                FROM playlist
                WHERE visibility = 'public' AND items_count > 0
            """)
            count = cursor.fetchone()[0]
            return math.ceil(count / PLAYLIST_PER_PAGE) if count else 0

@task(cache_policy=None)
def get_max_playlist_score(config: Config) -> float:
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COALESCE(
                    MAX(likes_count * 3 + saved_count * 2),
                    0
                )
                FROM playlist
                WHERE visibility = 'public' AND items_count > 0
            """)
            return cursor.fetchone()[0] or 0.0

@task(cache_policy=None)
def get_sitemap_playlists(config: Config, page: int) -> list:
    offset = page * PLAYLIST_PER_PAGE
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT id, updated_at, likes_count, saved_count
                FROM playlist
                WHERE visibility = 'public' AND items_count > 0
                ORDER BY id ASC
                LIMIT {PLAYLIST_PER_PAGE} OFFSET {offset}
            """)
            return cursor.fetchall()

@task(cache_policy=None)
def process_sitemap_page(page_index: int, max_score: float):
    config = Config()
    logger = config.logger
    playlists = get_sitemap_playlists(config, page_index)
    sitemap_entries = []

    for playlist_data in playlists:
        playlist_id, updated_at, likes_count, saved_count = playlist_data
        score = compute_playlist_score(likes_count, saved_count)
        sitemap_entries.append({
            "url": f"{config.site_url}/playlist/{playlist_id}",
            "lastModified": updated_at.isoformat() if updated_at else None,
            "priority": compute_priority(score, max_score),
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
    max_score = get_max_playlist_score(config)

    if count > 0:
        futures = process_sitemap_page.map(range(count), max_score=unmapped(max_score))
        wait(futures)

    cleanup_excess_playlist_sitemaps(config, "playlists/", count)

    sitemap_indexes = [f"{config.sitemap_base_url}/playlists/{i}.xml.gz" for i in range(count)]
    sitemap_index_xml = build_sitemap_index(sitemap_indexes)
    gzipped_index = gzip_encode(sitemap_index_xml)
    config.storage_client.upload("playlists/index.xml.gz", gzipped_index)
    logger.info("Uploaded new playlists/index.xml.gz")
    logger.info("Finished playlist sitemaps.")