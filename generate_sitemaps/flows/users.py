from prefect import flow, task
from prefect.futures import wait
from prefect.task_runners import ThreadPoolTaskRunner
from ..models.config import Config
from ..utils.sitemap import build_sitemap, build_sitemap_index, gzip_encode
import math
from datetime import datetime

USER_PER_PAGE = 10000

@task(cache_policy=None)
def get_sitemap_user_count(config: Config) -> int:
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(id) as count FROM profile WHERE private = false")
            count = cursor.fetchone()[0]
            return math.ceil(count / USER_PER_PAGE) if count else 0

@task(cache_policy=None)
def get_sitemap_users(config: Config, page: int) -> list:
    offset = page * USER_PER_PAGE
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT username, created_at FROM profile
                WHERE private = false
                ORDER BY created_at ASC
                LIMIT {USER_PER_PAGE} OFFSET {offset}
            """)
            return cursor.fetchall()

@task(cache_policy=None)
def process_sitemap_page(page_index: int):
    config = Config()
    logger = config.logger
    users = get_sitemap_users(config, page_index)
    sitemap_entries = []
    for user_data in users:
        username, created_at = user_data
        sitemap_entries.append({
            "url": f"{config.site_url}/@{username}",
            "lastModified": created_at.isoformat(),
            "priority": 0.6,
        })
    
    sitemap_xml = build_sitemap(sitemap_entries)
    gzipped_sitemap = gzip_encode(sitemap_xml)
    config.storage_client.upload(f"users/{page_index}.xml.gz", gzipped_sitemap)
    logger.info(f"  - Uploaded users/{page_index}.xml.gz")

@flow(name="generate_user_sitemaps", log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=5))
def generate_user_sitemaps():
    config = Config()
    logger = config.logger
    logger.info("Generating user sitemaps...")

    count = get_sitemap_user_count(config)
    sitemap_indexes = [f"{config.sitemap_base_url}/users/{i}" for i in range(count)]

    sitemap_index_xml = build_sitemap_index(sitemap_indexes)
    gzipped_index = gzip_encode(sitemap_index_xml)
    config.storage_client.upload("users/index.xml.gz", gzipped_index)
    logger.info("Uploaded users/index.xml.gz")

    if count > 0:
        futures = process_sitemap_page.map(range(count))
        wait(futures)

    logger.info("Finished user sitemaps.")
