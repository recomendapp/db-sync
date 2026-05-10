from prefect import flow, task
from prefect.futures import wait
from prefect.task_runners import ThreadPoolTaskRunner
from ..models.config import Config
from ..utils.sitemap import build_sitemap, build_sitemap_index, gzip_encode
from ..utils.slugify import slugify
import math

PERSON_PER_PAGE = 10000

@task(name="cleanup_excess_person_sitemaps", log_prints=True)
def cleanup_excess_person_sitemaps(config: Config, prefix: str, current_count: int):
    """Supprime les fichiers XML obsolètes si le nombre de pages a diminué."""
    config.storage_client.clean_excess_sitemaps(prefix, current_count)
    config.logger.info(f"Cleaned up {prefix} sitemaps from index {current_count} onwards.")

@task(cache_policy=None)
def get_sitemap_person_count(config: Config) -> int:
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT COUNT(id) as count FROM tmdb."person"')
            count = cursor.fetchone()[0]
            return math.ceil(count / PERSON_PER_PAGE) if count else 0

@task(cache_policy=None)
def get_sitemap_persons(config: Config, page: int) -> list:
    offset = page * PERSON_PER_PAGE
    
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT 
                    id, 
                    name,
                    updated_at
                FROM tmdb."person"
                ORDER BY id ASC
                LIMIT {PERSON_PER_PAGE} OFFSET {offset}
            """)
            return cursor.fetchall()

@task(cache_policy=None)
def process_sitemap_page(page_index: int):
    config = Config()
    logger = config.logger
    persons = get_sitemap_persons(config, page_index)
    sitemap_entries = []
    
    for person_data in persons:
        person_id, name, updated_at = person_data
        
        slug_val = slugify(name) if name else ""
        slug = f"{person_id}-{slug_val}" if slug_val else str(person_id)

        sitemap_entries.append({
            "url": f"{config.site_url}/person/{slug}",
            "lastModified": updated_at.isoformat() if updated_at else None,
            "priority": 0.8,
        })

    sitemap_xml = build_sitemap(sitemap_entries)
    gzipped_sitemap = gzip_encode(sitemap_xml)
    
    config.storage_client.upload(f"persons/{page_index}.xml.gz", gzipped_sitemap)
    logger.info(f"  - Uploaded persons/{page_index}.xml.gz")

@flow(name="generate_person_sitemaps", log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=5))
def generate_person_sitemaps():
    config = Config()
    logger = config.logger
    logger.info("Generating person sitemaps (Zero-Downtime)...")

    count = get_sitemap_person_count(config)

    sitemap_indexes = [f"{config.sitemap_base_url}/persons/{i}.xml.gz" for i in range(count)]
    sitemap_index_xml = build_sitemap_index(sitemap_indexes)
    gzipped_index = gzip_encode(sitemap_index_xml)
    config.storage_client.upload("persons/index.xml.gz", gzipped_index)
    logger.info("Uploaded new persons/index.xml.gz")

    cleanup_excess_person_sitemaps(config, "persons/", count)

    if count > 0:
        futures = process_sitemap_page.map(range(count))
        wait(futures)

    logger.info("Finished person sitemaps.")