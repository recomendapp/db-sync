from prefect import flow, task, get_run_logger
from ..models.config import Config
from ..utils.sitemap import build_sitemap_index, gzip_encode

CATEGORIES = ["movies", "tv-series", "persons", "users", "playlists"]

@task(cache_policy=None)
def collect_category_sitemaps(config: Config, category: str) -> list[str]:
    keys = config.storage_client.list_files(prefix=f"{category}/")
    return [f"{config.sitemap_base_url}/{key}" for key in keys]

@task(cache_policy=None)
def build_and_upload_master_index(config: Config, all_sitemaps: list[str]):
    logger = config.logger

    full_list = [f"{config.site_url}/sitemaps/statics/index.xml"] + all_sitemaps

    sitemap_index_xml = build_sitemap_index(full_list)
    gzipped_index = gzip_encode(sitemap_index_xml)
    config.storage_client.upload("index.xml.gz", gzipped_index)
    logger.info(f"Uploaded new index.xml.gz with {len(full_list)} entries.")

@flow(name="generate_sitemap_index", log_prints=True)
def generate_sitemap_index():
    config = Config()
    logger = get_run_logger()
    logger.info("Generating master sitemap index...")

    all_sitemaps: list[str] = []
    for category in CATEGORIES:
        category_sitemaps = collect_category_sitemaps(config, category)
        count = len(category_sitemaps)
        logger.info(f"  - Found {count} sitemap file(s) for '{category}'.")
        all_sitemaps.extend(category_sitemaps)

    build_and_upload_master_index(config, all_sitemaps)
    logger.info("Finished master sitemap index.")