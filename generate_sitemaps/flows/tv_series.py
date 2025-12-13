from prefect import flow, task
from prefect.futures import wait
from prefect.task_runners import ThreadPoolTaskRunner
from ..models.config import Config
from ..utils.sitemap import build_sitemap, build_sitemap_index, gzip_encode
from ..utils.slugify import slugify
from ..utils.locales import SITEMAP_LOCALES, DEFAULT_LOCALE
import math

TV_SERIES_PER_PAGE = 10000

@task(cache_policy=None)
def get_sitemap_media_tv_series_count(config: Config) -> int:
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(id) as count FROM tmdb_tv_series")
            count = cursor.fetchone()[0]
            return math.ceil(count / TV_SERIES_PER_PAGE) if count else 0

@task(cache_policy=None)
def get_sitemap_media_tv_series(config: Config, page: int) -> list:
    offset = page * TV_SERIES_PER_PAGE
    
    locales_for_query = [tuple(l.split('-')) for l in SITEMAP_LOCALES]
    where_locale_clause = " OR ".join([f"(t.iso_639_1 = '{lang}' AND t.iso_3166_1 = '{country}')" for lang, country in locales_for_query])

    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT
                    tv.id,
                    tv.original_name,
                    COALESCE(
                        (
                            SELECT json_agg(json_build_object('iso_639_1', t.iso_639_1, 'iso_3166_1', t.iso_3166_1, 'name', t.name))
                            FROM tmdb_tv_series_translations t
                            WHERE t.serie_id = tv.id AND ({where_locale_clause})
                        ),
                        '[]'::json
                    ) as tmdb_tv_series_translations
                FROM tmdb_tv_series tv
                ORDER BY tv.id ASC
                LIMIT {TV_SERIES_PER_PAGE} OFFSET {offset}
            """)
            return cursor.fetchall()

@task(cache_policy=None)
def process_sitemap_page(page_index: int):
    config = Config()
    logger = config.logger
    series = get_sitemap_media_tv_series(config, page_index)
    sitemap_entries = []
    for serie_data in series:
        serie_id, original_name, translations_json = serie_data
        
        translations = {f"{t['iso_639_1']}-{t['iso_3166_1']}": t['name'] for t in translations_json}

        default_name = translations.get(DEFAULT_LOCALE, original_name)
        slug_val = slugify(default_name)
        default_slug_url = f"{serie_id}{f'-{slug_val}' if slug_val else ''}"

        language_urls = {}
        for locale in SITEMAP_LOCALES:
            name = translations.get(locale, original_name)
            slug_val = slugify(name)
            slug = f"{serie_id}{f'-{slug_val}' if slug_val else ''}"
            url = f"{config.site_url}/tv-series/{slug}" if locale == DEFAULT_LOCALE else f"{config.site_url}/{locale}/tv-series/{slug}"
            language_urls[locale] = url

        sitemap_entries.append({
            "url": f"{config.site_url}/tv-series/{default_slug_url}",
            "priority": 0.8,
            "alternates": {
                "languages": language_urls,
            },
        })

    sitemap_xml = build_sitemap(sitemap_entries)
    gzipped_sitemap = gzip_encode(sitemap_xml)
    config.storage_client.upload(f"tv-series/{page_index}.xml.gz", gzipped_sitemap)
    logger.info(f"  - Uploaded tv-series/{page_index}.xml.gz")

@flow(name="generate_tv_series_sitemaps", log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=5))
def generate_tv_series_sitemaps():
    config = Config()
    logger = config.logger
    logger.info("Generating TV series sitemaps...")

    count = get_sitemap_media_tv_series_count(config)
    sitemap_indexes = [f"{config.sitemap_base_url}/tv-series/{i}" for i in range(count)]

    sitemap_index_xml = build_sitemap_index(sitemap_indexes)
    gzipped_index = gzip_encode(sitemap_index_xml)
    config.storage_client.upload("tv-series/index.xml.gz", gzipped_index)
    logger.info("Uploaded tv-series/index.xml.gz")

    if count > 0:
        futures = process_sitemap_page.map(range(count))
        wait(futures)

    logger.info("Finished TV series sitemaps.")
