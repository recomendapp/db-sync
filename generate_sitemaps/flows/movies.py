from prefect import flow, task
from ..models.config import Config
from ..utils.sitemap import build_sitemap, build_sitemap_index, gzip_encode
from ..utils.slugify import slugify
from ..utils.locales import SITEMAP_LOCALES, DEFAULT_LOCALE
import math

MOVIE_PER_PAGE = 10000

@task(cache_policy=None)
def get_sitemap_media_movie_count(config: Config) -> int:
    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(id) as count FROM tmdb_movie")
            count = cursor.fetchone()[0]
            return math.ceil(count / MOVIE_PER_PAGE) if count else 0

@task(cache_policy=None)
def get_sitemap_media_movies(config: Config, page: int) -> list:
    offset = page * MOVIE_PER_PAGE
    
    locales_for_query = [tuple(l.split('-')) for l in SITEMAP_LOCALES]
    where_locale_clause = " OR ".join([f"(t.iso_639_1 = '{lang}' AND t.iso_3166_1 = '{country}')" for lang, country in locales_for_query])

    with config.db_client.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT
                    m.id,
                    m.original_title,
                    COALESCE(
                        (
                            SELECT json_agg(json_build_object('iso_639_1', t.iso_639_1, 'iso_3166_1', t.iso_3166_1, 'title', t.title))
                            FROM tmdb_movie_translations t
                            WHERE t.movie_id = m.id AND ({where_locale_clause})
                        ),
                        '[]'::json
                    ) as tmdb_movie_translations
                FROM tmdb_movie m
                ORDER BY m.id ASC
                LIMIT {MOVIE_PER_PAGE} OFFSET {offset}
            """)
            return cursor.fetchall()

@flow(name="generate_movie_sitemaps", log_prints=True)
def generate_movie_sitemaps():
    config = Config()
    logger = config.logger
    logger.info("Generating movie sitemaps...")

    count = get_sitemap_media_movie_count(config)
    sitemap_indexes = [f"{config.sitemap_base_url}/films/{i}" for i in range(count)]

    sitemap_index_xml = build_sitemap_index(sitemap_indexes)
    gzipped_index = gzip_encode(sitemap_index_xml)
    config.storage_client.upload("movies/index.xml.gz", gzipped_index)
    logger.info("Uploaded movies/index.xml.gz")

    for i in range(count):
        movies = get_sitemap_media_movies(config, i)
        sitemap_entries = []
        for movie_data in movies:
            movie_id, original_title, translations_json = movie_data
            
            translations = {f"{t['iso_639_1']}-{t['iso_3166_1']}": t['title'] for t in translations_json}

            default_title = translations.get(DEFAULT_LOCALE, original_title)
            slug_val = slugify(default_title)
            default_slug_url = f"{movie_id}{f'-{slug_val}' if slug_val else ''}"

            language_urls = {}
            for locale in SITEMAP_LOCALES:
                title = translations.get(locale, original_title)
                slug_val = slugify(title)
                slug = f"{movie_id}{f'-{slug_val}' if slug_val else ''}"
                url = f"{config.site_url}/film/{slug}" if locale == DEFAULT_LOCALE else f"{config.site_url}/{locale}/film/{slug}"
                language_urls[locale] = url

            sitemap_entries.append({
                "url": f"{config.site_url}/film/{default_slug_url}",
                "priority": 0.8,
                "alternates": {
                    "languages": language_urls,
                },
            })

        sitemap_xml = build_sitemap(sitemap_entries)
        gzipped_sitemap = gzip_encode(sitemap_xml)
        config.storage_client.upload(f"movies/{i}.xml.gz", gzipped_sitemap)
        logger.info(f"  - Uploaded movies/{i}.xml.gz")

    logger.info("Finished movie sitemaps.")
