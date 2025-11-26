from prefect import flow, task
from prefect.logging import get_run_logger

from pathlib import Path
import json

from ...models.db_client import DBClient
from ...models.typesense_client import TypesenseClient
from .mapper import Mapper

BATCH_SIZE = 50000

@task
def ensure_movies_schema_exists():
    ts = TypesenseClient()

    schema_path = (
        Path(__file__)
        .resolve()
        .parents[2]
        / "schemas"
        / "movies.json"
    )

    with open(schema_path) as f:
        schema = json.load(f)

    try:
        ts.client.collections.create(schema)
        return "created"
    except Exception as e:
        if "already exists" in str(e).lower():
            return "exists"
        raise

@task
def fetch_movie_cursor():
    db = DBClient()
    conn = db.get_connection()
    cursor = conn.cursor(name="movies_cursor")
    cursor.itersize = BATCH_SIZE

    sql = """
        SELECT
            m.id,
            m.original_title,
            m.popularity::float,
            COALESCE(g.genre_ids, '{}') AS genre_ids,
            rt.runtime,
            rel.release_ts,
            COALESCE(titles.titles, '{}') AS titles
        FROM public.tmdb_movie m
        LEFT JOIN LATERAL (
            SELECT ARRAY_REMOVE(ARRAY_AGG(DISTINCT btrim(t.title)), NULL) AS titles
            FROM public.tmdb_movie_translations t
            WHERE t.movie_id = m.id 
              AND t.title IS NOT NULL 
              AND btrim(t.title) <> ''
        ) titles ON TRUE
        LEFT JOIN LATERAL (
            SELECT t.runtime
            FROM public.tmdb_movie_translations t
            WHERE t.movie_id = m.id
              AND t.runtime IS NOT NULL 
              AND t.runtime > 0
            ORDER BY (t.iso_639_1 = m.original_language) DESC, t.id
            LIMIT 1
        ) rt ON TRUE
        LEFT JOIN LATERAL (
            SELECT EXTRACT(EPOCH FROM r.release_date)::bigint AS release_ts
            FROM public.tmdb_movie_release_dates r
            WHERE r.movie_id = m.id 
              AND r.release_type IN (2,3)
            ORDER BY r.release_date ASC
            LIMIT 1
        ) rel ON TRUE
        LEFT JOIN LATERAL (
            SELECT ARRAY_AGG(DISTINCT mg.genre_id)::int[] AS genre_ids
            FROM public.tmdb_movie_genres mg
            WHERE mg.movie_id = m.id
        ) g ON TRUE
        ORDER BY m.id
    """

    cursor.execute(sql)
    return conn, cursor

@task
def import_batch(cursor):
    rows = cursor.fetchmany(BATCH_SIZE)
    if not rows:
        return 0, 0

    docs = [Mapper.movie(r) for r in rows]

    ts = TypesenseClient()
    result = ts.upsert_documents("movies", docs)

    success = sum(1 for r in result if r.get("success", False))
    return success, len(docs)

@flow(name="sync_typesense_movies", log_prints=True)
def sync_typesense_movies():
    logger = get_run_logger()
    logger.info("Ensuring Typesense schema for movies...")

    schema_status = ensure_movies_schema_exists()
    logger.info(f"Schema status: {schema_status}")

    logger.info("Starting synchronization with Typesense for movies...")

    conn, cursor = fetch_movie_cursor()
    total = 0

    while True:
        success, size = import_batch(cursor)
        if size == 0:
            break

        total += success
        logger.info(f"Imported {success}/{size} — total: {total}")

    conn.close()
    logger.info(f"Successfully synchronized {total} movies.")

    return total