from prefect import flow, task
from prefect.logging import get_run_logger

from pathlib import Path
import json

from ...models.db_client import DBClient
from ...models.typesense_client import TypesenseClient
from .mapper import Mapper

BATCH_SIZE = 50000

@task
def ensure_tv_series_schema_exists():
    ts = TypesenseClient()

    schema_path = (
        Path(__file__)
        .resolve()
        .parents[2]
        / "schemas"
        / "tv_series.json"
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
def fetch_tv_series_cursor():
    db = DBClient()
    conn = db.get_connection()
    cursor = conn.cursor(name="tv_series_cursor")
    cursor.itersize = BATCH_SIZE

    sql = """
        SELECT
            s.id,
            s.original_name,
            s.popularity::float AS popularity,
            COALESCE(g.genre_ids, '{}') AS genre_ids,
            s.number_of_episodes,
            s.number_of_seasons,
            s.vote_average::float AS vote_average,
            s.vote_count::int AS vote_count,
            s.status,
            s.type,
            first.first_air_ts,
            last.last_air_ts,
            COALESCE(names.names, '{}') AS names
        FROM public.tmdb_tv_series s
        LEFT JOIN LATERAL (
            SELECT ARRAY_REMOVE(ARRAY_AGG(DISTINCT btrim(t.name)), NULL) AS names
            FROM public.tmdb_tv_series_translations t
            WHERE t.serie_id = s.id
              AND t.name IS NOT NULL
              AND btrim(t.name) <> ''
        ) AS names ON TRUE
        LEFT JOIN LATERAL (
            SELECT ARRAY_AGG(DISTINCT sg.genre_id)::int[] AS genre_ids
            FROM public.tmdb_tv_series_genres sg
            WHERE sg.serie_id = s.id
        ) AS g ON TRUE
        LEFT JOIN LATERAL (
            SELECT EXTRACT(EPOCH FROM s.first_air_date)::bigint AS first_air_ts
        ) AS first ON TRUE
        LEFT JOIN LATERAL (
            SELECT EXTRACT(EPOCH FROM s.last_air_date)::bigint AS last_air_ts
        ) AS last ON TRUE
        ORDER BY s.id;
    """

    cursor.execute(sql)
    return conn, cursor

@task
def import_batch(cursor):
    rows = cursor.fetchmany(BATCH_SIZE)
    if not rows:
        return 0, 0

    docs = [Mapper.tv_series(r) for r in rows]

    ts = TypesenseClient()
    result = ts.upsert_documents("tv_series", docs)

    success = sum(1 for r in result if r.get("success", False))
    return success, len(docs)

@flow(name="sync_typesense_tv_series", log_prints=True)
def sync_typesense_tv_series():
    logger = get_run_logger()
    logger.info("Ensuring Typesense schema for tv_series...")

    schema_status = ensure_tv_series_schema_exists()
    logger.info(f"Schema status: {schema_status}")

    logger.info("Starting synchronization with Typesense for tv_series...")

    conn, cursor = fetch_tv_series_cursor()
    total = 0

    while True:
        success, size = import_batch(cursor)
        if size == 0:
            break

        total += success
        logger.info(f"Imported {success}/{size} — total: {total}")

    conn.close()
    logger.info(f"Successfully synchronized {total} tv_series.")

    return total