from prefect import flow, task
from prefect.logging import get_run_logger

from pathlib import Path
import json

from ...models.db_client import DBClient
from ...models.typesense_client import TypesenseClient
from .mapper import Mapper

BATCH_SIZE = 50000

@task
def ensure_playlists_schema_exists():
    ts = TypesenseClient()

    schema_path = (
        Path(__file__)
        .resolve()
        .parents[2]
        / "schemas"
        / "playlists.json"
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
def fetch_playlist_cursor():
    db = DBClient()
    conn = db.get_connection()
    cursor = conn.cursor(name="playlists_cursor")
    cursor.itersize = BATCH_SIZE

    sql = """
        SELECT
            p.id,
            p.title,
            p.description,
            p.likes_count,
            p.items_count,
            EXTRACT(EPOCH FROM p.created_at)::bigint AS created_ts,
            EXTRACT(EPOCH FROM p.updated_at)::bigint AS updated_ts,
            p.private,
            p.user_id,
            ARRAY_REMOVE(ARRAY_AGG(pg.user_id::text), NULL) AS guest_ids,
            p.type
        FROM public.playlists p
        LEFT JOIN public.playlist_guests pg
            ON p.id = pg.playlist_id
        GROUP BY p.id
        ORDER BY p.created_at;
    """

    cursor.execute(sql)
    return conn, cursor

@task
def import_batch(cursor):
    rows = cursor.fetchmany(BATCH_SIZE)
    if not rows:
        return 0, 0

    docs = [Mapper.playlist(r) for r in rows]

    ts = TypesenseClient()
    result = ts.upsert_documents("playlists", docs)

    success = sum(1 for r in result if r.get("success", False))
    return success, len(docs)

@flow(name="sync_typesense_playlists", log_prints=True)
def sync_typesense_playlists():
    logger = get_run_logger()
    logger.info("Ensuring Typesense schema for playlists...")

    schema_status = ensure_playlists_schema_exists()
    logger.info(f"Schema status: {schema_status}")

    logger.info("Starting synchronization with Typesense for playlists...")

    conn, cursor = fetch_playlist_cursor()
    total = 0

    while True:
        success, size = import_batch(cursor)
        if size == 0:
            break

        total += success
        logger.info(f"Imported {success}/{size} — total: {total}")

    conn.close()
    logger.info(f"Successfully synchronized {total} playlists.")

    return total