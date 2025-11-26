from prefect import flow, task
from prefect.logging import get_run_logger

from pathlib import Path
import json

from ...models.db_client import DBClient
from ...models.typesense_client import TypesenseClient
from .mapper import Mapper

BATCH_SIZE = 50000

@task
def ensure_persons_schema_exists():
    ts = TypesenseClient()

    schema_path = (
        Path(__file__)
        .resolve()
        .parents[2]
        / "schemas"
        / "persons.json"
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
def fetch_person_cursor():
    db = DBClient()
    conn = db.get_connection()
    cursor = conn.cursor(name="persons_cursor")
    cursor.itersize = BATCH_SIZE

    sql = """
        SELECT
            p.id,
            p.name,
            p.popularity::float AS popularity,
            p.known_for_department,
            COALESCE(aka.names, '{}') AS also_known_as
        FROM public.tmdb_person p
        LEFT JOIN LATERAL (
            SELECT ARRAY_AGG(DISTINCT btrim(a.name)) AS names
            FROM public.tmdb_person_also_known_as a
            WHERE a.person = p.id
        ) AS aka ON TRUE
        ORDER BY p.id;
    """

    cursor.execute(sql)
    return conn, cursor

@task
def import_batch(cursor):
    rows = cursor.fetchmany(BATCH_SIZE)
    if not rows:
        return 0, 0

    docs = [Mapper.person(r) for r in rows]

    ts = TypesenseClient()
    result = ts.upsert_documents("persons", docs)

    success = sum(1 for r in result if r.get("success", False))
    return success, len(docs)

@flow(name="sync_typesense_persons", log_prints=True)
def sync_typesense_persons():
    logger = get_run_logger()
    logger.info("Ensuring Typesense schema for persons...")

    schema_status = ensure_persons_schema_exists()
    logger.info(f"Schema status: {schema_status}")

    logger.info("Starting synchronization with Typesense for persons...")

    conn, cursor = fetch_person_cursor()
    total = 0

    while True:
        success, size = import_batch(cursor)
        if size == 0:
            break

        total += success
        logger.info(f"Imported {success}/{size} — total: {total}")

    conn.close()
    logger.info(f"Successfully synchronized {total} persons.")
    return total