from prefect import flow, task
from prefect.logging import get_run_logger

from pathlib import Path
import json

from ...models.db_client import DBClient
from ...models.typesense_client import TypesenseClient
from .mapper import Mapper

BATCH_SIZE = 50000


@task
def ensure_users_schema_exists():
    ts = TypesenseClient()

    schema_path = (
        Path(__file__)
        .resolve()
        .parents[2]
        / "schemas"
        / "users.json"
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
def fetch_user_cursor():
    db = DBClient()
    conn = db.get_connection()
    cursor = conn.cursor(name="users_cursor")
    cursor.itersize = BATCH_SIZE

    sql = """
        SELECT 
            id,
            username,
            full_name,
            followers_count
        FROM public.user
        ORDER BY created_at;
    """

    cursor.execute(sql)
    return conn, cursor


@task
def import_batch(cursor):
    rows = cursor.fetchmany(BATCH_SIZE)
    if not rows:
        return 0, 0

    docs = [Mapper.user(r) for r in rows]

    ts = TypesenseClient()
    result = ts.upsert_documents("users", docs)

    success = sum(1 for r in result if r.get("success", False))
    return success, len(docs)


@flow(name="sync_typesense_users", log_prints=True)
def sync_typesense_users():
    logger = get_run_logger()
    logger.info("Ensuring Typesense schema for users...")

    schema_status = ensure_users_schema_exists()
    logger.info(f"Schema status: {schema_status}")

    logger.info("Starting synchronization with Typesense for users...")

    conn, cursor = fetch_user_cursor()
    total = 0

    while True:
        success, size = import_batch(cursor)
        if size == 0:
            break

        total += success
        logger.info(f"Imported {success}/{size} — total: {total}")

    conn.close()
    logger.info(f"Successfully synchronized {total} users.")

    return total
