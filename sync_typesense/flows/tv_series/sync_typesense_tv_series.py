from prefect import flow, task
from prefect.logging import get_run_logger

from pathlib import Path
import json
import copy

from ...models.db_client import DBClient
from ...models.typesense_client import TypesenseClient
from .mapper import Mapper

BATCH_SIZE = 10000
COLLECTION_NAME = "tv_series"
SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / f"{COLLECTION_NAME}.json"

SQL_QUERY = """
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

@task
def manage_schema(ts_client: TypesenseClient):
    logger = get_run_logger()
    logger.info(f"Managing schema for '{COLLECTION_NAME}' collection...")

    with open(SCHEMA_PATH) as f:
        file_schema = json.load(f)

    try:
        remote_schema = ts_client.client.collections[COLLECTION_NAME].retrieve()
        logger.info(f"Found existing collection '{COLLECTION_NAME}'.")

        # Normalize schemas for comparison by sorting fields by name
        file_schema_sorted = copy.deepcopy(file_schema)
        if 'fields' in file_schema_sorted:
            file_schema_sorted['fields'] = sorted(file_schema_sorted['fields'], key=lambda x: x['name'])

        remote_schema_sorted = {
            'name': remote_schema.get('name'),
            'fields': sorted(remote_schema.get('fields', []), key=lambda x: x['name']),
            'default_sorting_field': remote_schema.get('default_sorting_field')
        }
        
        # Optional fields can be returned as None from remote, so we remove them for comparison if not in file
        for key in list(remote_schema_sorted.keys()):
            if key not in file_schema_sorted:
                 del remote_schema_sorted[key]


        if file_schema_sorted == remote_schema_sorted:
            logger.info("Schema is up to date.")
            return

        logger.info("Schema mismatch detected. Re-creating collection.")
        ts_client.client.collections[COLLECTION_NAME].delete()
        ts_client.client.collections.create(file_schema)
        logger.info(f"Collection '{COLLECTION_NAME}' re-created.")

    except Exception:  # Specifically looking for typesense.exceptions.ObjectNotFound
        logger.info(f"Collection '{COLLECTION_NAME}' not found. Creating it.")
        ts_client.client.collections.create(file_schema)
        logger.info(f"Collection '{COLLECTION_NAME}' created.")


@task
def sync_data(db_client: DBClient, ts_client: TypesenseClient):
    logger = get_run_logger()
    logger.info("Starting data synchronization from PostgreSQL to Typesense...")
    
    db_ids = set()
    total_docs = 0

    with db_client.connection() as conn:
        # Using a server-side cursor for memory efficiency
        with conn.cursor('tv_series_sync_cursor') as cursor: # Changed cursor name
            cursor.execute(SQL_QUERY)
            
            while True:
                rows = cursor.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                
                documents = [Mapper.tv_series(row) for row in rows] # Changed Mapper
                if not documents:
                    continue

                ts_client.upsert_documents(COLLECTION_NAME, documents)
                
                for doc in documents:
                    db_ids.add(doc['id'])

                total_docs += len(documents)
                logger.info(f"Upserted batch of {len(documents)} documents. Total upserted: {total_docs}")

    logger.info(f"Finished upserting {total_docs} documents from PostgreSQL.")
    return db_ids


@task
def get_typesense_ids(ts_client: TypesenseClient) -> set:
    logger = get_run_logger()
    logger.info("Fetching all document IDs from Typesense...")

    ts_ids = set()
    try:
        # The export endpoint is efficient for dumping all document IDs
        export_params = {'include_fields': 'id'}
        exported_lines = ts_client.client.collections[COLLECTION_NAME].documents.export(export_params).split('\n')
        
        for line in exported_lines:
            if line:
                ts_ids.add(json.loads(line)['id'])

    except Exception as e:
        logger.warning(f"Could not fetch IDs from Typesense: {e}. Skipping deletion step.")

    logger.info(f"Found {len(ts_ids)} documents in Typesense.")
    return ts_ids


@task
def delete_stale_documents(ts_client: TypesenseClient, db_ids: set, ts_ids: set):
    logger = get_run_logger()
    
    ids_to_delete = list(ts_ids - db_ids)

    if not ids_to_delete:
        logger.info("No stale documents to delete.")
        return

    logger.info(f"Found {len(ids_to_delete)} stale documents to delete.")

    # Delete in batches
    for i in range(0, len(ids_to_delete), BATCH_SIZE):
        batch = ids_to_delete[i:i + BATCH_SIZE]
        try:
            ts_client.delete_documents(COLLECTION_NAME, batch)
            logger.info(f"Deleted batch of {len(batch)} documents.")
        except Exception as e:
            logger.info(f"Failed to delete batch of {len(batch)} documents: {e}")

    logger.info("Finished deleting stale documents.")


@flow(name="sync_typesense_tv_series", log_prints=True) # Changed flow name
def sync_typesense_tv_series():
    logger = get_run_logger()
    logger.info("Starting synchronization with Typesense for TV series...") # Changed log message

    db_client = DBClient()
    ts_client = TypesenseClient()

    manage_schema(ts_client)
    
    db_ids = sync_data(db_client, ts_client)
    ts_ids = get_typesense_ids(ts_client)

    if ts_ids:
        delete_stale_documents(ts_client, db_ids, ts_ids)

    logger.info("Successfully synchronized TV series.") # Changed log message
