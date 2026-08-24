# ---------------------------------------------------------------------------- #
#                                    Imports                                   #
# ---------------------------------------------------------------------------- #

from prefect import flow
from prefect.blocks.system import Secret
from prefect.logging import get_run_logger

from .models.db_client import DBClient
from .models.typesense_client import TypesenseClient
from .api_client import post_event
from .providers import letterboxd

# ---------------------------------------------------------------------------- #

PROVIDERS = {
    "letterboxd": letterboxd.run,
}


@flow(name="run_import", log_prints=True, timeout_seconds=600)
def run_import(import_id: str, user_id: str, s3_key: str, provider: str):
    logger = get_run_logger()
    api_url = Secret.load("api-internal-url").get()
    internal_secret = Secret.load("api-internal-imports-secret").get()

    handler = PROVIDERS.get(provider)
    if handler is None:
        message = f"Unsupported provider: {provider}"
        logger.error(message)
        post_event(api_url, internal_secret, import_id, {"status": "failed", "error": message})
        raise ValueError(message)

    db = DBClient()
    ts = TypesenseClient()

    try:
        logger.info(f"Starting {provider} import {import_id} for user {user_id}")
        handler(db, ts, api_url, internal_secret, import_id, user_id, s3_key)
        logger.info(f"Import {import_id} staged successfully, awaiting user review")
    except Exception as e:
        logger.error(f"Import {import_id} failed: {e}")
        post_event(api_url, internal_secret, import_id, {"status": "failed", "error": str(e)})
        raise
