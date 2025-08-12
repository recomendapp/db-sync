from prefect import flow, get_run_logger
from prefect.deployments import run_deployment
from datetime import date

@flow(name="sync_tmdb_orchestrator", log_prints=True)
def sync_tmdb_orchestrator(current_date: date = date.today()):
    """
    This flow orchestrates the TMDB synchronization by triggering separate deployments
    for each step, ensuring memory isolation.
    """
    logger = get_run_logger()
    logger.info(f"Orchestrating TMDB synchronization for {current_date}...")

    # The list of deployments to run, IN ORDER.
    # The names correspond to the "name" field in your prefect.yaml
    base_deployment_names = [
        "sync_tmdb_language",
        "sync_tmdb_country",
        "sync_tmdb_genre",
        "sync_tmdb_keyword",
        "sync_tmdb_collection",
        "sync_tmdb_company",
        "sync_tmdb_network",
        "sync_tmdb_person",
        "sync_tmdb_movie",
        "sync_tmdb_serie",
    ]

    for base_name in base_deployment_names:
        full_deployment_name = f"{base_name}/{base_name}"
        
        logger.info(f"--- Triggering deployment: {full_deployment_name} ---")
        run_deployment(
            name=full_deployment_name,
            parameters={"date": current_date.isoformat()}
        )
        logger.info(f"--- Deployment {base_name} finished. ---")

    logger.info("TMDB synchronization orchestration finished successfully!")