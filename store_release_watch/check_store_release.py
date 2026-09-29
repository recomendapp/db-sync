# ---------------------------------------------------------------------------- #
#                                    Imports                                   #
# ---------------------------------------------------------------------------- #

from datetime import datetime, timedelta, timezone

from prefect import flow
from prefect.blocks.system import Secret
from prefect.deployments import run_deployment
from prefect.logging import get_run_logger

from .api_client import confirm_release_live, get_release_state
from .app_store_connect import is_ios_version_live
from .google_play import is_android_version_live

# ---------------------------------------------------------------------------- #

# Deliberately short: each run does at most one store check then exits, so a
# slow App Store review never ties up a cluster pod -- between checks nothing
# runs at all. See prefect.yaml for this deployment's (tiny) resource request.
CHECK_INTERVAL = timedelta(minutes=30)
# Past this, give up loudly (a Failed flow run, visible in the Prefect UI)
# rather than silently rescheduling forever. Well above the longest realistic
# App Store review; an operator should look at what's stuck manually.
MAX_WATCH_DURATION = timedelta(days=4)

CHECKERS = {
    "ios": is_ios_version_live,
    "android": is_android_version_live,
}


@flow(name="check_store_release", log_prints=True, timeout_seconds=120)
def check_store_release(platform: str, version: str):
    logger = get_run_logger()
    api_url = Secret.load("api-internal-url").get()
    internal_secret = Secret.load("api-internal-version-policy-secret").get()

    release = get_release_state(api_url, internal_secret, platform, version)
    if release is None:
        logger.warning(f"{platform}@{version} was never reported to the API, nothing to watch")
        return
    if release["state"] == "live":
        logger.info(f"{platform}@{version} is already confirmed live, nothing to do")
        return
    if release["state"] == "superseded":
        logger.info(f"{platform}@{version} was superseded by a later live release, stopping")
        return

    created_at = datetime.fromisoformat(release["createdAt"])
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    if datetime.now(timezone.utc) - created_at > MAX_WATCH_DURATION:
        raise TimeoutError(
            f"{platform}@{version} has been pending for over {MAX_WATCH_DURATION.days} days "
            "without going live -- giving up, check the store submission manually"
        )

    checker = CHECKERS.get(platform)
    if checker is None:
        logger.info(f"No store checker for platform '{platform}' (web?), nothing to do")
        return

    try:
        is_live = checker(version)
    except Exception as e:
        # A transient store-API hiccup must not kill the whole watch chain (nothing
        # would ever check again). Log it and retry on the next tick like any other
        # "still pending" outcome -- MAX_WATCH_DURATION above is the real backstop if
        # this keeps failing for days.
        logger.error(f"Store check failed for {platform}@{version}, will retry: {e}")
        is_live = False

    if is_live:
        logger.info(f"{platform}@{version} is now live on the store, confirming")
        confirm_release_live(api_url, internal_secret, platform, version)
        return

    logger.info(f"{platform}@{version} is still pending, checking again in {CHECK_INTERVAL}")
    run_deployment(
        name="check_store_release/check_store_release",
        parameters={"platform": platform, "version": version},
        scheduled_time=datetime.now(timezone.utc) + CHECK_INTERVAL,
        timeout=0,  # fire-and-forget: don't block this (short-lived) run on the new one
    )
