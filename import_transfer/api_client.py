import requests


def post_event(api_url: str, internal_secret: str, import_job_id: str, payload: dict) -> None:
    """
    Push a progress update to the API's internal webhook, which relays it to the user
    over the existing /realtime websocket (see apps/api/src/app/imports). Best-effort:
    a missed progress ping must never fail the whole import.
    """
    try:
        requests.post(
            f"{api_url}/v1/internal/imports/{import_job_id}/events",
            json=payload,
            headers={"Authorization": f"Bearer {internal_secret}"},
            timeout=10,
        )
    except Exception:
        pass
