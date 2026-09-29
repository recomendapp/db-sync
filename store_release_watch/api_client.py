import requests


def get_release_state(api_url: str, internal_secret: str, platform: str, version: str) -> dict | None:
    """
    GET the release's current state from the API. Returns None if it was never
    reported (shouldn't normally happen -- this flow is only ever triggered
    right after the API itself inserted the row).
    """
    response = requests.get(
        f"{api_url}/v1/internal/version-policy/state",
        params={"platform": platform, "version": version},
        headers={"Authorization": f"Bearer {internal_secret}"},
        timeout=10,
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def confirm_release_live(api_url: str, internal_secret: str, platform: str, version: str) -> None:
    """Tell the API this version is now confirmed available on the store."""
    response = requests.post(
        f"{api_url}/v1/internal/version-policy/confirm",
        json={"platform": platform, "version": version},
        headers={"Authorization": f"Bearer {internal_secret}"},
        timeout=10,
    )
    response.raise_for_status()
