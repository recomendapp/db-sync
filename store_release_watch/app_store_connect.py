"""
App Store Connect has no webhook for "this version is now live" -- it must be
polled. Auth is a short-lived ES256 JWT signed with an App Store Connect API
key (App Store Connect -> Users and Access -> Integrations -> Keys; NOT the
Sign-in-with-Apple key already used for auth). Docs:
https://developer.apple.com/documentation/appstoreconnectapi
"""

import time

import jwt
import requests
from prefect.blocks.system import Secret

ASC_BASE_URL = "https://api.appstoreconnect.apple.com/v1"
# Apple's appStoreVersions.attributes.appStoreState values that mean "a user can
# actually download this version right now". PENDING_DEVELOPER_RELEASE is
# deliberately excluded: approved, but the developer hasn't hit "Release" yet.
LIVE_STATES = {"READY_FOR_SALE"}


def _get_token() -> str:
    key_id = Secret.load("app-store-connect-key-id").get()
    issuer_id = Secret.load("app-store-connect-issuer-id").get()
    private_key = Secret.load("app-store-connect-private-key").get()

    now = int(time.time())
    payload = {
        "iss": issuer_id,
        "iat": now,
        "exp": now + 15 * 60,
        "aud": "appstoreconnect-v1",
    }
    return jwt.encode(payload, private_key, algorithm="ES256", headers={"kid": key_id, "typ": "JWT"})


def is_ios_version_live(version: str) -> bool:
    app_id = Secret.load("app-store-connect-app-id").get()
    token = _get_token()

    response = requests.get(
        f"{ASC_BASE_URL}/apps/{app_id}/appStoreVersions",
        params={
            "filter[versionString]": version,
            "fields[appStoreVersions]": "versionString,appStoreState",
        },
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    response.raise_for_status()

    return any(
        item["attributes"]["versionString"] == version
        and item["attributes"]["appStoreState"] in LIVE_STATES
        for item in response.json().get("data", [])
    )
