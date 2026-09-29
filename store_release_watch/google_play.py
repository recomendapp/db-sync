"""
Google Play has no webhook for "this version is now live" either -- Real-time
Developer Notifications only cover purchases/subscriptions, not release status.
Polled via the Play Developer API (androidpublisher), authenticated with a
service account (Play Console -> Setup -> API access). Docs:
https://developers.google.com/android-publisher/api-ref/rest/v3/edits.tracks

N.B. an "edit" must be opened to read track state and explicitly discarded --
there's no read-only endpoint for this. It's never committed, so it never
actually changes anything on Play.
"""

import json

from google.oauth2 import service_account
from googleapiclient.discovery import build
from prefect.blocks.system import Secret

SCOPES = ["https://www.googleapis.com/auth/androidpublisher"]
PRODUCTION_TRACK = "production"
# Play release statuses that mean "available to users" -- "inProgress" and
# "halted" are a staged rollout still ramping up or paused midway.
LIVE_STATUSES = {"completed"}


def is_android_version_live(version: str) -> bool:
    package_name = Secret.load("google-play-package-name").get()
    credentials_json = json.loads(Secret.load("google-play-service-account-json").get())
    credentials = service_account.Credentials.from_service_account_info(credentials_json, scopes=SCOPES)
    service = build("androidpublisher", "v3", credentials=credentials)

    edit_id = service.edits().insert(packageName=package_name, body={}).execute()["id"]
    try:
        track = (
            service.edits()
            .tracks()
            .get(packageName=package_name, editId=edit_id, track=PRODUCTION_TRACK)
            .execute()
        )
    finally:
        # Read-only visit: always discard, never commit.
        service.edits().delete(packageName=package_name, editId=edit_id).execute()

    return any(
        release.get("name") == version and release.get("status") in LIVE_STATUSES
        for release in track.get("releases", [])
    )
