import zipfile
import io
import pandas as pd
from prefect.logging import get_run_logger

from .. import db_writer
from .. import matcher
from ..api_client import post_event
from ..s3_client import download_import_file
from ..models.db_client import DBClient
from ..models.typesense_client import TypesenseClient

# Progress is pushed to the API every N processed items, not on every single one —
# keeps the websocket chatter reasonable for large libraries.
PROGRESS_INTERVAL = 25


def _safe_int(value) -> int | None:
    try:
        if value is None or pd.isna(value):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _read_csv(zf: zipfile.ZipFile, name: str) -> pd.DataFrame | None:
    if name not in zf.namelist():
        return None
    with zf.open(name) as f:
        return pd.read_csv(io.BytesIO(f.read()))


def _parse_export(zip_path: str) -> dict:
    with zipfile.ZipFile(zip_path) as zf:
        watched = _read_csv(zf, "watched.csv")
        diary = _read_csv(zf, "diary.csv")
        ratings = _read_csv(zf, "ratings.csv")
        watchlist = _read_csv(zf, "watchlist.csv")
        likes = _read_csv(zf, "likes/films.csv")

    # One entry per film, keyed by (Name, Year) — Letterboxd's own de-dup key. Rating scale:
    # Letterboxd is 0.5-5 stars, our log_movie.rating check constraint wants 0.5-10 (half steps),
    # so we double it.
    films: dict[tuple, dict] = {}

    def get_or_create(name: str, year) -> dict:
        key = (name, _safe_int(year))
        if key not in films:
            films[key] = {
                "rawTitle": name,
                "rawYear": _safe_int(year),
                "watchedDates": [],
                "rating": None,
                "isLiked": False,
            }
        return films[key]

    if watched is not None:
        for _, row in watched.iterrows():
            get_or_create(row["Name"], row.get("Year"))

    if diary is not None:
        for _, row in diary.iterrows():
            entry = get_or_create(row["Name"], row.get("Year"))
            watched_date = row.get("Watched Date")
            if watched_date is not None and not pd.isna(watched_date):
                entry["watchedDates"].append(str(watched_date))
            rating = row.get("Rating")
            if rating is not None and not pd.isna(rating):
                entry["rating"] = float(rating) * 2

    if ratings is not None:
        for _, row in ratings.iterrows():
            entry = get_or_create(row["Name"], row.get("Year"))
            if entry["rating"] is None:
                rating = row.get("Rating")
                if rating is not None and not pd.isna(rating):
                    entry["rating"] = float(rating) * 2

    # Likes only apply to films already being logged (matches watched/diary/ratings) — Letterboxd
    # can carry a like with no corresponding watch, but this app's is_liked lives on log_movie
    # itself, so there's no log to attach it to without inventing a watch that didn't happen.
    if likes is not None:
        for _, row in likes.iterrows():
            key = (row["Name"], _safe_int(row.get("Year")))
            if key in films:
                films[key]["isLiked"] = True

    watchlist_entries = []
    if watchlist is not None:
        for _, row in watchlist.iterrows():
            watchlist_entries.append(
                {"rawTitle": row["Name"], "rawYear": _safe_int(row.get("Year"))}
            )

    return {"films": list(films.values()), "watchlist": watchlist_entries}


def run(
    db: DBClient,
    ts: TypesenseClient,
    api_url: str,
    internal_secret: str,
    import_job_id: str,
    user_id: str,
    s3_key: str,
) -> None:
    logger = get_run_logger()

    zip_path = download_import_file(s3_key)
    parsed = _parse_export(zip_path)
    films = parsed["films"]
    watchlist = parsed["watchlist"]

    total = len(films) + len(watchlist)
    processed = matched = failed = 0
    logger.info(f"Parsed {len(films)} logged films and {len(watchlist)} watchlist entries")
    post_event(api_url, internal_secret, import_job_id, {"itemsTotal": total})

    match_keys = {(entry["rawTitle"], entry["rawYear"]) for entry in films} | {
        (entry["rawTitle"], entry["rawYear"]) for entry in watchlist
    }
    logger.info(f"Matching {len(match_keys)} distinct titles")
    movie_lookup = matcher.match_movies_batch(ts, match_keys)

    for entry in films:
        movie_id = movie_lookup[(entry["rawTitle"], entry["rawYear"])]
        existing = db_writer.get_existing_log_movie(db, user_id, movie_id) if movie_id else None
        existing_dates = (
            db_writer.get_existing_watched_dates(db, existing["id"]) if existing else set()
        )

        staging_id = db_writer.insert_staging_log_movie(
            db,
            import_job_id,
            entry["rawTitle"],
            entry["rawYear"],
            movie_id,
            entry["rating"],
            entry["isLiked"],
        )
        db_writer.insert_staging_watched_dates(
            db, staging_id, entry["watchedDates"], existing_dates
        )

        processed += 1
        matched += 1 if movie_id else 0
        failed += 0 if movie_id else 1

        if processed % PROGRESS_INTERVAL == 0:
            post_event(
                api_url,
                internal_secret,
                import_job_id,
                {"itemsProcessed": processed, "itemsMatched": matched, "itemsFailed": failed},
            )

    for entry in watchlist:
        movie_id = movie_lookup[(entry["rawTitle"], entry["rawYear"])]
        db_writer.insert_staging_bookmark(
            db, import_job_id, entry["rawTitle"], entry["rawYear"], movie_id
        )

        processed += 1
        matched += 1 if movie_id else 0
        failed += 0 if movie_id else 1

        if processed % PROGRESS_INTERVAL == 0:
            post_event(
                api_url,
                internal_secret,
                import_job_id,
                {"itemsProcessed": processed, "itemsMatched": matched, "itemsFailed": failed},
            )

    logger.info(f"Done: {processed} processed, {matched} matched, {failed} unmatched")
    post_event(
        api_url,
        internal_secret,
        import_job_id,
        {
            "itemsProcessed": processed,
            "itemsMatched": matched,
            "itemsFailed": failed,
            "status": "awaiting_review",
        },
    )
