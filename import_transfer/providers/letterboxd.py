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


def _find_entry(zf: zipfile.ZipFile, relative_path: str) -> str | None:
    """Letterboxd's own "Export data" always wraps everything in a top-level
    `letterboxd-<user>-<date>-utc/` folder; a zip built by re-zipping an already-extracted
    folder (see zipFolder.ts, used when a user selects a folder instead of a .zip) doesn't.
    Match by suffix so both shapes work, skipping macOS zip metadata entries."""
    for name in zf.namelist():
        if name.startswith("__MACOSX/"):
            continue
        if name == relative_path or name.endswith("/" + relative_path):
            return name
    return None


def _read_csv(zf: zipfile.ZipFile, relative_path: str) -> pd.DataFrame | None:
    entry = _find_entry(zf, relative_path)
    if entry is None:
        return None
    with zf.open(entry) as f:
        return pd.read_csv(io.BytesIO(f.read()))


def _find_list_entries(zf: zipfile.ZipFile) -> list[str]:
    """Each list is its own file under lists/ with a name derived from the list's title (e.g.
    lists/cool-stuff.csv) — unlike every other export file, there's no fixed filename to look
    up, so this scans for anything directly inside a lists/ directory. Matched by parent
    directory name rather than a path prefix so it works whether or not the export is wrapped
    in a top-level folder, and doesn't accidentally pick up likes/lists.csv (parent dir
    "likes", not "lists" — a totally different, unrelated file)."""
    entries = []
    for name in zf.namelist():
        if name.startswith("__MACOSX/") or not name.endswith(".csv"):
            continue
        parts = name.split("/")
        if len(parts) >= 2 and parts[-2] == "lists":
            entries.append(name)
    return entries


def _parse_list_csv(raw_text: str) -> dict:
    """Letterboxd list exports aren't a single flat table — line 1 is a version banner
    ("Letterboxd list export v7"), then a one-row metadata block (list title/description),
    a blank line, then the item rows:

        Letterboxd list export v7
        Date,Name,Tags,URL,Description
        2026-08-25,Cool stuff,,https://boxd.it/WQfBy,

        Position,Name,Year,URL,Description
        1,Toy Story,1995,https://boxd.it/29qA,
        ...

    Not parseable by a single pd.read_csv call."""
    # Letterboxd exports use CRLF line endings, so the blank-line separator is "\r\n\r\n", not
    # "\n\n" — normalize first or the split below silently never matches.
    raw_text = raw_text.replace("\r\n", "\n")
    _banner, _, body = raw_text.partition("\n")
    meta_block, _, items_block = body.partition("\n\n")

    meta = pd.read_csv(io.StringIO(meta_block)).iloc[0]
    title = str(meta["Name"])
    description = meta.get("Description")
    description = str(description).strip() if pd.notna(description) and str(description).strip() else None

    items_df = pd.read_csv(io.StringIO(items_block)) if items_block.strip() else pd.DataFrame()
    items = [
        {"rawTitle": row["Name"], "rawYear": _safe_int(row.get("Year")), "sourceOrder": _safe_int(row.get("Position")) or (i + 1)}
        for i, (_, row) in enumerate(items_df.iterrows())
    ]

    return {"title": title, "description": description, "items": items}


def _parse_export(zip_path: str) -> dict:
    with zipfile.ZipFile(zip_path) as zf:
        watched = _read_csv(zf, "watched.csv")
        diary = _read_csv(zf, "diary.csv")
        ratings = _read_csv(zf, "ratings.csv")
        watchlist = _read_csv(zf, "watchlist.csv")
        likes = _read_csv(zf, "likes/films.csv")
        list_entries = _find_list_entries(zf)
        lists = []
        for entry in list_entries:
            with zf.open(entry) as f:
                raw_text = f.read().decode("utf-8-sig")
            lists.append(_parse_list_csv(raw_text))

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

    return {"films": list(films.values()), "watchlist": watchlist_entries, "lists": lists}


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
    lists = parsed["lists"]
    list_item_count = sum(len(lst["items"]) for lst in lists)

    total = len(films) + len(watchlist) + list_item_count
    processed = matched = failed = 0
    logger.info(
        f"Parsed {len(films)} logged films, {len(watchlist)} watchlist entries, "
        f"{len(lists)} lists ({list_item_count} items)"
    )
    post_event(api_url, internal_secret, import_job_id, {"itemsTotal": total})

    # The same film routinely shows up in more than one place (logged + watchlisted + in a
    # list) — resolve every distinct (title, year) exactly once across all of them.
    match_keys = {(entry["rawTitle"], entry["rawYear"]) for entry in films}
    match_keys |= {(entry["rawTitle"], entry["rawYear"]) for entry in watchlist}
    for lst in lists:
        match_keys |= {(item["rawTitle"], item["rawYear"]) for item in lst["items"]}
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

    for lst in lists:
        staging_playlist_id = db_writer.insert_staging_playlist(
            db, import_job_id, lst["title"], lst["description"]
        )
        for item in lst["items"]:
            movie_id = movie_lookup[(item["rawTitle"], item["rawYear"])]
            db_writer.insert_staging_playlist_item(
                db,
                staging_playlist_id,
                item["rawTitle"],
                item["rawYear"],
                movie_id,
                item["sourceOrder"],
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
