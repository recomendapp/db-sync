from .models.db_client import DBClient


def get_existing_log_movie(db: DBClient, user_id: str, movie_id: int) -> dict | None:
    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, rating, is_liked FROM log_movie WHERE user_id = %s AND movie_id = %s",
                (user_id, movie_id),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {"id": row[0], "rating": row[1], "is_liked": row[2]}


def get_existing_watched_dates(db: DBClient, log_movie_id: int) -> set:
    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT watched_date FROM log_movie_watched_date WHERE log_movie_id = %s",
                (log_movie_id,),
            )
            return {row[0].isoformat() for row in cur.fetchall()}


def insert_staging_log_movie(
    db: DBClient,
    import_job_id: str,
    raw_title: str,
    raw_year: int | None,
    movie_id: int | None,
    rating: float | None,
    is_liked: bool,
) -> int:
    # No existing_log_movie_id/existing_rating/existing_is_liked here — the API now re-checks
    # conflicts live at validate() time instead of trusting a staging-time snapshot, which could
    # go stale if the user logs this movie themselves before reviewing the import (see
    # apps/api/src/app/imports/imports.service.ts::validate).
    match_status = "matched" if movie_id else "unmatched"
    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO import_job_log_movie
                    (import_job_id, raw_title, raw_year, movie_id, match_status,
                     imported_rating, imported_is_liked)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    import_job_id,
                    raw_title,
                    raw_year,
                    movie_id,
                    match_status,
                    rating,
                    is_liked,
                ),
            )
            staging_id = cur.fetchone()[0]
            conn.commit()
            return staging_id


def insert_staging_watched_dates(
    db: DBClient,
    staging_log_movie_id: int,
    watched_dates: list[str],
    existing_dates: set,
) -> None:
    if not watched_dates:
        return
    with db.connection() as conn:
        with conn.cursor() as cur:
            for watched_date in watched_dates:
                cur.execute(
                    """
                    INSERT INTO import_job_log_movie_watched_date
                        (import_job_log_movie_id, watched_date, is_duplicate)
                    VALUES (%s, %s, %s)
                    """,
                    (staging_log_movie_id, watched_date, watched_date in existing_dates),
                )
            conn.commit()


def insert_staging_bookmark(
    db: DBClient,
    import_job_id: str,
    raw_title: str,
    raw_year: int | None,
    movie_id: int | None,
) -> None:
    match_status = "matched" if movie_id else "unmatched"
    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO import_job_bookmark (import_job_id, raw_title, raw_year, type, movie_id, match_status)
                VALUES (%s, %s, %s, 'movie', %s, %s)
                """,
                (import_job_id, raw_title, raw_year, movie_id, match_status),
            )
            conn.commit()
