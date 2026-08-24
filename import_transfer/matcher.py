import math
import time

from .models.typesense_client import TypesenseClient

# Weights for the combined match score. Title relevance dominates (a wrong title should never
# win no matter how close the year is); year-closeness and popularity are tie-breakers among
# plausible title matches, not filters in their own right.
TITLE_WEIGHT = 1.0
YEAR_WEIGHT = 0.5
POPULARITY_WEIGHT = 0.05

# Below this, we don't trust the best candidate enough to call it a match. Tuned against a real
# 85-title Letterboxd export (see providers/letterboxd.py): every genuine title scored well above
# this with a full title match (title_score=1.0) even when the year was completely unknown, while
# the one genuinely nonexistent title's best candidate (partial title match) fell short.
MATCH_SCORE_THRESHOLD = 0.85


def _year_closeness(release_ts: int | None, year: int | None) -> float:
    """1.0 = exact year match, decaying as the candidate's release year drifts from the export's
    year. Never a hard filter — Letterboxd's year can legitimately be off by one (festival vs.
    wide release, regional release dates, re-releases), so this only nudges the score: the closer
    the date, the more it adds. Neutral (0.5) when either side has no year to compare, so title
    relevance alone still decides."""
    if release_ts is None or year is None:
        return 0.5
    candidate_year = time.gmtime(release_ts).tm_year
    return 1.0 / (1.0 + abs(candidate_year - year))


def _score_candidate(candidate: dict, year: int | None) -> float:
    # _title_score (0..1, set by TypesenseClient.search_movies from num_tokens_dropped) is how
    # much of the searched title was actually found in this candidate — the primary signal.
    title_score = candidate.get("_title_score", 0.0)
    year_score = _year_closeness(candidate.get("release_date"), year)
    popularity_bonus = POPULARITY_WEIGHT * math.log1p(candidate.get("popularity") or 0)
    return TITLE_WEIGHT * title_score + YEAR_WEIGHT * year_score + popularity_bonus


def match_movie(ts: TypesenseClient, title: str, year: int | None) -> int | None:
    """
    Typesense full-text search (typo-tolerant) against the already-synced catalog only, scored by
    title relevance + release-year closeness + a small popularity tie-breaker. Below
    MATCH_SCORE_THRESHOLD (or no candidates at all) counts as unmatched — no external TMDb calls
    here, this flow only ever resolves against what's already in our own synced catalog.
    """
    candidates = ts.search_movies(title)
    if not candidates:
        return None

    best = max(candidates, key=lambda c: _score_candidate(c, year))
    if _score_candidate(best, year) >= MATCH_SCORE_THRESHOLD:
        return int(best["id"])

    return None


def match_movies_batch(
    ts: TypesenseClient,
    keys: set[tuple[str, int | None]],
) -> dict[tuple[str, int | None], int | None]:
    """Resolves every (raw_title, raw_year) pair exactly once. The same film routinely shows up
    in more than one place in an export (logged + watchlisted + in a list), and each lookup is a
    Typesense round trip — callers should collect every (title, year) pair they need across
    logs/bookmarks/lists up front and call this once, instead of matching the same title
    repeatedly."""
    return {key: match_movie(ts, key[0], key[1]) for key in keys}
