import math

DEFAULT_FLOOR = 0.1
DEFAULT_CEILING = 1.0

def compute_priority(
    popularity: float | None,
    max_popularity: float,
    floor: float = DEFAULT_FLOOR,
    ceiling: float = DEFAULT_CEILING,
) -> float:
    """
    Computes a sitemap priority (between `floor` and `ceiling`) from a TMDB popularity score,
    using a logarithmic scale to avoid a few very popular items from dominating the distribution.

    :param popularity: the raw popularity score of the item (can be None/0)
    :param max_popularity: the maximum popularity observed in the batch/catalog,
                           used to normalize the log scale
  
    :param floor: the minimum priority to assign (never 0, per sitemap recommendations)
    :param ceiling: the maximum priority to assign

    :return: a float between `floor` and `ceiling`, rounded to 2 decimal places
    """
    if not popularity or popularity <= 0 or max_popularity <= 0:
        return floor

    log_popularity = math.log1p(popularity)
    log_max = math.log1p(max_popularity)

    if log_max == 0:
        return floor

    normalized = log_popularity / log_max
    normalized = min(max(normalized, 0.0), 1.0)

    priority = floor + normalized * (ceiling - floor)
    return round(priority, 2)