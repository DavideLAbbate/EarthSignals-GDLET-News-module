"""
GDELT BigQuery query builder.

Builds parameterized GoogleSQL queries from NormalizedFilters.
Always selects only the 16 required columns.
Always applies SQLDATE integer range filtering (primary cost-control mechanism).
Enforces MAX_BQ_SCAN_DAYS to prevent runaway table scans.
"""

from __future__ import annotations

from google.cloud import bigquery

from app.core.config import get_settings
from app.core.exceptions import QueryValidationError

# The 16 columns we always select from gdelt-bq.gdeltv2.events
# Never use SELECT * — BigQuery bills on bytes scanned per column
GDELT_SELECT_COLUMNS = """
    GLOBALEVENTID,
    SQLDATE,
    Actor1CountryCode,
    Actor2CountryCode,
    EventCode,
    EventBaseCode,
    EventRootCode,
    QuadClass,
    GoldsteinScale,
    AvgTone,
    NumMentions,
    NumSources,
    NumArticles,
    ActionGeo_FullName,
    ActionGeo_CountryCode,
    SOURCEURL
""".strip()

GDELT_TABLE = "`gdelt-bq.gdeltv2.events`"


def build_events_query(
    *,
    date_from_sqldate: int,
    date_to_sqldate: int,
    fips_country_code: str | None = None,
    cameo_country_code: str | None = None,
    event_root_codes: list[str] | None = None,
    event_base_codes: list[str] | None = None,
    max_results: int | None = None,
) -> tuple[str, list]:
    """
    Build a parameterized BigQuery SQL query for GDELT events.

    Returns:
        (sql_string, list_of_query_parameters)

    Raises:
        QueryValidationError: if date range exceeds MAX_BQ_SCAN_DAYS or
                              if no meaningful filter is provided.
    """
    settings = get_settings()
    max_results = max_results or settings.bq_max_results

    _validate_date_range(date_from_sqldate, date_to_sqldate, settings.max_bq_scan_days)

    params: list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter] = []
    where_clauses: list[str] = []

    # ── Required: date range (always INTEGER comparison, never CAST) ──────
    where_clauses.append("SQLDATE >= @date_from AND SQLDATE <= @date_to")
    params.append(bigquery.ScalarQueryParameter("date_from", "INT64", date_from_sqldate))
    params.append(bigquery.ScalarQueryParameter("date_to", "INT64", date_to_sqldate))

    # ── Optional: geographic country filter (FIPS 2-letter) ──────────────
    if fips_country_code:
        where_clauses.append("ActionGeo_CountryCode = @fips_country_code")
        params.append(
            bigquery.ScalarQueryParameter("fips_country_code", "STRING", fips_country_code)
        )

    # ── Optional: actor country filter (CAMEO 3-letter) ──────────────────
    if cameo_country_code:
        where_clauses.append(
            "(Actor1CountryCode = @cameo_country_code OR Actor2CountryCode = @cameo_country_code)"
        )
        params.append(
            bigquery.ScalarQueryParameter("cameo_country_code", "STRING", cameo_country_code)
        )

    # ── Optional: event root codes (CAMEO 2-digit) ────────────────────────
    if event_root_codes:
        where_clauses.append("EventRootCode IN UNNEST(@event_root_codes)")
        params.append(
            bigquery.ArrayQueryParameter("event_root_codes", "STRING", event_root_codes)
        )

    # ── Optional: event base codes (CAMEO 3-digit, more granular) ─────────
    if event_base_codes:
        where_clauses.append("EventBaseCode IN UNNEST(@event_base_codes)")
        params.append(
            bigquery.ArrayQueryParameter("event_base_codes", "STRING", event_base_codes)
        )

    where_sql = " AND ".join(where_clauses)

    sql = f"""
SELECT
    {GDELT_SELECT_COLUMNS}
FROM {GDELT_TABLE}
WHERE
    {where_sql}
ORDER BY SQLDATE DESC, NumMentions DESC
LIMIT @max_results
""".strip()

    params.append(bigquery.ScalarQueryParameter("max_results", "INT64", max_results))

    return sql, params


def build_sync_latest_timestamp_query() -> tuple[str, list]:
    """Query for the most recent SQLDATE and DATEADDED in the Events table."""
    sql = f"""
SELECT
    MAX(SQLDATE) AS latest_sqldate,
    MAX(DATEADDED) AS latest_dateadded
FROM {GDELT_TABLE}
""".strip()
    return sql, []


def build_sync_top_countries_query(last_n_days_sqldate: int) -> tuple[str, list]:
    """
    Query top-20 geographic countries by event count over the last N days.
    Uses FIPS codes (ActionGeo_CountryCode).
    """
    sql = f"""
SELECT
    ActionGeo_CountryCode AS fips_code,
    COUNT(*) AS event_count
FROM {GDELT_TABLE}
WHERE
    SQLDATE >= @since_sqldate
    AND ActionGeo_CountryCode IS NOT NULL
    AND ActionGeo_CountryCode != ''
GROUP BY ActionGeo_CountryCode
ORDER BY event_count DESC
LIMIT 20
""".strip()
    params = [bigquery.ScalarQueryParameter("since_sqldate", "INT64", last_n_days_sqldate)]
    return sql, params


def build_sync_top_event_codes_query(last_n_days_sqldate: int) -> tuple[str, list]:
    """
    Query top-20 event root codes by event count over the last N days.
    """
    sql = f"""
SELECT
    EventRootCode AS root_code,
    COUNT(*) AS event_count
FROM {GDELT_TABLE}
WHERE
    SQLDATE >= @since_sqldate
    AND EventRootCode IS NOT NULL
    AND EventRootCode != ''
GROUP BY EventRootCode
ORDER BY event_count DESC
LIMIT 20
""".strip()
    params = [bigquery.ScalarQueryParameter("since_sqldate", "INT64", last_n_days_sqldate)]
    return sql, params


# ── Private helpers ───────────────────────────────────────────────────────


def _validate_date_range(
    date_from: int, date_to: int, max_bq_scan_days: int
) -> None:
    """
    Validate the date range.

    Raises QueryValidationError if:
      - date_from > date_to
      - date range exceeds max_bq_scan_days
    """
    if date_from > date_to:
        raise QueryValidationError(
            f"date_from ({date_from}) must be <= date_to ({date_to})"
        )

    # Convert YYYYMMDD integers to approximate day count for range check
    from_year, from_day = divmod(date_from, 10000)
    to_year, to_day = divmod(date_to, 10000)
    # Rough approximation: (year difference * 365) + (month/day difference)
    approximate_days = (to_year - from_year) * 365 + (to_day // 100 - from_day // 100) * 30

    if approximate_days > max_bq_scan_days:
        raise QueryValidationError(
            f"Date range of ~{approximate_days} days exceeds the maximum allowed "
            f"{max_bq_scan_days} days. Narrow your date range to reduce BigQuery costs.",
            detail=f"date_from={date_from}, date_to={date_to}, max_days={max_bq_scan_days}",
        )
