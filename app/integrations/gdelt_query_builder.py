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
SOURCE_DOMAIN_SQL = "REGEXP_EXTRACT(LOWER(SOURCEURL), r'^https?://(?:www\\.)?([^/]+)')"


def build_events_query(
    *,
    date_from_sqldate: int,
    date_to_sqldate: int,
    fips_country_code: str | None = None,
    geo_country_codes: list[str] | None = None,
    cameo_country_code: str | None = None,
    actor1_country_code: str | None = None,
    actor2_country_code: str | None = None,
    event_root_codes: list[str] | None = None,
    event_base_codes: list[str] | None = None,
    event_codes: list[str] | None = None,
    quad_classes: list[int] | None = None,
    source_domains: list[str] | None = None,
    tone_min: float | None = None,
    tone_max: float | None = None,
    goldstein_min: float | None = None,
    goldstein_max: float | None = None,
    min_mentions: int | None = None,
    min_sources: int | None = None,
    min_articles: int | None = None,
    max_results: int | None = None,
) -> tuple[str, list]:
    """Build a parameterized BigQuery SQL query for GDELT events."""
    settings = get_settings()
    max_results = max_results or settings.bq_max_results

    _validate_date_range(date_from_sqldate, date_to_sqldate, settings.max_bq_scan_days)

    params: list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter] = []
    where_clauses: list[str] = []

    # ── Required: date range (always INTEGER comparison, never CAST) ──────
    where_clauses.append("SQLDATE >= @date_from AND SQLDATE <= @date_to")
    params.append(bigquery.ScalarQueryParameter("date_from", "INT64", date_from_sqldate))
    params.append(bigquery.ScalarQueryParameter("date_to", "INT64", date_to_sqldate))

    # ── Optional: geographic filters (FIPS 2-letter) ──────────────────────
    merged_geo_codes = sorted(set((geo_country_codes or []) + ([fips_country_code] if fips_country_code else [])))
    if merged_geo_codes:
        where_clauses.append("ActionGeo_CountryCode IN UNNEST(@geo_country_codes)")
        params.append(bigquery.ArrayQueryParameter("geo_country_codes", "STRING", merged_geo_codes))

    # ── Optional: broad actor-country filter (either actor) ────────────────
    if cameo_country_code:
        where_clauses.append(
            "(Actor1CountryCode = @cameo_country_code OR Actor2CountryCode = @cameo_country_code)"
        )
        params.append(bigquery.ScalarQueryParameter("cameo_country_code", "STRING", cameo_country_code))

    # ── Optional: direct actor-country filters ─────────────────────────────
    if actor1_country_code:
        where_clauses.append("Actor1CountryCode = @actor1_country_code")
        params.append(bigquery.ScalarQueryParameter("actor1_country_code", "STRING", actor1_country_code))

    if actor2_country_code:
        where_clauses.append("Actor2CountryCode = @actor2_country_code")
        params.append(bigquery.ScalarQueryParameter("actor2_country_code", "STRING", actor2_country_code))

    # ── Optional: event code filters ───────────────────────────────────────
    if event_root_codes:
        where_clauses.append("EventRootCode IN UNNEST(@event_root_codes)")
        params.append(bigquery.ArrayQueryParameter("event_root_codes", "STRING", event_root_codes))

    if event_base_codes:
        where_clauses.append("EventBaseCode IN UNNEST(@event_base_codes)")
        params.append(bigquery.ArrayQueryParameter("event_base_codes", "STRING", event_base_codes))

    if event_codes:
        where_clauses.append("EventCode IN UNNEST(@event_codes)")
        params.append(bigquery.ArrayQueryParameter("event_codes", "STRING", event_codes))

    # ── Optional: classification / sentiment / impact filters ──────────────
    if quad_classes:
        where_clauses.append("QuadClass IN UNNEST(@quad_classes)")
        params.append(bigquery.ArrayQueryParameter("quad_classes", "INT64", quad_classes))

    if tone_min is not None:
        where_clauses.append("AvgTone >= @tone_min")
        params.append(bigquery.ScalarQueryParameter("tone_min", "FLOAT64", tone_min))

    if tone_max is not None:
        where_clauses.append("AvgTone <= @tone_max")
        params.append(bigquery.ScalarQueryParameter("tone_max", "FLOAT64", tone_max))

    if goldstein_min is not None:
        where_clauses.append("GoldsteinScale >= @goldstein_min")
        params.append(bigquery.ScalarQueryParameter("goldstein_min", "FLOAT64", goldstein_min))

    if goldstein_max is not None:
        where_clauses.append("GoldsteinScale <= @goldstein_max")
        params.append(bigquery.ScalarQueryParameter("goldstein_max", "FLOAT64", goldstein_max))

    if min_mentions is not None:
        where_clauses.append("NumMentions >= @min_mentions")
        params.append(bigquery.ScalarQueryParameter("min_mentions", "INT64", min_mentions))

    if min_sources is not None:
        where_clauses.append("NumSources >= @min_sources")
        params.append(bigquery.ScalarQueryParameter("min_sources", "INT64", min_sources))

    if min_articles is not None:
        where_clauses.append("NumArticles >= @min_articles")
        params.append(bigquery.ScalarQueryParameter("min_articles", "INT64", min_articles))

    # ── Optional: source domains ───────────────────────────────────────────
    if source_domains:
        where_clauses.append(f"{SOURCE_DOMAIN_SQL} IN UNNEST(@source_domains)")
        params.append(bigquery.ArrayQueryParameter("source_domains", "STRING", source_domains))

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
    """Query top-20 event root codes by event count over the last N days."""
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
    """Validate the date range against order and rough scan size."""
    if date_from > date_to:
        raise QueryValidationError(
            f"date_from ({date_from}) must be <= date_to ({date_to})"
        )

    from_year, from_day = divmod(date_from, 10000)
    to_year, to_day = divmod(date_to, 10000)
    approximate_days = (to_year - from_year) * 365 + (to_day // 100 - from_day // 100) * 30

    if approximate_days > max_bq_scan_days:
        raise QueryValidationError(
            f"Date range of ~{approximate_days} days exceeds the maximum allowed "
            f"{max_bq_scan_days} days. Narrow your date range to reduce BigQuery costs.",
            detail=f"date_from={date_from}, date_to={date_to}, max_days={max_bq_scan_days}",
        )
