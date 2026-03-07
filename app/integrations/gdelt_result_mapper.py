"""
Maps raw BigQuery row dicts → GDELTEvent Pydantic models.

Source name is derived from the SOURCEURL domain (e.g., "ansa.it")
because the Events table does not contain a source name column.
A GKG join would cost 200+ GB per month/country and is out of scope.
"""

from __future__ import annotations

from urllib.parse import urlparse

from app.schemas.events import GDELTEvent


def _extract_source_name(source_url: str | None) -> str | None:
    """Extract the domain name from a URL to use as the source name."""
    if not source_url:
        return None
    try:
        parsed = urlparse(source_url)
        netloc = parsed.netloc or ""
        # Strip 'www.' prefix for cleaner display
        return netloc.removeprefix("www.") or None
    except Exception:
        return None


def _sqldate_to_iso(sqldate: int | None) -> str | None:
    """Convert a GDELT SQLDATE integer (YYYYMMDD) to an ISO date string (YYYY-MM-DD)."""
    if sqldate is None:
        return None
    s = str(sqldate)
    if len(s) != 8:
        return str(sqldate)
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"


def map_row_to_event(row: dict) -> GDELTEvent:
    """
    Map a single BigQuery row dict to a GDELTEvent model.

    Field mapping:
      GLOBALEVENTID        → event_id
      SQLDATE              → date (converted to ISO string)
      Actor1CountryCode    → actor1_country
      Actor2CountryCode    → actor2_country
      EventCode            → event_code
      EventBaseCode        → event_base_code
      EventRootCode        → event_root_code
      QuadClass            → quad_class
      GoldsteinScale       → goldstein_scale
      AvgTone              → tone
      NumMentions          → num_mentions
      NumSources           → num_sources
      NumArticles          → num_articles
      ActionGeo_FullName   → action_geo_fullname
      ActionGeo_CountryCode → action_geo_country
      SOURCEURL            → source_url + derived source_name
    """
    source_url = row.get("SOURCEURL")

    return GDELTEvent(
        event_id=str(row.get("GLOBALEVENTID", "")),
        date=_sqldate_to_iso(row.get("SQLDATE")),
        actor1_country=row.get("Actor1CountryCode") or None,
        actor2_country=row.get("Actor2CountryCode") or None,
        event_code=row.get("EventCode") or None,
        event_base_code=row.get("EventBaseCode") or None,
        event_root_code=row.get("EventRootCode") or None,
        quad_class=row.get("QuadClass"),
        goldstein_scale=row.get("GoldsteinScale"),
        tone=row.get("AvgTone"),
        num_mentions=row.get("NumMentions"),
        num_sources=row.get("NumSources"),
        num_articles=row.get("NumArticles"),
        action_geo_fullname=row.get("ActionGeo_FullName") or None,
        action_geo_country=row.get("ActionGeo_CountryCode") or None,
        source_name=_extract_source_name(source_url),
        source_url=source_url or None,
    )


def map_rows_to_events(rows: list[dict]) -> list[GDELTEvent]:
    """Map a list of BigQuery row dicts to a list of GDELTEvent models."""
    events: list[GDELTEvent] = []
    for row in rows:
        try:
            events.append(map_row_to_event(row))
        except Exception:
            # Skip malformed rows; log in the calling layer if needed
            continue
    return events
