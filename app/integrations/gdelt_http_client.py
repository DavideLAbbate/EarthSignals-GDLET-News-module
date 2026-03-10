"""
GDELT HTTP client for direct CSV file ingestion.

Downloads and parses GDELT v2 export ZIP files from the public GDELT file
server. Replaces the BigQuery path for event ingestion by fetching the
compressed CSVs that GDELT publishes every 15 minutes.

Key endpoints:
- lastupdate.txt  — 3 lines, one per file type; format: <size> <md5> <url>
- masterfilelist.txt — full historical archive, same format per line
- *.export.CSV.zip   — 61-column tab-separated events CSV, no header row
- *.mentions.CSV.zip — 14-column tab-separated EVENTMENTIONS CSV, no header row
- *.gkg.csv.zip      — 27-column tab-separated GKG 2.1 CSV, no header row
"""

from __future__ import annotations

import io
import re
import zipfile
from typing import Any, Callable

import httpx

from app.core.exceptions import IngestionError
from app.core.logging import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_LASTUPDATE_URL = "https://data.gdeltproject.org/gdeltv2/lastupdate.txt"
_MASTERFILELIST_URL = "https://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

# Minimum number of columns required in a valid GDELT v2 export row.
_GDELT_COLUMN_COUNT = 61

# Minimum columns required in a valid GDELT EVENTMENTIONS row (14 fields used).
_MENTIONS_COLUMN_COUNT = 14

# ── GKG 2.1 column indices (0-based, tab-separated, 27 columns total) ──────
# Source: GDELT v2 GKG 2.1 file format specification
_GKG_COL_RECORD_ID = 0
_GKG_COL_DATE = 1
_GKG_COL_SOURCE_COMMON_NAME = 3
_GKG_COL_DOCUMENT_IDENTIFIER = 4
_GKG_COL_THEMES = 7
_GKG_COL_LOCATIONS = 9
_GKG_COL_PERSONS = 11
_GKG_COL_ORGANIZATIONS = 13
_GKG_COL_TONE = 15
_GKG_MIN_COLS = 16  # minimum columns to safely read through TONE

# Regex to extract the timestamp from any GDELT file URL (works for all three types):
#   .../20260308120000.export.CSV.zip
#   .../20260308120000.mentions.CSV.zip
#   .../20260308120000.gkg.csv.zip
_FILE_URL_TS_RE = re.compile(r"/(\d{14})\.", re.IGNORECASE)

# Legacy alias kept for backward-compat — points to the same regex.
_EXPORT_URL_TS_RE = _FILE_URL_TS_RE


# ---------------------------------------------------------------------------
# Pure parsing helpers
# ---------------------------------------------------------------------------


def _str_or_none(value: str) -> str | None:
    """Return the string stripped of whitespace, or None if empty."""
    stripped = value.strip()
    return stripped if stripped else None


def _int_or_none(value: str) -> int | None:
    """Parse an integer or return None for empty/whitespace strings."""
    stripped = value.strip()
    if not stripped:
        return None
    return int(stripped)


def _float_or_none(value: str) -> float | None:
    """Parse a float or return None for empty/whitespace strings."""
    stripped = value.strip()
    if not stripped:
        return None
    return float(stripped)


def _code_or_none(value: str, max_length: int) -> str | None:
    """Return an uppercase code if present and within the expected max length."""
    stripped = value.strip().upper()
    if not stripped or len(stripped) > max_length:
        return None
    return stripped


def parse_gdelt_csv_row(row: list[str]) -> dict[str, Any]:
    """
    Extract the 17 relevant fields from a 61-column GDELT v2 export row.

    Column indices are positional (no header). All optional fields that arrive
    as empty strings are converted to None; required integer fields are cast
    unconditionally.

    Args:
        row: A list of 61 string values from one tab-separated CSV line.

    Returns:
        A dict with the 17 mapped keys and correctly typed values.
    """
    return {
        "GLOBALEVENTID": int(row[0]),
        "SQLDATE": int(row[1]),
        "Actor1CountryCode": _code_or_none(row[7], 3),
        "Actor2CountryCode": _code_or_none(row[17], 3),
        "EventCode": _str_or_none(row[26]),
        "EventBaseCode": _str_or_none(row[27]),
        "EventRootCode": _str_or_none(row[28]),
        "QuadClass": _int_or_none(row[29]),
        "GoldsteinScale": _float_or_none(row[30]),
        "NumMentions": _int_or_none(row[31]),
        "NumSources": _int_or_none(row[32]),
        "NumArticles": _int_or_none(row[33]),
        "AvgTone": _float_or_none(row[34]),
        "ActionGeo_FullName": _str_or_none(row[52]),
        "ActionGeo_CountryCode": _code_or_none(row[53], 2),
        "DATEADDED": int(row[59]),
        "SOURCEURL": _str_or_none(row[60]),
    }


# ---------------------------------------------------------------------------
# lastupdate.txt / masterfilelist.txt parsing
# ---------------------------------------------------------------------------


def _parse_semicolon_field(value: str | None) -> list[str]:
    """Split a semicolon-delimited GDELT field into a list, stripping empty items."""
    if not value:
        return []
    return [item.strip() for item in value.split(";") if item.strip()]


def parse_gdelt_mentions_row(row: list[str]) -> dict[str, Any]:
    """
    Extract the 9 relevant fields from a GDELT EVENTMENTIONS row.

    EVENTMENTIONS CSV has no header. The 14 positional columns are:
        0  GLOBALEVENTID, 1 EventTimeDate, 2 MentionTimeDate, 3 MentionType,
        4  MentionSourceName, 5 MentionIdentifier, 6 SentenceAlid,
        7  Actor1CharOffset, 8 Actor2CharOffset, 9 ActionCharOffset,
        10 InRawText, 11 Confidence, 12 MentionDocLen, 13 MentionDocTone

    Returns an empty dict for short/malformed rows so callers can filter with `if row`.
    """
    if len(row) < _MENTIONS_COLUMN_COUNT:
        return {}
    return {
        "GLOBALEVENTID": _int_or_none(row[0]),
        "EventTimeDate": _int_or_none(row[1]),
        "MentionTimeDate": _int_or_none(row[2]),
        "MentionType": _int_or_none(row[3]),
        "MentionSourceName": _str_or_none(row[4]),
        "MentionIdentifier": _str_or_none(row[5]),
        "MentionDocLen": _int_or_none(row[12]),
        "MentionDocTone": _float_or_none(row[13]),
    }


def parse_gdelt_gkg_row(row: list[str]) -> dict[str, Any]:
    """
    Extract the 9 relevant fields from a GDELT GKG 2.1 row.

    GKG 2.1 has 27 tab-separated columns (no header). We read through column 15
    (V2TONE). The first comma-separated value of V2TONE is the average document tone.

    Returns an empty dict for short/malformed rows so callers can filter with `if row`.
    """
    if len(row) < _GKG_MIN_COLS:
        return {}
    tone_str = _str_or_none(row[_GKG_COL_TONE])
    avg_tone: float | None = None
    if tone_str:
        try:
            avg_tone = float(tone_str.split(",")[0])
        except (ValueError, IndexError):
            avg_tone = None
    return {
        "GKGRECORDID": _str_or_none(row[_GKG_COL_RECORD_ID]),
        "DATE": _int_or_none(row[_GKG_COL_DATE]),
        "SourceCommonName": _str_or_none(row[_GKG_COL_SOURCE_COMMON_NAME]),
        "DocumentIdentifier": _str_or_none(row[_GKG_COL_DOCUMENT_IDENTIFIER]),
        "V1Themes": _parse_semicolon_field(_str_or_none(row[_GKG_COL_THEMES])),
        "V1Locations": _parse_semicolon_field(_str_or_none(row[_GKG_COL_LOCATIONS])),
        "V1Persons": _parse_semicolon_field(_str_or_none(row[_GKG_COL_PERSONS])),
        "V1Organizations": _parse_semicolon_field(_str_or_none(row[_GKG_COL_ORGANIZATIONS])),
        "AvgTone": avg_tone,
    }


def _parse_export_lines(text: str, filename_fragment: str = "export") -> list[tuple[str, int]]:
    """
    Parse lines from lastupdate.txt or masterfilelist.txt.

    Each line has the format: <size> <md5> <url>
    Returns a list of (url, timestamp_int) tuples for lines whose URL contains
    ``filename_fragment``, preserving original line order.

    The default ``filename_fragment="export"`` preserves backward-compatible
    behaviour (only events export lines are returned). Pass ``"mentions"`` or
    ``"gkg"`` to filter for those file types.
    """
    results: list[tuple[str, int]] = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        url = parts[2]
        if filename_fragment not in url:
            continue
        match = _FILE_URL_TS_RE.search(url)
        if not match:
            continue
        ts = int(match.group(1))
        results.append((url, ts))
    return results


# ---------------------------------------------------------------------------
# GdeltHttpClient
# ---------------------------------------------------------------------------


class GdeltHttpClient:
    """
    Async HTTP client for downloading and parsing GDELT v2 export files.

    Accepts an injected httpx.AsyncClient for testability. Use the
    ``create`` class method to build a production-ready instance.
    """

    def __init__(self, http_client: httpx.AsyncClient) -> None:
        self._http = http_client

    @classmethod
    def create(cls, timeout: float = 30.0) -> GdeltHttpClient:
        """Create a production GdeltHttpClient with a real httpx.AsyncClient.

        SSL verification is disabled because GDELT's CDN serves a certificate
        whose hostname does not match 'data.gdeltproject.org' (hostname mismatch).
        The connection is still encrypted — only peer identity verification is skipped.
        GDELT is a public read-only data source with no credentials involved.
        """
        client = httpx.AsyncClient(timeout=timeout, verify=False)
        return cls(http_client=client)

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._http.aclose()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_latest_export_url(self) -> tuple[str, int]:
        """
        Fetch lastupdate.txt and return the export file's (url, timestamp).

        Raises:
            IngestionError: On HTTP failure or if no export line is found.
        """
        text = await self._get_text(_LASTUPDATE_URL)
        export_lines = _parse_export_lines(text, filename_fragment="export")
        if not export_lines:
            raise IngestionError(
                "No export URL found in lastupdate.txt — GDELT may have changed its format."
            )
        # lastupdate.txt has exactly one export line (the first line)
        url, ts = export_lines[0]
        logger.info("gdelt_latest_export_url_fetched", url=url, timestamp=ts)
        return url, ts

    async def fetch_latest_mentions_url(self) -> tuple[str, int]:
        """
        Fetch lastupdate.txt and return the EVENTMENTIONS file's (url, timestamp).

        lastupdate.txt contains three lines:
            line 0 → events export
            line 1 → mentions export
            line 2 → gkg export

        Raises:
            IngestionError: On HTTP failure or if no mentions line is found.
        """
        text = await self._get_text(_LASTUPDATE_URL)
        mentions_lines = _parse_export_lines(text, filename_fragment="mentions")
        if not mentions_lines:
            raise IngestionError(
                "No mentions URL found in lastupdate.txt — GDELT may have changed its format."
            )
        url, ts = mentions_lines[0]
        logger.info("gdelt_latest_mentions_url_fetched", url=url, timestamp=ts)
        return url, ts

    async def fetch_latest_gkg_url(self) -> tuple[str, int]:
        """
        Fetch lastupdate.txt and return the GKG file's (url, timestamp).

        Raises:
            IngestionError: On HTTP failure or if no GKG line is found.
        """
        text = await self._get_text(_LASTUPDATE_URL)
        gkg_lines = _parse_export_lines(text, filename_fragment="gkg")
        if not gkg_lines:
            raise IngestionError(
                "No GKG URL found in lastupdate.txt — GDELT may have changed its format."
            )
        url, ts = gkg_lines[0]
        logger.info("gdelt_latest_gkg_url_fetched", url=url, timestamp=ts)
        return url, ts

    async def fetch_master_export_urls(
        self,
        since_ts: int,
        until_ts: int,
    ) -> list[tuple[str, int]]:
        """
        Fetch masterfilelist.txt and return export URLs within [since_ts, until_ts].

        Results are sorted by timestamp ascending (chronological order).

        Args:
            since_ts: Lower bound timestamp (inclusive), e.g. 20260301000000.
            until_ts: Upper bound timestamp (inclusive), e.g. 20260308235959.

        Returns:
            Sorted list of (url, timestamp_int) tuples.

        Raises:
            IngestionError: On HTTP failure.
        """
        text = await self._get_text(_MASTERFILELIST_URL)
        all_exports = _parse_export_lines(text, filename_fragment="export")
        filtered = [(url, ts) for url, ts in all_exports if since_ts <= ts <= until_ts]
        filtered.sort(key=lambda pair: pair[1])
        logger.info(
            "gdelt_master_export_urls_fetched",
            since_ts=since_ts,
            until_ts=until_ts,
            count=len(filtered),
        )
        return filtered

    async def fetch_master_mentions_urls(
        self,
        since_ts: int,
        until_ts: int,
    ) -> list[tuple[str, int]]:
        """
        Fetch masterfilelist.txt and return EVENTMENTIONS URLs within [since_ts, until_ts].

        Results are sorted by timestamp ascending (chronological order).
        """
        text = await self._get_text(_MASTERFILELIST_URL)
        all_mentions = _parse_export_lines(text, filename_fragment="mentions")
        filtered = [(url, ts) for url, ts in all_mentions if since_ts <= ts <= until_ts]
        filtered.sort(key=lambda pair: pair[1])
        logger.info(
            "gdelt_master_mentions_urls_fetched",
            since_ts=since_ts,
            until_ts=until_ts,
            count=len(filtered),
        )
        return filtered

    async def fetch_master_gkg_urls(
        self,
        since_ts: int,
        until_ts: int,
    ) -> list[tuple[str, int]]:
        """
        Fetch masterfilelist.txt and return GKG URLs within [since_ts, until_ts].

        Results are sorted by timestamp ascending (chronological order).
        """
        text = await self._get_text(_MASTERFILELIST_URL)
        all_gkg = _parse_export_lines(text, filename_fragment="gkg")
        filtered = [(url, ts) for url, ts in all_gkg if since_ts <= ts <= until_ts]
        filtered.sort(key=lambda pair: pair[1])
        logger.info(
            "gdelt_master_gkg_urls_fetched",
            since_ts=since_ts,
            until_ts=until_ts,
            count=len(filtered),
        )
        return filtered

    async def download_events(self, url: str) -> list[dict[str, Any]]:
        """
        Download a GDELT export ZIP and parse all valid rows.

        Short rows (fewer than 61 columns) are logged and skipped. Malformed
        individual field values are also logged and skipped.

        Args:
            url: Full HTTPS URL to a *.export.CSV.zip file.

        Returns:
            List of row dicts, each with the 17 mapped keys.

        Raises:
            IngestionError: On HTTP failure or unreadable ZIP archive.
        """
        content = await self._get_bytes(url)
        rows = _parse_zip_content(content, url, parse_gdelt_csv_row, _GDELT_COLUMN_COUNT)
        logger.info("gdelt_events_downloaded", url=url, row_count=len(rows))
        return rows

    async def download_mentions(self, url: str) -> list[dict[str, Any]]:
        """
        Download a GDELT EVENTMENTIONS ZIP and parse all valid rows.

        Args:
            url: Full HTTPS URL to a *.mentions.CSV.zip file.

        Returns:
            List of row dicts, each with the 8 mapped EVENTMENTIONS keys.

        Raises:
            IngestionError: On HTTP failure or unreadable ZIP archive.
        """
        content = await self._get_bytes(url)
        rows = _parse_zip_content(content, url, parse_gdelt_mentions_row, _MENTIONS_COLUMN_COUNT)
        logger.info("gdelt_mentions_downloaded", url=url, row_count=len(rows))
        return rows

    async def download_gkg(self, url: str) -> list[dict[str, Any]]:
        """
        Download a GDELT GKG ZIP and parse all valid rows.

        Args:
            url: Full HTTPS URL to a *.gkg.csv.zip file.

        Returns:
            List of row dicts, each with the 9 mapped GKG keys.

        Raises:
            IngestionError: On HTTP failure or unreadable ZIP archive.
        """
        content = await self._get_bytes(url)
        rows = _parse_zip_content(content, url, parse_gdelt_gkg_row, _GKG_MIN_COLS)
        logger.info("gdelt_gkg_downloaded", url=url, row_count=len(rows))
        return rows

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _get_text(self, url: str) -> str:
        """GET a URL and return the response body as text. Raises IngestionError on failure."""
        try:
            response = await self._http.get(url)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise IngestionError(f"HTTP error fetching {url}: {exc.response.status_code}") from exc
        except httpx.RequestError as exc:
            raise IngestionError(f"Network error fetching {url}: {exc}") from exc
        return response.text

    async def _get_bytes(self, url: str) -> bytes:
        """GET a URL and return the raw response bytes. Raises IngestionError on failure."""
        try:
            response = await self._http.get(url)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise IngestionError(f"HTTP error fetching {url}: {exc.response.status_code}") from exc
        except httpx.RequestError as exc:
            raise IngestionError(f"Network error fetching {url}: {exc}") from exc
        return response.content


# ---------------------------------------------------------------------------
# ZIP + CSV parsing (module-level to keep the class lean)
# ---------------------------------------------------------------------------


def _parse_zip_content(
    content: bytes,
    source_url: str,
    row_parser: Callable[[list[str]], dict[str, Any]],
    min_cols: int,
) -> list[dict[str, Any]]:
    """
    Extract and parse the CSV from any GDELT ZIP file using the provided row parser.

    Args:
        content: Raw ZIP bytes.
        source_url: Used only for error/warning messages.
        row_parser: A callable that maps a list[str] row to a dict.
                    It must return an empty dict {} for invalid rows.
        min_cols: Minimum column count required; short rows are logged and skipped.

    Returns:
        Parsed row dicts (empty-dict rows are excluded).

    Raises:
        IngestionError: If the bytes cannot be opened as a ZIP.
    """
    rows: list[dict[str, Any]] = []
    try:
        buf = io.BytesIO(content)
        with zipfile.ZipFile(buf) as zf:
            for name in zf.namelist():
                csv_bytes = zf.read(name)
                csv_text = csv_bytes.decode("utf-8", errors="replace")
                for line in csv_text.splitlines():
                    if not line.strip():
                        continue
                    cols = line.split("\t")
                    if len(cols) < min_cols:
                        logger.warning(
                            "gdelt_short_row_skipped",
                            column_count=len(cols),
                            source_url=source_url,
                        )
                        continue
                    try:
                        parsed = row_parser(cols)
                        if parsed:
                            rows.append(parsed)
                    except (ValueError, IndexError) as exc:
                        logger.warning(
                            "gdelt_malformed_row_skipped",
                            error=str(exc),
                            source_url=source_url,
                        )
    except zipfile.BadZipFile as exc:
        raise IngestionError(f"Downloaded file is not a valid ZIP: {source_url}") from exc
    return rows
