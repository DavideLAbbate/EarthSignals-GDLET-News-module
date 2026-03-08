"""
GDELT HTTP client for direct CSV file ingestion.

Downloads and parses GDELT v2 export ZIP files from the public GDELT file
server. Replaces the BigQuery path for event ingestion by fetching the
compressed CSVs that GDELT publishes every 15 minutes.

Key endpoints:
- lastupdate.txt  — 3 lines, one per file type; format: <size> <url> <md5>
- masterfilelist.txt — full historical archive, same format per line
- *.export.CSV.zip — 61-column tab-separated CSV with no header row
"""

from __future__ import annotations

import io
import re
import zipfile
from typing import Any

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

# Regex to extract the timestamp from an export URL like:
#   .../20260308120000.export.CSV.zip
_EXPORT_URL_TS_RE = re.compile(r"/(\d{14})\.export\.", re.IGNORECASE)


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
        "Actor1CountryCode": _str_or_none(row[5]),
        "Actor2CountryCode": _str_or_none(row[15]),
        "EventCode": _str_or_none(row[26]),
        "EventBaseCode": _str_or_none(row[27]),
        "EventRootCode": _str_or_none(row[28]),
        "QuadClass": _int_or_none(row[29]),
        "GoldsteinScale": _float_or_none(row[30]),
        "NumMentions": _int_or_none(row[31]),
        "NumSources": _int_or_none(row[32]),
        "NumArticles": _int_or_none(row[33]),
        "AvgTone": _float_or_none(row[34]),
        "ActionGeo_FullName": _str_or_none(row[51]),
        "ActionGeo_CountryCode": _str_or_none(row[53]),
        "DATEADDED": int(row[57]),
        "SOURCEURL": _str_or_none(row[60]),
    }


# ---------------------------------------------------------------------------
# lastupdate.txt / masterfilelist.txt parsing
# ---------------------------------------------------------------------------


def _parse_export_lines(text: str) -> list[tuple[str, int]]:
    """
    Parse lines from lastupdate.txt or masterfilelist.txt.

    Each line has the format: <size> <md5> <url>
    Returns a list of (url, timestamp_int) tuples for export files only,
    preserving the original line order.
    """
    results: list[tuple[str, int]] = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        url = parts[2]
        match = _EXPORT_URL_TS_RE.search(url)
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
        export_lines = _parse_export_lines(text)
        if not export_lines:
            raise IngestionError(
                "No export URL found in lastupdate.txt — GDELT may have changed its format."
            )
        # lastupdate.txt has exactly one export line (the first line)
        url, ts = export_lines[0]
        logger.info("gdelt_latest_export_url_fetched", url=url, timestamp=ts)
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
        all_exports = _parse_export_lines(text)
        filtered = [(url, ts) for url, ts in all_exports if since_ts <= ts <= until_ts]
        filtered.sort(key=lambda pair: pair[1])
        logger.info(
            "gdelt_master_export_urls_fetched",
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
        rows = _parse_zip_content(content, url)
        logger.info("gdelt_events_downloaded", url=url, row_count=len(rows))
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


def _parse_zip_content(content: bytes, source_url: str) -> list[dict[str, Any]]:
    """
    Extract and parse the CSV from a GDELT export ZIP.

    Args:
        content: Raw ZIP bytes.
        source_url: Used only for error messages.

    Returns:
        Parsed row dicts.

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
                    if len(cols) < _GDELT_COLUMN_COUNT:
                        logger.warning(
                            "gdelt_short_row_skipped",
                            column_count=len(cols),
                            source_url=source_url,
                        )
                        continue
                    try:
                        rows.append(parse_gdelt_csv_row(cols))
                    except (ValueError, IndexError) as exc:
                        logger.warning(
                            "gdelt_malformed_row_skipped",
                            error=str(exc),
                            source_url=source_url,
                        )
    except zipfile.BadZipFile as exc:
        raise IngestionError(f"Downloaded file is not a valid ZIP: {source_url}") from exc
    return rows
