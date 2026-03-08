"""
Tests for GdeltHttpClient and parse_gdelt_csv_row.

Covers CSV row parsing, ZIP download/parsing, lastupdate.txt parsing,
and masterfilelist.txt filtering.

All HTTP calls are mocked via AsyncMock; ZIP bytes are built in-memory.
"""

from __future__ import annotations

import io
import zipfile
from unittest.mock import AsyncMock, MagicMock


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_row(overrides: dict[int, str] | None = None) -> list[str]:
    """Build a 61-column row with sensible defaults, applying any overrides."""
    row = [""] * 61
    row[0] = "123456789"  # GLOBALEVENTID
    row[1] = "20260308"  # SQLDATE
    row[5] = "US"  # Actor1CountryCode
    row[15] = "RU"  # Actor2CountryCode
    row[26] = "0411"  # EventCode
    row[27] = "04"  # EventBaseCode
    row[28] = "04"  # EventRootCode
    row[29] = "2"  # QuadClass
    row[30] = "-2.5"  # GoldsteinScale
    row[31] = "3"  # NumMentions
    row[32] = "2"  # NumSources
    row[33] = "3"  # NumArticles
    row[34] = "1.75"  # AvgTone
    row[51] = "Washington, DC"  # ActionGeo_FullName
    row[53] = "US"  # ActionGeo_CountryCode
    row[57] = "20260308120000"  # DATEADDED
    row[60] = "https://example.com/article"  # SOURCEURL
    if overrides:
        for idx, val in overrides.items():
            row[idx] = val
    return row


def _make_zip_bytes(csv_content: str) -> bytes:
    """Wrap a CSV string in an in-memory ZIP file, mimicking GDELT's format."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("20260308120000.export.CSV", csv_content)
    return buf.getvalue()


def _row_to_tsv(row: list[str]) -> str:
    return "\t".join(row)


# ---------------------------------------------------------------------------
# parse_gdelt_csv_row
# ---------------------------------------------------------------------------


class TestParseGdeltCsvRow:
    def test_parse_gdelt_csv_row_extracts_correct_columns(self):
        """All 17 mapped columns are extracted with correct types."""
        from app.integrations.gdelt_http_client import parse_gdelt_csv_row

        row = _make_row()
        result = parse_gdelt_csv_row(row)

        assert result["GLOBALEVENTID"] == 123456789
        assert result["SQLDATE"] == 20260308
        assert result["Actor1CountryCode"] == "US"
        assert result["Actor2CountryCode"] == "RU"
        assert result["EventCode"] == "0411"
        assert result["EventBaseCode"] == "04"
        assert result["EventRootCode"] == "04"
        assert result["QuadClass"] == 2
        assert result["GoldsteinScale"] == -2.5
        assert result["NumMentions"] == 3
        assert result["NumSources"] == 2
        assert result["NumArticles"] == 3
        assert result["AvgTone"] == 1.75
        assert result["ActionGeo_FullName"] == "Washington, DC"
        assert result["ActionGeo_CountryCode"] == "US"
        assert result["DATEADDED"] == 20260308120000
        assert result["SOURCEURL"] == "https://example.com/article"

    def test_parse_gdelt_csv_row_handles_empty_optional_string_fields(self):
        """Empty strings for optional string columns become None."""
        from app.integrations.gdelt_http_client import parse_gdelt_csv_row

        row = _make_row({5: "", 15: "", 51: "", 53: "", 60: ""})
        result = parse_gdelt_csv_row(row)

        assert result["Actor1CountryCode"] is None
        assert result["Actor2CountryCode"] is None
        assert result["ActionGeo_FullName"] is None
        assert result["ActionGeo_CountryCode"] is None
        assert result["SOURCEURL"] is None

    def test_parse_gdelt_csv_row_handles_empty_numeric_fields(self):
        """Empty strings for optional numeric columns become None."""
        from app.integrations.gdelt_http_client import parse_gdelt_csv_row

        row = _make_row({26: "", 27: "", 28: "", 29: "", 30: "", 31: "", 32: "", 33: "", 34: ""})
        result = parse_gdelt_csv_row(row)

        assert result["EventCode"] is None
        assert result["EventBaseCode"] is None
        assert result["EventRootCode"] is None
        assert result["QuadClass"] is None
        assert result["GoldsteinScale"] is None
        assert result["NumMentions"] is None
        assert result["NumSources"] is None
        assert result["NumArticles"] is None
        assert result["AvgTone"] is None


# ---------------------------------------------------------------------------
# GdeltHttpClient.download_events
# ---------------------------------------------------------------------------


class TestDownloadEvents:
    def _make_client(self, response_bytes: bytes) -> object:
        """Build a GdeltHttpClient with a mocked httpx.AsyncClient."""
        from app.integrations.gdelt_http_client import GdeltHttpClient

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.content = response_bytes

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_response)

        return GdeltHttpClient(http_client=mock_http)

    async def test_download_events_returns_parsed_rows(self):
        """Valid ZIP with two data rows returns two parsed dicts."""

        row1 = _make_row({0: "111", 57: "20260308120000"})
        row2 = _make_row({0: "222", 57: "20260308120000"})
        csv_content = "\n".join([_row_to_tsv(row1), _row_to_tsv(row2)])
        zip_bytes = _make_zip_bytes(csv_content)

        client = self._make_client(zip_bytes)
        rows = await client.download_events(
            "https://data.gdeltproject.org/gdeltv2/20260308120000.export.CSV.zip"
        )

        assert len(rows) == 2
        assert rows[0]["GLOBALEVENTID"] == 111
        assert rows[1]["GLOBALEVENTID"] == 222

    async def test_download_events_skips_short_rows(self):
        """Rows with fewer than 61 columns are silently skipped."""

        good_row = _make_row({0: "999"})
        short_row = ["col1", "col2", "col3"]  # only 3 columns
        csv_content = "\n".join([_row_to_tsv(good_row), "\t".join(short_row)])
        zip_bytes = _make_zip_bytes(csv_content)

        client = self._make_client(zip_bytes)
        rows = await client.download_events(
            "https://data.gdeltproject.org/gdeltv2/20260308120000.export.CSV.zip"
        )

        assert len(rows) == 1
        assert rows[0]["GLOBALEVENTID"] == 999


# ---------------------------------------------------------------------------
# GdeltHttpClient.fetch_latest_export_url
# ---------------------------------------------------------------------------


class TestFetchLatestExportUrl:
    async def test_fetch_latest_export_url_parses_lastupdate(self):
        """Parses lastupdate.txt and returns (url, timestamp_int) for the export line."""
        from app.integrations.gdelt_http_client import GdeltHttpClient

        lastupdate_text = (
            "1194680 http://data.gdeltproject.org/gdeltv2/20260308120000.export.CSV.zip "
            "abc123def456abc123def456abc123de\n"
            "1194680 http://data.gdeltproject.org/gdeltv2/20260308120000.mentions.CSV.zip "
            "abc123def456abc123def456abc123df\n"
            "1194680 http://data.gdeltproject.org/gdeltv2/20260308120000.gkg.csv.zip "
            "abc123def456abc123def456abc123d0\n"
        )

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.text = lastupdate_text

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_response)

        client = GdeltHttpClient(http_client=mock_http)
        url, ts = await client.fetch_latest_export_url()

        assert ".export." in url
        assert ts == 20260308120000


# ---------------------------------------------------------------------------
# GdeltHttpClient.fetch_master_export_urls
# ---------------------------------------------------------------------------


class TestFetchMasterExportUrls:
    _MASTER_TEXT = (
        "1000 http://data.gdeltproject.org/gdeltv2/20260301000000.export.CSV.zip hash1\n"
        "1000 http://data.gdeltproject.org/gdeltv2/20260301000000.mentions.CSV.zip hash2\n"
        "1000 http://data.gdeltproject.org/gdeltv2/20260301001500.export.CSV.zip hash3\n"
        "1000 http://data.gdeltproject.org/gdeltv2/20260305000000.export.CSV.zip hash4\n"
        "1000 http://data.gdeltproject.org/gdeltv2/20260305000000.gkg.csv.zip hash5\n"
        "1000 http://data.gdeltproject.org/gdeltv2/20260308000000.export.CSV.zip hash6\n"
        "1000 http://data.gdeltproject.org/gdeltv2/20260309000000.export.CSV.zip hash7\n"
    )

    def _make_client(self) -> object:
        from app.integrations.gdelt_http_client import GdeltHttpClient

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.text = self._MASTER_TEXT

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_response)

        return GdeltHttpClient(http_client=mock_http)

    async def test_fetch_master_export_urls_filters_by_since(self):
        """Only returns export URLs with timestamp >= since_ts."""
        client = self._make_client()
        results = await client.fetch_master_export_urls(
            since_ts=20260305000000,
            until_ts=20260309000000,
        )

        timestamps = [ts for _, ts in results]
        assert all(ts >= 20260305000000 for ts in timestamps)
        assert all(ts <= 20260309000000 for ts in timestamps)

    async def test_fetch_master_export_urls_excludes_non_export_files(self):
        """mentions and gkg lines are excluded; only .export. URLs returned."""
        client = self._make_client()
        results = await client.fetch_master_export_urls(
            since_ts=20260301000000,
            until_ts=20260309000000,
        )

        urls = [url for url, _ in results]
        assert all(".export." in url for url in urls)
        assert not any(".mentions." in url for url in urls)
        assert not any(".gkg." in url for url in urls)

    async def test_fetch_master_export_urls_returns_chronological_order(self):
        """Returned list is sorted by timestamp ascending."""
        client = self._make_client()
        results = await client.fetch_master_export_urls(
            since_ts=20260301000000,
            until_ts=20260309000000,
        )

        timestamps = [ts for _, ts in results]
        assert timestamps == sorted(timestamps)
