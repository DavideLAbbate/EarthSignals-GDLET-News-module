"""
Tests for GdeltHttpClient, parse_gdelt_csv_row, parse_gdelt_mentions_row,
and parse_gdelt_gkg_row.

Covers CSV row parsing, ZIP download/parsing, lastupdate.txt parsing,
and masterfilelist.txt filtering for all three GDELT file types
(events, EVENTMENTIONS, GKG).

All HTTP calls are mocked via AsyncMock; ZIP bytes are built in-memory.
"""

from __future__ import annotations

import io
import zipfile
from unittest.mock import AsyncMock, MagicMock

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_row(overrides: dict[int, str] | None = None) -> list[str]:
    """Build a 61-column row with sensible defaults, applying any overrides."""
    row = [""] * 61
    row[0] = "123456789"  # GLOBALEVENTID
    row[1] = "20260308"  # SQLDATE
    row[7] = "US"  # Actor1CountryCode
    row[17] = "RU"  # Actor2CountryCode
    row[26] = "0411"  # EventCode
    row[27] = "04"  # EventBaseCode
    row[28] = "04"  # EventRootCode
    row[29] = "2"  # QuadClass
    row[30] = "-2.5"  # GoldsteinScale
    row[31] = "3"  # NumMentions
    row[32] = "2"  # NumSources
    row[33] = "3"  # NumArticles
    row[34] = "1.75"  # AvgTone
    row[52] = "Washington, DC"  # ActionGeo_FullName
    row[53] = "US"  # ActionGeo_CountryCode
    row[59] = "20260308120000"  # DATEADDED
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

    def test_parse_gdelt_csv_row_maps_real_gdelt_sample_row(self):
        """A real GDELT export row maps action geo and DATEADDED correctly."""
        from app.integrations.gdelt_http_client import parse_gdelt_csv_row

        row = [""] * 61
        row[0] = "1293127183"
        row[1] = "20250308"
        row[15] = "AGR"
        row[16] = "FARMER"
        row[22] = "AGR"
        row[25] = "1"
        row[26] = "100"
        row[27] = "100"
        row[28] = "10"
        row[29] = "3"
        row[30] = "-5.0"
        row[31] = "5"
        row[32] = "1"
        row[33] = "5"
        row[34] = "-2.11480362537765"
        row[51] = "1"
        row[52] = "Canada"
        row[53] = "CA"
        row[54] = "CA"
        row[56] = "60"
        row[57] = "-96"
        row[58] = "CA"
        row[59] = "20260308181500"
        row[60] = (
            "https://www.cbc.ca/news/canada/nova-scotia/n-s-diesel-price-increases-pressure-farming-trucking-sectors-9.7119581"
        )

        result = parse_gdelt_csv_row(row)

        assert result["GLOBALEVENTID"] == 1293127183
        assert result["SQLDATE"] == 20250308
        assert result["EventCode"] == "100"
        assert result["ActionGeo_FullName"] == "Canada"
        assert result["ActionGeo_CountryCode"] == "CA"
        assert result["DATEADDED"] == 20260308181500
        assert result["SOURCEURL"].startswith("https://www.cbc.ca/")

    def test_parse_gdelt_csv_row_handles_empty_optional_string_fields(self):
        """Empty strings for optional string columns become None."""
        from app.integrations.gdelt_http_client import parse_gdelt_csv_row

        row = _make_row({7: "", 17: "", 52: "", 53: "", 60: ""})
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
            "1194680 abc123def456abc123def456abc123de "
            "http://data.gdeltproject.org/gdeltv2/20260308120000.export.CSV.zip\n"
            "1194680 abc123def456abc123def456abc123df "
            "http://data.gdeltproject.org/gdeltv2/20260308120000.mentions.CSV.zip\n"
            "1194680 abc123def456abc123def456abc123d0 "
            "http://data.gdeltproject.org/gdeltv2/20260308120000.gkg.csv.zip\n"
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
        "1000 hash1 http://data.gdeltproject.org/gdeltv2/20260301000000.export.CSV.zip\n"
        "1000 hash2 http://data.gdeltproject.org/gdeltv2/20260301000000.mentions.CSV.zip\n"
        "1000 hash3 http://data.gdeltproject.org/gdeltv2/20260301001500.export.CSV.zip\n"
        "1000 hash4 http://data.gdeltproject.org/gdeltv2/20260305000000.export.CSV.zip\n"
        "1000 hash5 http://data.gdeltproject.org/gdeltv2/20260305000000.gkg.csv.zip\n"
        "1000 hash6 http://data.gdeltproject.org/gdeltv2/20260308000000.export.CSV.zip\n"
        "1000 hash7 http://data.gdeltproject.org/gdeltv2/20260309000000.export.CSV.zip\n"
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


# ---------------------------------------------------------------------------
# parse_gdelt_mentions_row
# ---------------------------------------------------------------------------


class TestParseGdeltMentionsRow:
    def test_parse_gdelt_mentions_row_maps_fields(self):
        """All 8 relevant EVENTMENTIONS fields are mapped correctly."""
        from app.integrations.gdelt_http_client import parse_gdelt_mentions_row

        row = [
            "999",  # 0 GLOBALEVENTID
            "20260310120000",  # 1 EventTimeDate
            "20260310120500",  # 2 MentionTimeDate
            "1",  # 3 MentionType (WEB)
            "example.com",  # 4 MentionSourceName
            "https://example.com/article",  # 5 MentionIdentifier
            "0",  # 6 SentenceAlid
            "0",  # 7 Actor1CharOffset
            "0",  # 8 Actor2CharOffset
            "0",  # 9 ActionCharOffset
            "1",  # 10 InRawText
            "90",  # 11 Confidence
            "450",  # 12 MentionDocLen
            "-5.2",  # 13 MentionDocTone
        ]
        result = parse_gdelt_mentions_row(row)

        assert result["GLOBALEVENTID"] == 999
        assert result["EventTimeDate"] == 20260310120000
        assert result["MentionTimeDate"] == 20260310120500
        assert result["MentionType"] == 1
        assert result["MentionSourceName"] == "example.com"
        assert result["MentionIdentifier"] == "https://example.com/article"
        assert result["MentionDocLen"] == 450
        assert result["MentionDocTone"] == pytest.approx(-5.2)

    def test_parse_gdelt_mentions_row_short_row_returns_empty(self):
        """Rows with fewer than 14 columns return an empty dict."""
        from app.integrations.gdelt_http_client import parse_gdelt_mentions_row

        assert parse_gdelt_mentions_row(["1", "2"]) == {}
        assert parse_gdelt_mentions_row([]) == {}

    def test_parse_gdelt_mentions_row_empty_fields_become_none(self):
        """Empty string fields map to None for optional columns."""
        from app.integrations.gdelt_http_client import parse_gdelt_mentions_row

        row = ["1000", "", "", "", "", "", "", "", "", "", "", "", "", ""]
        result = parse_gdelt_mentions_row(row)
        assert result["MentionSourceName"] is None
        assert result["MentionIdentifier"] is None
        assert result["MentionDocTone"] is None


# ---------------------------------------------------------------------------
# parse_gdelt_gkg_row
# ---------------------------------------------------------------------------


class TestParseGdeltGkgRow:
    def _make_gkg_row(self) -> list[str]:
        """Build a minimal 27-column GKG row with test data."""
        row = [""] * 27
        row[0] = "20260310-1"  # GKGRECORDID
        row[1] = "20260310120000"  # DATE
        row[3] = "fnnews.com"  # SourceCommonName
        row[4] = "https://fnnews.com/article"  # DocumentIdentifier
        row[7] = "ARMEDCONFLICT;IRAN;MILITARY"  # V1Themes
        row[9] = "Tehran, Tehran, Iran#IR#0#35.6#51.4"  # V1Locations
        row[11] = "Mojtaba Khamenei"  # V1Persons
        row[13] = "IRGC"  # V1Organizations
        row[15] = "-8.7,1.2,2.3,4.5,5.6,6.7"  # V2Tone (avg tone is first value)
        return row

    def test_parse_gdelt_gkg_row_maps_all_fields(self):
        """All 9 GKG fields are mapped correctly, including tone parsing."""
        from app.integrations.gdelt_http_client import parse_gdelt_gkg_row

        result = parse_gdelt_gkg_row(self._make_gkg_row())

        assert result["GKGRECORDID"] == "20260310-1"
        assert result["DATE"] == 20260310120000
        assert result["SourceCommonName"] == "fnnews.com"
        assert result["DocumentIdentifier"] == "https://fnnews.com/article"
        assert "ARMEDCONFLICT" in result["V1Themes"]
        assert "IRAN" in result["V1Themes"]
        assert result["AvgTone"] == pytest.approx(-8.7)
        assert "Mojtaba Khamenei" in result["V1Persons"]
        assert "IRGC" in result["V1Organizations"]

    def test_parse_gdelt_gkg_row_short_row_returns_empty(self):
        """Rows with fewer than 16 columns return an empty dict."""
        from app.integrations.gdelt_http_client import parse_gdelt_gkg_row

        assert parse_gdelt_gkg_row(["1", "2"]) == {}
        assert parse_gdelt_gkg_row([]) == {}

    def test_parse_gdelt_gkg_row_semicolon_themes_split_correctly(self):
        """Semicolon-separated themes are split into a list."""
        from app.integrations.gdelt_http_client import parse_gdelt_gkg_row

        row = self._make_gkg_row()
        row[7] = "WAR;PEACE;ECONOMY"
        result = parse_gdelt_gkg_row(row)
        assert result["V1Themes"] == ["WAR", "PEACE", "ECONOMY"]

    def test_parse_gdelt_gkg_row_empty_tone_returns_none(self):
        """An empty V2TONE field results in AvgTone = None."""
        from app.integrations.gdelt_http_client import parse_gdelt_gkg_row

        row = self._make_gkg_row()
        row[15] = ""
        result = parse_gdelt_gkg_row(row)
        assert result["AvgTone"] is None

    def test_parse_gdelt_gkg_row_empty_semicolon_fields_return_empty_list(self):
        """Empty semicolon-delimited fields return empty lists."""
        from app.integrations.gdelt_http_client import parse_gdelt_gkg_row

        row = self._make_gkg_row()
        row[7] = ""  # themes
        row[11] = ""  # persons
        row[13] = ""  # organizations
        result = parse_gdelt_gkg_row(row)
        assert result["V1Themes"] == []
        assert result["V1Persons"] == []
        assert result["V1Organizations"] == []


# ---------------------------------------------------------------------------
# GdeltHttpClient.fetch_latest_mentions_url / fetch_latest_gkg_url
# ---------------------------------------------------------------------------


class TestFetchLatestMentionsAndGkgUrl:
    _LASTUPDATE_TEXT = (
        "1194680 abc123 "
        "http://data.gdeltproject.org/gdeltv2/20260310120000.export.CSV.zip\n"
        "1194680 def456 "
        "http://data.gdeltproject.org/gdeltv2/20260310120000.mentions.CSV.zip\n"
        "1194680 ghi789 "
        "http://data.gdeltproject.org/gdeltv2/20260310120000.gkg.csv.zip\n"
    )

    def _make_client(self) -> object:
        from app.integrations.gdelt_http_client import GdeltHttpClient

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.text = self._LASTUPDATE_TEXT

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_response)

        return GdeltHttpClient(http_client=mock_http)

    async def test_fetch_latest_mentions_url_returns_mentions_url(self):
        """fetch_latest_mentions_url returns a URL containing 'mentions'."""
        client = self._make_client()
        url, ts = await client.fetch_latest_mentions_url()
        assert "mentions" in url
        assert ts == 20260310120000

    async def test_fetch_latest_gkg_url_returns_gkg_url(self):
        """fetch_latest_gkg_url returns a URL containing 'gkg'."""
        client = self._make_client()
        url, ts = await client.fetch_latest_gkg_url()
        assert "gkg" in url
        assert ts == 20260310120000

    async def test_fetch_latest_export_url_still_returns_export_url(self):
        """Existing fetch_latest_export_url still returns only the export line."""
        client = self._make_client()
        url, ts = await client.fetch_latest_export_url()
        assert ".export." in url
        assert ts == 20260310120000


# ---------------------------------------------------------------------------
# GdeltHttpClient.fetch_master_mentions_urls / fetch_master_gkg_urls
# ---------------------------------------------------------------------------


class TestFetchMasterMentionsAndGkgUrls:
    _MASTER_TEXT = (
        "1000 h1 http://data.gdeltproject.org/gdeltv2/20260301000000.export.CSV.zip\n"
        "1000 h2 http://data.gdeltproject.org/gdeltv2/20260301000000.mentions.CSV.zip\n"
        "1000 h3 http://data.gdeltproject.org/gdeltv2/20260301000000.gkg.csv.zip\n"
        "1000 h4 http://data.gdeltproject.org/gdeltv2/20260308000000.export.CSV.zip\n"
        "1000 h5 http://data.gdeltproject.org/gdeltv2/20260308000000.mentions.CSV.zip\n"
        "1000 h6 http://data.gdeltproject.org/gdeltv2/20260308000000.gkg.csv.zip\n"
    )

    def _make_client(self) -> object:
        from app.integrations.gdelt_http_client import GdeltHttpClient

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.text = self._MASTER_TEXT

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_response)

        return GdeltHttpClient(http_client=mock_http)

    async def test_fetch_master_mentions_urls_returns_only_mentions(self):
        """fetch_master_mentions_urls returns only .mentions. URLs, sorted by timestamp."""
        client = self._make_client()
        results = await client.fetch_master_mentions_urls(
            since_ts=20260301000000, until_ts=20260309000000
        )
        urls = [url for url, _ in results]
        assert all("mentions" in url for url in urls)
        assert not any("export" in url for url in urls)
        timestamps = [ts for _, ts in results]
        assert timestamps == sorted(timestamps)

    async def test_fetch_master_gkg_urls_returns_only_gkg(self):
        """fetch_master_gkg_urls returns only .gkg. URLs, sorted by timestamp."""
        client = self._make_client()
        results = await client.fetch_master_gkg_urls(
            since_ts=20260301000000, until_ts=20260309000000
        )
        urls = [url for url, _ in results]
        assert all("gkg" in url for url in urls)
        assert not any("export" in url for url in urls)


# ---------------------------------------------------------------------------
# GdeltHttpClient.download_mentions / download_gkg
# ---------------------------------------------------------------------------


class TestDownloadMentionsAndGkg:
    def _make_mentions_zip(self) -> bytes:
        """Build an in-memory ZIP with a minimal EVENTMENTIONS CSV."""
        row = "\t".join(
            [
                "1001",
                "20260310120000",
                "20260310120500",
                "1",
                "example.com",
                "https://example.com/a",
                "0",
                "0",
                "0",
                "0",
                "1",
                "90",
                "350",
                "-3.5",
            ]
        )
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, mode="w") as zf:
            zf.writestr("20260310120000.mentions.CSV", row + "\n")
        return buf.getvalue()

    def _make_gkg_zip(self) -> bytes:
        """Build an in-memory ZIP with a minimal GKG CSV (27 columns)."""
        cols = [
            "20260310-1",
            "20260310120000",
            "1",
            "example.com",
            "https://example.com/a",
            "",
            "",
            "WAR;PEACE",
            "",
            "Tehran#IR",
            "",
            "Alice",
            "",
            "IRGC",
            "",
            "-7.0,1.0,2.0",
            "",
        ] + [""] * 10
        row = "\t".join(cols)
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, mode="w") as zf:
            zf.writestr("20260310120000.gkg.csv", row + "\n")
        return buf.getvalue()

    def _make_client(self, response_bytes: bytes) -> object:
        from app.integrations.gdelt_http_client import GdeltHttpClient

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.content = response_bytes

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_response)

        return GdeltHttpClient(http_client=mock_http)

    async def test_download_mentions_returns_parsed_rows(self):
        """download_mentions returns parsed EVENTMENTIONS dicts."""
        client = self._make_client(self._make_mentions_zip())
        rows = await client.download_mentions(
            "https://data.gdeltproject.org/gdeltv2/20260310120000.mentions.CSV.zip"
        )
        assert len(rows) == 1
        assert rows[0]["GLOBALEVENTID"] == 1001
        assert rows[0]["MentionIdentifier"] == "https://example.com/a"
        assert rows[0]["MentionDocTone"] == pytest.approx(-3.5)

    async def test_download_gkg_returns_parsed_rows(self):
        """download_gkg returns parsed GKG dicts."""
        client = self._make_client(self._make_gkg_zip())
        rows = await client.download_gkg(
            "https://data.gdeltproject.org/gdeltv2/20260310120000.gkg.csv.zip"
        )
        assert len(rows) == 1
        assert rows[0]["DocumentIdentifier"] == "https://example.com/a"
        assert "WAR" in rows[0]["V1Themes"]
        assert rows[0]["AvgTone"] == pytest.approx(-7.0)
