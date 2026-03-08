# Design: Replace BigQuery Ingestion with GDELT HTTP

**Date:** 2026-03-08  
**Status:** Approved

## Problem

The ingestion layer (`run_bootstrap`, `run_incremental`) currently fetches GDELT events from
Google BigQuery, which requires GCP credentials, incurs per-byte scan costs, and adds a
heavyweight dependency. GDELT publishes the same raw event data as compressed CSV files over
plain HTTP every 15 minutes — no credentials required.

## Goal

Replace BigQuery as the ingestion data source with GDELT HTTP files. The rest of the stack
(PostgreSQL local store, query service, BigQuery for live user queries, Anthropic, scheduler
wiring) stays unchanged.

## GDELT HTTP API

- **Latest 3 files:** `http://data.gdeltproject.org/gdeltv2/lastupdate.txt`  
  One line per file type (`export`, `mentions`, `gkg`). We only need `export`.
- **Historical file list:** `http://data.gdeltproject.org/gdeltv2/masterfilelist.txt`  
  ~50k lines, one per 15-min export file since 2015. Used for bootstrap.
- **File format:** `<url> <size> <md5>` per line.
- **Data files:** ZIP archives containing a single tab-separated CSV with 61 columns.
- **Update cadence:** every 15 minutes, aligned to clock boundaries.

## Architecture

### New: `app/integrations/gdelt_http_client.py`

An async `GdeltHttpClient` wrapping `httpx.AsyncClient`:

```
GdeltHttpClient
  ├── fetch_latest_file_url() -> str          # parse lastupdate.txt, return export URL
  ├── fetch_master_file_urls(since, until) -> list[str]   # filter masterfilelist.txt by date
  └── download_events(url: str) -> list[dict] # download ZIP, parse CSV, return row dicts
```

Column mapping: the 61-column CSV uses positional columns (no header). We map only the 17
columns already used by `_row_to_event_dict`. The output dict keys match the existing BigQuery
row format exactly so `_row_to_event_dict` needs no changes.

### Modified: `app/services/ingestion_service.py`

- `run_bootstrap(session)` — removes `bq_client` parameter; uses `GdeltHttpClient` internally
- `run_incremental(session)` — removes `bq_client` parameter; uses `GdeltHttpClient` internally
- Watermark stays as `DATEADDED` integer (derived from filename timestamp, same format)
- `_iter_bootstrap_windows` / `_iter_incremental_windows` replaced with file-list filtering
  (simpler: filter `masterfilelist.txt` URLs by timestamp >= watermark)

### Modified: `app/scheduler/scheduler.py`

- `add_sync_job`, `run_ingestion_job`, `trigger_startup_ingestion_if_needed` — drop `bq_client`
  argument from ingestion calls

### Modified: `app/main.py`

- `trigger_startup_ingestion_if_needed(bq_client)` → `trigger_startup_ingestion_if_needed()`

### No changes needed

- `app/integrations/bigquery_client.py` — still used by query service for live user queries
- `app/integrations/gdelt_query_builder.py` — still used for live queries
- `app/db/` — no schema changes; `watermark_dateadded` column reused
- `app/core/exceptions.py` — `IngestionError` already exists and is appropriate

## Data Flow

### Bootstrap

```
masterfilelist.txt
  → filter lines where file timestamp >= (now - retention_days)
  → for each file URL (chronological order):
      download ZIP → decompress in-memory → parse TSV
      → filter rows to 17 columns → _row_to_event_dict
      → bulk_insert_events → update watermark
```

### Incremental

```
lastupdate.txt
  → extract export file URL
  → derive timestamp from filename
  → if timestamp > watermark: download ZIP → parse → insert → update watermark
  → else: no-op (already up to date)
```

## Error Handling

- HTTP errors / timeouts: raise `IngestionError` (already exists)
- Corrupt/empty ZIP: log + skip file, continue ingestion
- GDELT unavailable (503): retry 3× with exponential backoff before raising

## Dependencies

Add to `pyproject.toml`:
- `httpx` — already a dev dependency; promote to runtime

No new dependencies beyond `httpx`.

## Testing

- Replace `mock_bq_client` in ingestion tests with `mock_gdelt_http_client`
- Fixture provides in-memory TSV bytes to simulate downloaded files
- Test: bootstrap downloads correct date range of files
- Test: incremental skips already-ingested files (watermark check)
- Test: corrupt ZIP is skipped, ingestion continues
- Tests for `_row_to_event_dict` remain unchanged (same output format)
