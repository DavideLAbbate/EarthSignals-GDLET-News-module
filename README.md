# GDELT News Backend

A production-grade REST API that queries the [GDELT 2.0](https://www.gdeltproject.org/) global event dataset via Google BigQuery, uses Claude (Anthropic) to interpret natural-language news filters, and caches results in PostgreSQL. Designed to be consumed by a frontend news journal application.

---

## How it works

```
Frontend request
      │
      ▼
POST /events/search  {"country": "Italy", "event_type": "protest"}
      │
      ▼
Claude (Anthropic) ──► normalizes to CAMEO/FIPS codes + date range
      │
      ▼
Google BigQuery ──► queries gdelt-bq.gdeltv2.events (public dataset)
      │
      ▼
PostgreSQL ──► caches normalized filters (avoids redundant Claude calls)
      │
      ▼
Structured JSON response with events, scores, sources
```

A background scheduler syncs GDELT metadata (top countries, event types) every 15 minutes.

---

## Tech stack

| Layer | Technology |
|---|---|
| Web framework | FastAPI 0.115 + Uvicorn |
| LLM | Anthropic Claude (claude-opus-4-5) |
| Data source | GDELT 2.0 via Google BigQuery |
| Database | PostgreSQL 16 (async via asyncpg + SQLAlchemy 2.0) |
| Migrations | Alembic |
| Scheduler | APScheduler 3.10 |
| Logging | structlog (JSON in production) |
| Config | pydantic-settings (reads `.env`) |
| Auth | `X-API-Key` header |
| Rate limiting | slowapi |
| Containerization | Docker + Docker Compose |

---

## Prerequisites

- Docker and Docker Compose
- A [GCP service account](https://console.cloud.google.com/iam-admin/serviceaccounts) key JSON with BigQuery read access on the `gdelt-bq` public dataset
- An [Anthropic API key](https://console.anthropic.com/)

---

## Setup

**1. Clone and configure environment**

```bash
git clone <repo-url>
cd web-journal-news-module
cp .env.example .env
```

Edit `.env` and fill in your real values:

```env
GOOGLE_APPLICATION_CREDENTIALS=/run/secrets/gcp-key.json   # path inside container, do not change
GCP_PROJECT_ID=your-gcp-project-id
ANTHROPIC_API_KEY=sk-ant-...
DATABASE_URL=postgresql+asyncpg://gdelt_user:changeme@db:5432/gdelt_db
API_KEY=your-secret-api-key                                 # used by the frontend as X-API-Key
CORS_ORIGINS=["http://localhost:3000"]
```

**2. Place your GCP service account key**

```
bigQuery/
└── web-journal-news-<hash>.json   ← your downloaded GCP key file
```

Update `docker-compose.yml` volume mount if the filename differs:
```yaml
- ./bigQuery/your-key-filename.json:/run/secrets/gcp-key.json:ro
```

**3. Start the stack**

```bash
docker-compose up --build
```

This will:
1. Start PostgreSQL
2. Run Alembic migrations (`alembic upgrade head`)
3. Start the FastAPI app on port `8000`
4. Run an initial GDELT metadata sync immediately

---

## API Reference

All endpoints except `/health` require the header:
```
X-API-Key: <your API_KEY from .env>
```

### `GET /health`
Health check. No auth required.
```json
{ "status": "ok" }
```

---

### `POST /events/search`
Main search endpoint. Free-text filters are normalized through Claude, structured filters are applied directly, then the backend queries BigQuery.

**Request body** — at least one field required:
```json
{
  "country": "Italy",
  "countries": ["France", "Germany"],
  "event_type": "protest",
  "macro_topic": "economy",
  "date_range": {
    "from": 2022,
    "to": 2024
  },
  "sentiment": {
    "tone_min": -5,
    "tone_max": 1,
    "goldstein_min": -10,
    "goldstein_max": 2
  },
  "impact": {
    "min_mentions": 10,
    "min_sources": 2,
    "min_articles": 4
  },
  "actors": {
    "actor1_country": "USA",
    "actor2_country": "Italy"
  },
  "source": {
    "domains": ["ansa.it", "reuters.com"]
  },
  "event_codes": {
    "root_codes": ["14"],
    "base_codes": ["141"],
    "full_codes": ["1411"]
  },
  "quad_classes": [3, 4]
}
```

**Response:**
```json
{
  "filters_received": { "country": "Italy", "event_type": "protest" },
  "filters_normalized": {
    "cameo_country_code": "ITA",
    "fips_country_code": "IT",
    "geo_country_codes": ["FR", "DE", "IT"],
    "actor1_country_code": "USA",
    "actor2_country_code": "ITA",
    "event_root_codes": ["14"],
    "event_base_codes": ["141"],
    "event_codes": ["1411"],
    "quad_classes": [3, 4],
    "source_domains": ["ansa.it", "reuters.com"],
    "tone_min": -5,
    "tone_max": 1,
    "goldstein_min": -10,
    "goldstein_max": 2,
    "min_mentions": 10,
    "min_sources": 2,
    "min_articles": 4,
    "date_from_sqldate": 20220101,
    "date_to_sqldate": 20241231,
    "normalization_notes": "Mapped Italy → ITA/IT, protest → CAMEO root 14"
  },
  "results": [
    {
      "event_id": "1234567890",
      "date": "2024-06-15",
      "actor1_country": "ITA",
      "actor2_country": null,
      "event_code": "141",
      "event_base_code": "141",
      "event_root_code": "14",
      "quad_class": 3,
      "goldstein_scale": -6.5,
      "tone": -4.2,
      "num_mentions": 45,
      "num_sources": 12,
      "num_articles": 38,
      "action_geo_fullname": "Rome, Italy",
      "action_geo_country": "IT",
      "source_name": "reuters.com",
      "source_url": "https://reuters.com/..."
    }
  ],
  "metadata": {
    "total_results": 1,
    "query_time_ms": 1243,
    "last_gdelt_sync": "2026-03-07T01:03:37Z",
    "mapping_version": "2026-03-07T01:03:34Z",
    "bq_bytes_processed": 10468810084
  }
}
```

**Field reference:**

| Field | Description |
|---|---|
| `goldstein_scale` | `-10` (destabilizing) to `+10` (stabilizing) |
| `quad_class` | `1` Verbal Cooperation, `2` Material Cooperation, `3` Verbal Conflict, `4` Material Conflict |
| `tone` | Average sentiment of news articles covering the event (negative = negative tone) |
| `fips_country_code` | 2-letter FIPS code (e.g. `IT`, `US`) — differs from ISO alpha-2 |
| `cameo_country_code` | 3-letter CAMEO code (e.g. `ITA`, `USA`) — differs from ISO alpha-3 |

#### Supported filters in detail

The backend supports two filter families:

- **Free-text filters** — interpreted by Claude and translated into GDELT-compatible codes
- **Structured filters** — validated and applied directly to BigQuery without LLM interpretation

##### Free-text filters (Claude-powered)

These are useful when the frontend wants to keep the UI flexible and let users type natural language.

| Field | Type | Example | What it does |
|---|---|---|---|
| `country` | `string` | `"Italy"`, `"Italia"`, `"United States"` | Claude maps it to `cameo_country_code` and `fips_country_code` |
| `event_type` | `string` | `"protest"`, `"war"`, `"sanctions"` | Claude maps it to one or more CAMEO root/base codes |
| `macro_topic` | `string` | `"energy"`, `"climate"`, `"migration"` | Claude infers the most relevant event categories |

Notes:
- `country` affects both actor-country filtering and geographic-country filtering
- `event_type` and `macro_topic` are merged with any direct `event_codes` supplied by the UI
- these fields are cached in PostgreSQL, so repeated searches do not keep calling Claude

##### Structured filters (direct BigQuery filters)

These are deterministic and do not require Claude.

| Field | Type | Example | BigQuery effect |
|---|---|---|---|
| `countries` | `string[]` | `["France", "Germany"]` | Filters `ActionGeo_CountryCode` using normalized FIPS codes |
| `date_range` | `{from,to}` | `{ "from": 2022, "to": 2024 }` | Filters `SQLDATE` between `YYYY0101` and `YYYY1231` |
| `sentiment.tone_min` | `number` | `-5` | `AvgTone >= tone_min` |
| `sentiment.tone_max` | `number` | `1` | `AvgTone <= tone_max` |
| `sentiment.goldstein_min` | `number` | `-10` | `GoldsteinScale >= goldstein_min` |
| `sentiment.goldstein_max` | `number` | `2` | `GoldsteinScale <= goldstein_max` |
| `impact.min_mentions` | `integer` | `10` | `NumMentions >= min_mentions` |
| `impact.min_sources` | `integer` | `2` | `NumSources >= min_sources` |
| `impact.min_articles` | `integer` | `4` | `NumArticles >= min_articles` |
| `actors.actor1_country` | `string` | `"USA"` | Filters `Actor1CountryCode` using normalized CAMEO codes |
| `actors.actor2_country` | `string` | `"Italy"` | Filters `Actor2CountryCode` using normalized CAMEO codes |
| `source.domains` | `string[]` | `["ansa.it", "reuters.com"]` | Filters by domain extracted from `SOURCEURL` |
| `event_codes.root_codes` | `string[]` | `["14"]` | Filters `EventRootCode` |
| `event_codes.base_codes` | `string[]` | `["141"]` | Filters `EventBaseCode` |
| `event_codes.full_codes` | `string[]` | `["1411"]` | Filters `EventCode` |
| `quad_classes` | `integer[]` | `[3, 4]` | Filters `QuadClass` |

##### How mixed filters behave

You can combine free-text and structured filters in the same request.

Example:
```json
{
  "country": "Italy",
  "event_type": "protest",
  "countries": ["France", "Germany"],
  "actors": { "actor1_country": "USA" },
  "sentiment": { "tone_min": -5 },
  "source": { "domains": ["ansa.it"] },
  "event_codes": { "full_codes": ["141"] },
  "quad_classes": [3]
}
```

Behavior:
- Claude interprets `country` and `event_type`
- the backend merges those interpreted codes with the explicit structured filters
- all resulting filters are applied in a single BigQuery query

##### Validation rules

- at least one filter must be present
- `date_range.from` must be `<= date_range.to`
- `sentiment.tone_min` must be `<= sentiment.tone_max`
- `sentiment.goldstein_min` must be `<= sentiment.goldstein_max`
- `quad_classes` should use GDELT values `1`, `2`, `3`, `4`
- `MAX_BQ_SCAN_DAYS` still applies: very wide date windows are rejected as a cost guard

##### Suggested UI mapping

For a globe-based frontend, a practical UI setup is:

- **Search / natural language**: `country`, `event_type`, `macro_topic`
- **Geography**: `countries`
- **Time**: `date_range`
- **Sentiment**: `sentiment.tone_*`, `sentiment.goldstein_*`
- **Impact**: `impact.min_mentions`, `impact.min_sources`, `impact.min_articles`
- **Actors**: `actors.actor1_country`, `actors.actor2_country`
- **Categories**: `event_codes.*`, `quad_classes`
- **Source filtering**: `source.domains`

---

### `POST /filters/interpret`
Dry-run: normalizes filters and returns the final GDELT query parameters **without** querying BigQuery. Useful for frontend filter preview.

Same request body as `/events/search`. Response is just `filters_normalized`:
```json
{
  "cameo_country_code": "ITA",
  "fips_country_code": "IT",
  "geo_country_codes": ["IT", "FR"],
  "actor1_country_code": "USA",
  "actor2_country_code": null,
  "event_root_codes": ["14"],
  "event_base_codes": ["141"],
  "event_codes": ["1411"],
  "quad_classes": [3],
  "source_domains": ["ansa.it"],
  "tone_min": -5,
  "tone_max": null,
  "goldstein_min": null,
  "goldstein_max": 2,
  "min_mentions": 10,
  "min_sources": 2,
  "min_articles": 4,
  "date_from_sqldate": 20220101,
  "date_to_sqldate": 20241231,
  "normalization_notes": "..."
}
```

---

### `GET /filters/metadata`
Returns the top countries and event types from the last 15-minute GDELT sync. Use this to populate frontend filter dropdowns.

```json
{
  "top_countries": [
    { "fips_code": "US", "event_count": 1197700 },
    { "fips_code": "IR", "event_count": 281225 }
  ],
  "top_event_root_codes": [
    { "root_code": "04", "label": "CONSULT", "event_count": 1281037 },
    { "root_code": "14", "label": "PROTEST", "event_count": 98000 }
  ],
  "last_sync_at": "2026-03-07T01:03:37Z",
  "mapping_version": "2026-03-07T01:03:34Z"
}
```

---

### `GET /sync/status`
Returns the current state of the background GDELT sync job.

```json
{
  "last_sync_at": "2026-03-07T01:03:37Z",
  "latest_sqldate": 20260307,
  "sync_status": "success",
  "error_message": null,
  "top_countries": [...],
  "top_event_root_codes": [...]
}
```

---

### `POST /sync/refresh`
Manually triggers an immediate GDELT metadata sync outside the 15-minute schedule.

---

## Interactive docs

Once the container is running, the full Swagger UI is available at:
```
http://localhost:8000/docs
```
Click **Authorize** in the top right and enter your `API_KEY` to test all endpoints interactively.

---

## Development (without Docker)

```bash
# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

# Install dependencies
pip install -e ".[dev]"

# Apply migrations (requires a running PostgreSQL)
alembic upgrade head

# Run the dev server with auto-reload
uvicorn app.main:create_app --factory --reload --port 8000
```

---

## Running tests

```bash
# Run all tests
pytest

# Run a single test file
pytest tests/test_api_events.py

# Run a single test function
pytest tests/test_api_events.py::test_search_events_success

# Verbose output
pytest -v

# With stdout (useful for debugging)
pytest -s -v
```

Tests use an in-memory SQLite database and mock all external services (BigQuery and Anthropic). No real credentials are needed to run the test suite.

---

## Project structure

```
app/
├── api/
│   ├── dependencies.py       # Auth, DB session, BQ client, Anthropic client
│   ├── error_handlers.py     # Domain exception → HTTP status mapping
│   └── routes/               # events, filters, health, interpret, sync
├── core/
│   ├── config.py             # Settings (pydantic-settings, lru_cache singleton)
│   ├── exceptions.py         # Typed domain exceptions
│   └── logging.py            # structlog setup
├── db/
│   ├── models.py             # SyncState, FilterMappingCache ORM models
│   ├── session.py            # Async engine + session factory
│   └── repositories/         # Raw DB access (services call these)
├── integrations/
│   ├── anthropic_client.py   # AsyncAnthropic client factory
│   ├── bigquery_client.py    # Sync BQ SDK wrapped in ThreadPoolExecutor
│   ├── country_codes.py      # CAMEO + FIPS lookup tables
│   ├── filter_interpreter.py # Claude prompt + retry logic
│   ├── gdelt_query_builder.py# Parameterized BigQuery SQL builders
│   └── gdelt_result_mapper.py# BQ row dict → GDELTEvent Pydantic models
├── scheduler/
│   ├── scheduler.py          # APScheduler setup
│   └── sync_job.py           # 15-minute GDELT metadata sync
├── schemas/
│   ├── events.py             # GDELTEvent, SearchResponse
│   ├── filters.py            # RawFilterInput, NormalizedFilters
│   └── sync.py               # SyncStatusResponse, FiltersMetadataResponse
└── services/
    ├── filter_service.py     # normalize_filters() — cache + Claude orchestration
    └── query_service.py      # search_events() — BQ query + response assembly
alembic/versions/             # Database migrations
tests/                        # pytest test suite
```

---

## Environment variables reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS` | Yes | — | Path to GCP service account JSON (inside container: `/run/secrets/gcp-key.json`) |
| `GCP_PROJECT_ID` | Yes | — | GCP project ID for BigQuery billing |
| `ANTHROPIC_API_KEY` | Yes | — | Anthropic API key |
| `DATABASE_URL` | Yes | — | Async PostgreSQL URL (`postgresql+asyncpg://...`) |
| `API_KEY` | Yes | — | Secret key sent by frontend as `X-API-Key` header |
| `CORS_ORIGINS` | No | `[]` | JSON array of allowed origins, e.g. `["http://localhost:3000"]` |
| `ANTHROPIC_MODEL` | No | `claude-opus-4-5` | Claude model to use |
| `ANTHROPIC_MAX_RETRIES` | No | `3` | Retries with exponential backoff on transient errors |
| `BQ_MAX_RESULTS` | No | `500` | Max rows returned per search query |
| `MAX_BQ_SCAN_DAYS` | No | `3650` | Max date window (cost guard — queries exceeding this are rejected) |
| `SYNC_INTERVAL_MINUTES` | No | `15` | Background sync frequency |
| `RATE_LIMIT_PER_MINUTE` | No | `10` | Max requests/minute per IP on `/events/search` |
| `APP_ENV` | No | `production` | Set to `development` for colored console logs |
| `LOG_LEVEL` | No | `INFO` | Logging level |
