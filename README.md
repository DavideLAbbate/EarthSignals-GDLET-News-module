<!-- Replace YOUR_GH_USERNAME/YOUR_REPO below once the repo is on GitHub so the CI badge resolves. -->
![CI](https://github.com/YOUR_GH_USERNAME/YOUR_REPO/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11+-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-async-009688.svg)
![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)

# GDELT News Backend

An async **FastAPI** backend that turns the raw, high-volume [GDELT 2.0](https://www.gdeltproject.org/) global news feed into queryable, LLM-enriched **news stories**.

It is, at its core, an **applied-LLM system** built around two distinct integrations:

- **Anthropic Claude** translates free-text search filters (`"Italy"`, `"protests"`, `"energy crisis"`) into precise GDELT query codes — with caching, retries and cost guards.
- **A local LLM microservice (Ollama)** reads the actual articles behind each clustered story and synthesises a canonical title, neutral summary, topics, keywords and named entities.

Everything in between — ingestion, deduplication, graph-based clustering, materialisation, scheduling — exists to feed those two LLM stages clean, consolidated input.

---

## Why this project is interesting

- **Two complementary LLM patterns in one codebase** — a cloud model used as a *structured-output tool* (NL → codes) and a self-hosted model used for *multi-document synthesis*, each wrapped with the engineering you actually need in production.
- **LLM cost & latency discipline** — Claude calls are content-addressed and cached (`FilterMappingCache`, SHA-256 keyed); structured filters bypass the LLM entirely; enrichment dedupes article fetches across a batch and caches failures.
- **Robustness over happy-path** — retries with backoff, an idempotent enrichment state machine (`pending → processing → success/failed`), stale-state recovery, and graceful degradation when an article is unreachable.
- **Real data engineering** — a graph-based clustering pipeline that merges three GDELT layers (events, mentions, GKG) into persistent, cross-run story components. The full architecture is documented in [`docs/paper.md`](docs/paper.md).
- **Production shape** — layered architecture (routes → services → repositories), async SQLAlchemy 2.0 + Alembic, APScheduler jobs, structured logging, rate limiting, API-key auth, Docker, and a ~38-file test suite that mocks every external dependency.

---

## Architecture at a glance

```
                         data.gdeltproject.org (GDELT v2 HTTP exports, every 15 min)
                                          │
                                          ▼
                          ┌──────────────────────────────┐
                          │  Ingestion (HTTP CSV/ZIP)     │
                          │  events · mentions · GKG      │
                          └──────────────┬───────────────┘
                                         ▼
                          ┌──────────────────────────────┐
                          │  Clustering pipeline          │
                          │  event–mention graph →        │
                          │  components → merge →          │
                          │  story_clusters / root_clusters│
                          └───────┬───────────────┬───────┘
                                  │               │
              LLM enrichment ◄────┘               └────► Search API
        (Ollama microservice :8001)                  (FastAPI, X-API-Key)
        title · summary · topics ·                  /events/search
        keywords · entities                         /clusters/search
                                                    /root-clusters/search

        Search request with free-text filters
                       │
                       ▼
            Anthropic Claude ──► normalizes "Italy"/"protest"/"energy"
                       │            into CAMEO/FIPS + event codes (cached)
                       ▼
            PostgreSQL ──► local query over ingested GDELT layers
```

Key runtime facts:

- `POST /events/search` reads only from local **PostgreSQL** — never a live external query.
- Free-text fields (`country`, `event_type`, `macro_topic`) may call **Claude**; purely structured filters never do.
- Clusters and root clusters are **pre-materialised** by background jobs; the API just serves consolidated views.
- LLM enrichment runs as a **separate microservice**, so the model/provider can change without touching the main app.

---

## The dual-LLM design

### 1. Claude as a structured-filter interpreter

The frontend can send human language. The backend turns it into deterministic GDELT codes:

```
POST /events/search
{ "country": "Italia", "event_type": "protest", "macro_topic": "energy" }
        │
        ▼  filter_service.normalize_filters()
Cache lookup (SHA-256 of canonical filters) ──hit──► return cached mapping
        │ miss
        ▼
Claude ──► { cameo_country_code: "ITA", fips_country_code: "IT",
            event_root_codes: ["14"], date_from/to, ... }
        │
        ▼  cache write (TTL 24h)  →  merge with structured filters  →  SQL
```

Engineering details that matter:
- **Content-addressed cache** so identical searches never re-hit the model.
- **Validated output** — Claude's JSON is parsed into a strict Pydantic schema; malformed responses raise a typed domain error.
- **Retries with backoff** on transient API errors.
- **Bypass path** — requests using only structured filters (codes, dates, sentiment thresholds) skip the LLM entirely.

### 2. A local LLM for multi-article synthesis

Each materialised story cluster points at the URLs that corroborate it. The enrichment job fetches up to N articles **from distinct domains**, concatenates them, and asks a locally hosted model (via the Ollama-backed microservice) for an editorial representation:

- `article_title`, `article_summary` (synthesised across sources, not copied from one)
- `cited_sources`, `main_topics`, `keywords`
- `entities` (people, orgs, locations, products, brands, …)

Production concerns handled: source-diversity selection, per-URL retries, intra-batch fetch/failure caching, an idempotent status machine that survives crashes, and priority ordering by story coverage. Full write-up in [`docs/paper.md`](docs/paper.md).

---

## Clustering pipeline (short version)

GDELT emits atomic events, document mentions and semantic (GKG) signals separately. The pipeline:

1. builds an **event–mention bipartite graph** over a rolling window;
2. extracts **connected components** as story candidates and filters out aggregators/section pages/singletons;
3. enriches each candidate with batched event/mention/GKG data;
4. **merges** components into wider stories (Union-Find) gated by mention overlap, GKG-theme Jaccard similarity, time proximity and event-type agreement;
5. splits output into `story_clusters` (normal) and `root_clusters` (mega-stories) and reconciles identity across runs via persistent `cluster_components`.

The design rationale (idempotency, cross-run continuity, failure modes) lives in [`docs/paper.md`](docs/paper.md).

---

## Tech stack

| Layer | Technology |
|---|---|
| Web framework | FastAPI 0.115 + Uvicorn |
| Filter LLM | Anthropic Claude (`claude-opus-4-5`) |
| Enrichment LLM | Local model via Ollama microservice |
| Data source | GDELT 2.0 v2 HTTP exports (events / mentions / GKG) |
| Database | PostgreSQL 16 (async: asyncpg + SQLAlchemy 2.0) |
| Migrations | Alembic |
| Scheduler | APScheduler 3.10 |
| Logging | structlog (JSON in production) |
| Config | pydantic-settings |
| Auth | `X-API-Key` header |
| Rate limiting | slowapi |
| Tooling | Ruff (lint + format), pytest, Docker, GitHub Actions CI |

---

## Quick start (Docker)

**Prerequisites:** Docker + Docker Compose, and an [Anthropic API key](https://console.anthropic.com/). No cloud data warehouse or service account is required — ingestion pulls directly from GDELT's public HTTP server.

```bash
git clone <repo-url>
cd web-journal-news-module
cp .env.example .env        # then edit ANTHROPIC_API_KEY and API_KEY
docker-compose up --build
```

This will:
1. start PostgreSQL,
2. run Alembic migrations,
3. start the FastAPI app on port `8000`,
4. run an initial metadata sync,
5. bootstrap ingestion only if the `gdelt_events` table is empty.

> LLM enrichment is **off by default** (`ENABLE_CLUSTER_ENRICHMENT=false`) because it needs the separate Ollama-backed microservice running on `:8001`. Search and clustering work fully without it.

---

## Local development (without Docker)

```bash
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -e ".[dev]"

alembic upgrade head                  # requires a running PostgreSQL
uvicorn app.main:create_app --factory --reload --port 8000
```

Lint, format and test:

```bash
ruff check .
ruff format --check .
pytest
```

Tests run on in-memory SQLite and mock all external services (Anthropic, GDELT HTTP, enrichment) — **no real credentials needed**.

Interactive API docs (Swagger): `http://localhost:8000/docs` — click **Authorize** and paste your `API_KEY`.

---

## API reference

All routes except `/health` require the header `X-API-Key: <your API_KEY>`.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Liveness check (no auth) |
| `POST` | `/events/search` | Search GDELT events; free-text filters normalized by Claude |
| `POST` | `/filters/interpret` | Dry-run: return normalized filters without querying |
| `GET` | `/filters/metadata` | Top countries / event codes for populating UI dropdowns |
| `GET` | `/clusters/search` | Search materialised **story** clusters |
| `GET` | `/root-clusters/search` | Search materialised **root** (mega-story) clusters |
| `POST` | `/enrich/trigger` | Manually run one LLM enrichment batch (1-min cooldown) |
| `GET` | `/sync/status` | Background GDELT sync state |
| `POST` | `/sync/refresh` | Trigger an immediate metadata sync |

### `POST /events/search`

Free-text filters are interpreted by Claude; structured filters are applied directly. At least one field is required.

```json
{
  "country": "Italy",
  "countries": ["France", "Germany"],
  "event_type": "protest",
  "macro_topic": "economy",
  "date_range": { "from": 2022, "to": 2024 },
  "sentiment": { "tone_min": -5, "tone_max": 1, "goldstein_min": -10, "goldstein_max": 2 },
  "impact": { "min_mentions": 10, "min_sources": 2, "min_articles": 4 },
  "actors": { "actor1_country": "USA", "actor2_country": "Italy" },
  "source": { "domains": ["ansa.it", "reuters.com"] },
  "event_codes": { "root_codes": ["14"], "base_codes": ["141"], "full_codes": ["1411"] },
  "quad_classes": [3, 4]
}
```

The response echoes `filters_received`, the Claude-normalized `filters_normalized`, the matching `results`, and a `metadata` envelope. Two filter families are supported:

- **Free-text (Claude-powered):** `country`, `event_type`, `macro_topic` — mapped to CAMEO/FIPS country codes and CAMEO event codes. Cached, so repeated searches don't re-call the model.
- **Structured (direct SQL):** `countries`, `date_range`, `sentiment.*`, `impact.*`, `actors.*`, `source.domains`, `event_codes.*`, `quad_classes` — deterministic, no LLM.

You can mix both in one request; interpreted codes are merged with explicit ones into a single PostgreSQL query. See `/docs` for the full schema.

### `GET /clusters/search`

Filter pre-materialised stories by score, event count, country, date/mention windows, event type, quad class, GKG theme, and — for LLM-enriched clusters — `keyword` and `topic`.

```
GET /clusters/search?country_code=US&enrichment_status=success&date_from=20240301&min_score=3.5&limit=20
```

The response block is mutually exclusive by state: `enrichment_status=success` returns the `llm_enrichment` block; otherwise it returns the GDELT `gkg_enrichment` block. `mentions_enrichment` is always present. `/root-clusters/search` takes the same parameters.

---

## Project structure

```
app/
├── api/routes/        # events, filters, interpret, clusters, enrich, sync, health
├── core/              # Settings, typed exceptions, structlog setup
├── db/                # SQLAlchemy models, async session, repositories
├── integrations/
│   ├── anthropic_client.py      # AsyncAnthropic factory
│   ├── filter_interpreter.py    # Claude prompt + validation + retry
│   ├── gdelt_http_client.py     # GDELT v2 export download + parse
│   ├── event_enrichment_client.py  # HTTP client for the LLM microservice
│   ├── article_fetcher.py / article_extractor.py
│   ├── postgres_compiler.py     # NormalizedFilters → SQL
│   └── country_codes.py         # CAMEO + FIPS lookups
├── scheduler/         # APScheduler wiring + job functions
├── schemas/           # Pydantic v2 request/response models
└── services/          # filter / query / ingestion / clustering / enrichment logic
alembic/versions/      # migrations
docs/paper.md          # full clustering + enrichment architecture write-up
tests/                 # pytest suite (mocked external services)
```

---

## Configuration

Required: `ANTHROPIC_API_KEY`, `API_KEY`, and a database URL (`DATABASE_URL`, auto-built from `POSTGRES_*` under Docker). Everything else has sensible defaults — see [`.env.example`](.env.example) for the common knobs and [`app/core/config.py`](app/core/config.py) for the full set (cluster merge thresholds, candidate gates, GKG caps, retention, scheduler intervals, feature flags).

| Variable | Default | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | — | **Required.** Anthropic API key for filter normalization |
| `API_KEY` | — | **Required.** Secret for the `X-API-Key` header |
| `DATABASE_URL` | — | **Required.** Async PostgreSQL URL (`postgresql+asyncpg://…`) |
| `ANTHROPIC_MODEL` | `claude-opus-4-5` | Claude model used for filters |
| `EVENT_ENRICHMENT_SERVICE_BASE_URL` | `http://localhost:8001` | LLM enrichment microservice |
| `ENABLE_CLUSTER_ENRICHMENT` | `false` | Turn on LLM cluster enrichment |
| `ENABLE_CLUSTER_MATERIALISATION` | `true` | Turn on the clustering job |
| `SYNC_INTERVAL_MINUTES` | `15` | GDELT metadata sync frequency |
| `INGESTION_INTERVAL_MINUTES` | `60` | Incremental ingestion frequency |
| `RETENTION_DAYS` | `30` | Local retention / bootstrap window |
| `RATE_LIMIT_PER_MINUTE` | `10` | Per-IP limit on `/events/search` |
| `CORS_ORIGINS` | `[]` | JSON array of allowed frontend origins |

---

## License

[MIT](LICENSE) © Davide L'Abbate
