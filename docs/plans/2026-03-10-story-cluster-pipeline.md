# Story Cluster Pipeline Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extend the GDELT backend to build enriched story clusters by adding three new DB tables (`gdelt_mentions`, `gdelt_gkg`, `story_clusters`), two new GDELT HTTP ingestion paths (EVENTMENTIONS, GKG), a `ClusterService`, and a `/clusters/search` REST endpoint.

**Architecture:** The GDELT CDN publishes three export streams per 15-minute window: EVENTS (already ingested), EVENTMENTIONS (to add), and GKG (to add). A new `ClusterService` reads from `gdelt_events`, joins with mentions and GKG data, computes `topic_score` using the logarithmic formula from the spec, and materialises `story_clusters` rows. A new `GET /clusters/search` route exposes the pre-materialised clusters to API consumers with standard pagination and filtering.

**Tech Stack:** FastAPI, SQLAlchemy 2 async, asyncpg, aiosqlite (tests), Alembic, APScheduler 3.x, httpx, structlog, Pydantic v2, Ruff.

---

## Architectural Decisions

| Decision | Rationale |
|---|---|
| Three new tables, strictly separate | Spec mandates layer separation: raw events / mentions / GKG / clusters |
| Surrogate PK on `gdelt_mentions` | EVENTMENTIONS has no single-column natural PK in GDELT |
| Logarithmic `topic_score` formula | Dampens outliers: `ln(count+1)*weight` per spec Step 3 |
| `ClusterService` materialises into `story_clusters` | Pre-computed rows → fast API reads |
| Ingestion downloads mentions+GKG in same run | Temporal alignment of three layers |
| **Dialect-aware upsert helper** (`app/db/repositories/_upsert.py`) | `pg_insert ON CONFLICT DO NOTHING` for PostgreSQL, `sqlite_insert OR IGNORE` for SQLite — mirrors `_get_insert_chunk_size` pattern in `ingestion_repository.py` |
| `cluster_id = "{YYYYMMDD}_{sha256(source_url)[:12]}"` | Deterministic, collision-resistant, survives re-materialisation; unique constraint on `cluster_id` |
| Ingestion failure isolation: partial success acceptable | Mentions/GKG failure must NOT roll back committed events; log and continue — matches `event_enrichment_service` pattern |
| Cluster materialisation guarded by `enable_cluster_materialisation` | Matches `enable_event_enrichment` pattern for optional features |
| `GET /clusters/search` with query params | REST semantics for read-only paginated queries |
| `ClusterError` + `ClusterBuildError` in `exceptions.py` | All domain exceptions must inherit `GDELTBackendError` and be mapped in `error_handlers.py` |

---

## Phase 1 — DB Models + Migrations

### Task 1.1 — `GdeltMention` ORM model

**Files:**
- Modify: `app/db/models.py`

Add after `GdeltEvent`:

```python
class GdeltMention(Base):
    """One GDELT EVENTMENTIONS row — a document that mentions a specific event."""

    __tablename__ = "gdelt_mentions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    global_event_id: Mapped[int] = mapped_column(BigInteger, index=True, nullable=False)
    event_time_date: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    mention_time_date: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    mention_type: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mention_source_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    mention_identifier: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    sent_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mention_doc_len: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mention_doc_tone: Mapped[float | None] = mapped_column(Float, nullable=True)

    __table_args__ = (
        Index("ix_gdelt_mentions_event_mention", "global_event_id", "mention_identifier"),
    )
```

**Step 1:** Write `tests/test_gdelt_mentions_model.py`

```python
"""Tests for the GdeltMention ORM model."""
from __future__ import annotations

import pytest
from sqlalchemy import select

from app.db.models import GdeltMention


async def test_gdelt_mention_insert_and_retrieve(db_session):
    row = GdeltMention(
        global_event_id=1000,
        mention_identifier="https://example.com/article",
        mention_doc_tone=-3.5,
        mention_source_name="example.com",
        mention_type=1,
    )
    db_session.add(row)
    await db_session.commit()
    result = await db_session.execute(
        select(GdeltMention).where(GdeltMention.global_event_id == 1000)
    )
    rows = result.scalars().all()
    assert len(rows) == 1
    assert rows[0].mention_identifier == "https://example.com/article"
    assert rows[0].mention_doc_tone == pytest.approx(-3.5)
```

Run: `pytest tests/test_gdelt_mentions_model.py -v`

**Step 2:** Create `alembic/versions/006_add_gdelt_mentions.py`

```python
"""Add gdelt_mentions table.

Revision ID: 006
Revises: 005
Create Date: 2026-03-10
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "gdelt_mentions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("global_event_id", sa.BigInteger(), nullable=False),
        sa.Column("event_time_date", sa.BigInteger(), nullable=True),
        sa.Column("mention_time_date", sa.BigInteger(), nullable=True),
        sa.Column("mention_type", sa.Integer(), nullable=True),
        sa.Column("mention_source_name", sa.String(length=200), nullable=True),
        sa.Column("mention_identifier", sa.Text(), nullable=True),
        sa.Column("sent_count", sa.Integer(), nullable=True),
        sa.Column("mention_doc_len", sa.Integer(), nullable=True),
        sa.Column("mention_doc_tone", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_gdelt_mentions_global_event_id", "gdelt_mentions", ["global_event_id"])
    op.create_index("ix_gdelt_mentions_mention_identifier", "gdelt_mentions", ["mention_identifier"])
    op.create_index(
        "ix_gdelt_mentions_event_mention",
        "gdelt_mentions",
        ["global_event_id", "mention_identifier"],
    )


def downgrade() -> None:
    op.drop_index("ix_gdelt_mentions_event_mention", table_name="gdelt_mentions")
    op.drop_index("ix_gdelt_mentions_mention_identifier", table_name="gdelt_mentions")
    op.drop_index("ix_gdelt_mentions_global_event_id", table_name="gdelt_mentions")
    op.drop_table("gdelt_mentions")
```

**Step 3:** `ruff check . && pytest tests/test_gdelt_mentions_model.py -v`

**Step 4:** `git commit -m "feat: add GdeltMention model and migration 006"`

---

### Task 1.2 — `GdeltGkg` ORM model

Add after `GdeltMention` in `app/db/models.py`:

```python
class GdeltGkg(Base):
    """One GDELT GKG row — semantic metadata for a document URL."""

    __tablename__ = "gdelt_gkg"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    gkg_record_id: Mapped[str | None] = mapped_column(String(50), nullable=True, index=True)
    date: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    source_common_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    document_identifier: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    themes: Mapped[list | None] = mapped_column(JSON, nullable=True)
    persons: Mapped[list | None] = mapped_column(JSON, nullable=True)
    organizations: Mapped[list | None] = mapped_column(JSON, nullable=True)
    locations: Mapped[list | None] = mapped_column(JSON, nullable=True)
    document_tone: Mapped[float | None] = mapped_column(Float, nullable=True)
```

Write `tests/test_gdelt_gkg_model.py`, create `alembic/versions/007_add_gdelt_gkg.py`, run tests, commit.

---

### Task 1.3 — `StoryCluster` ORM model

Add after `GdeltGkg` in `app/db/models.py`:

```python
class StoryCluster(Base):
    """Materialised story cluster — one row per source_url, enriched with mentions and GKG."""

    __tablename__ = "story_clusters"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cluster_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)

    # Scoring
    event_count: Mapped[int] = mapped_column(Integer, default=0)
    num_articles: Mapped[int] = mapped_column(Integer, default=0)
    num_mentions: Mapped[int] = mapped_column(Integer, default=0)
    num_sources: Mapped[int] = mapped_column(Integer, default=0)
    topic_score: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Event layer
    event_ids: Mapped[list | None] = mapped_column(JSON, nullable=True)
    dominant_event_types: Mapped[list | None] = mapped_column(JSON, nullable=True)
    dominant_quad_classes: Mapped[list | None] = mapped_column(JSON, nullable=True)
    avg_severity_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    dominant_countries: Mapped[list | None] = mapped_column(JSON, nullable=True)
    dominant_locations: Mapped[list | None] = mapped_column(JSON, nullable=True)

    # Mentions layer
    mention_count: Mapped[int] = mapped_column(Integer, default=0)
    distinct_mention_sources: Mapped[list | None] = mapped_column(JSON, nullable=True)
    mention_identifiers: Mapped[list | None] = mapped_column(JSON, nullable=True)
    first_mention_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_mention_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # GKG layer
    themes: Mapped[list | None] = mapped_column(JSON, nullable=True)
    persons: Mapped[list | None] = mapped_column(JSON, nullable=True)
    organizations: Mapped[list | None] = mapped_column(JSON, nullable=True)
    gkg_locations: Mapped[list | None] = mapped_column(JSON, nullable=True)
    document_tone_avg: Mapped[float | None] = mapped_column(Float, nullable=True)

    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )

    __table_args__ = (Index("ix_story_clusters_topic_score", "topic_score"),)
```

> Ensure `from datetime import datetime, UTC` is present in the models.py imports.

Write `tests/test_story_cluster_model.py`, create `alembic/versions/008_add_story_clusters.py`, run tests, commit.

**Final Phase 1 commit:** `git commit -m "feat: add GdeltMention, GdeltGkg, StoryCluster models and migrations 006-008"`

---

## Phase 2 — GDELT HTTP Client Extensions

### Task 2.1 — Verify GKG 2.1 column layout

Before writing any parser: confirm GDELT GKG 2.1 column positions against the official GDELT documentation. Add named constants at the top of `gdelt_http_client.py`:

```python
# ── GKG 2.1 column indices (0-based, tab-separated) ─────────────────────
_GKG_COL_RECORD_ID = 0
_GKG_COL_DATE = 1
_GKG_COL_SOURCE_COMMON_NAME = 3
_GKG_COL_DOCUMENT_IDENTIFIER = 4
_GKG_COL_THEMES = 7
_GKG_COL_LOCATIONS = 9
_GKG_COL_PERSONS = 11
_GKG_COL_ORGANIZATIONS = 13
_GKG_COL_TONE = 15
_GKG_MIN_COLS = 16
```

### Task 2.2 — Refactor `_parse_export_lines`

Make `filename_fragment` an optional parameter (default `"export"`) so existing callers are unaffected:

```python
def _parse_export_lines(text: str, filename_fragment: str = "export") -> list[tuple[str, int]]:
    ...
    # filter lines containing filename_fragment
```

### Task 2.3 — EVENTMENTIONS parser + fetch methods

**EVENTMENTIONS CSV columns (tab-separated, 14 relevant fields):**

| Index | Column |
|---|---|
| 0 | `GLOBALEVENTID` |
| 1 | `EventTimeDate` (YYYYMMDDHHMMSS) |
| 2 | `MentionTimeDate` |
| 3 | `MentionType` (1=WEB) |
| 4 | `MentionSourceName` |
| 5 | `MentionIdentifier` (URL) |
| 6 | `SentenceAlid` |
| 12 | `MentionDocLen` |
| 13 | `MentionDocTone` |

```python
def parse_gdelt_mentions_row(row: list[str]) -> dict:
    if len(row) < 14:
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
```

Add to `GdeltHttpClient`:
- `fetch_latest_mentions_url() -> tuple[str, int]` — parses `lastupdate.txt` line containing `"mentions"`
- `fetch_master_mentions_urls(since_ts, until_ts) -> list[tuple[str, int]]`
- `download_mentions(url) -> list[dict]`

### Task 2.4 — GKG parser + fetch methods

```python
def _parse_semicolon_field(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(";") if item.strip()]


def parse_gdelt_gkg_row(row: list[str]) -> dict:
    if len(row) < _GKG_MIN_COLS:
        return {}
    tone_str = _str_or_none(row[_GKG_COL_TONE])
    avg_tone: float | None = None
    if tone_str:
        try:
            avg_tone = float(tone_str.split(",")[0])
        except (ValueError, IndexError):
            pass
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
```

Add: `fetch_latest_gkg_url()`, `fetch_master_gkg_urls()`, `download_gkg()`.

**Tests:** Add to `tests/test_gdelt_http_client.py` covering both parsers and the `lastupdate.txt` line-selection logic.

**Commit:** `git commit -m "feat: add EVENTMENTIONS and GKG download support to GdeltHttpClient"`

---

## Phase 3 — Repositories

### Task 3.1 — Dialect-aware upsert helper

Create `app/db/repositories/_upsert.py`:

```python
"""Dialect-aware upsert helpers for PostgreSQL and SQLite."""
from __future__ import annotations

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession


def make_insert_ignore(session: AsyncSession, model, rows: list[dict]):
    """Build an INSERT OR IGNORE / ON CONFLICT DO NOTHING statement for the current dialect."""
    dialect = session.bind.dialect.name if session.bind else "postgresql"
    if dialect == "sqlite":
        return sqlite_insert(model).values(rows).prefix_with("OR IGNORE")
    return pg_insert(model).values(rows).on_conflict_do_nothing()
```

> Mirror the `_get_insert_chunk_size` branching pattern already in `ingestion_repository.py`.

### Task 3.2 — `MentionsRepository`

`app/db/repositories/mentions_repository.py` — `bulk_upsert`, `get_by_event_ids`, `delete_before_dateadded`.

### Task 3.3 — `GkgRepository`

`app/db/repositories/gkg_repository.py` — `bulk_upsert`, `get_by_document_identifiers`, `delete_before_date`.

### Task 3.4 — `ClusterRepository`

`app/db/repositories/cluster_repository.py` — `upsert` (ON CONFLICT DO UPDATE on `cluster_id`), `bulk_upsert`, `search(min_score, country_code, limit, offset) -> (list, total)`, `delete_computed_before`.

**Tests for each repository:** `tests/test_mentions_repository.py`, `tests/test_gkg_repository.py`, `tests/test_cluster_repository.py`.

**Commit:** `git commit -m "feat: add MentionsRepository, GkgRepository, ClusterRepository with dialect-aware upsert"`

---

## Phase 4 — Domain Exceptions + ClusterService

### Task 4.1 — New exceptions

In `app/core/exceptions.py`, add:

```python
class ClusterError(GDELTBackendError):
    """General cluster pipeline error."""

class ClusterBuildError(ClusterError):
    """Cluster materialisation job failed."""
```

In `app/api/error_handlers.py`, map:
- `ClusterBuildError` → HTTP 503
- `ClusterError` → HTTP 500

Add tests in `tests/test_error_handlers.py`.

### Task 4.2 — `event_enrichment_mapper.py`

`app/integrations/event_enrichment_mapper.py`:

```python
"""Label mappings and score formulas for GDELT event enrichment."""
from __future__ import annotations

import math

_QUAD_CLASS_LABELS: dict[int, str] = {
    1: "Cooperazione diplomatica",
    2: "Cooperazione concreta",
    3: "Tensione verbale",
    4: "Conflitto materiale",
}

_EVENT_ROOT_CODE_LABELS: dict[str, str] = {
    "11": "Critica", "13": "Minaccia", "14": "Protesta",
    "18": "Attacco", "19": "Combattimento", "20": "Violenza di massa",
    # ... full mapping per spec
}


def get_quad_class_label(quad_class: int | None) -> str: ...

def get_event_root_code_label(code: str | None) -> str: ...

def compute_severity_score(
    quad_class: int | None, goldstein_scale: float | None, avg_tone: float | None
) -> float:
    """severity = quad_weight + abs(goldstein)*0.5 + abs(tone)*0.3"""
    quad_weight = {1: 0.0, 2: 2.0, 3: 5.0, 4: 10.0}.get(quad_class or 0, 0.0)
    return round(min(quad_weight + abs(goldstein_scale or 0)*0.5 + abs(avg_tone or 0)*0.3, 20.0), 2)

def compute_topic_score(
    event_count: int, num_articles: int, num_mentions: int, num_sources: int
) -> float:
    """topic_score = ln(events+1)*0.4 + ln(articles+1)*0.3 + ln(mentions+1)*0.2 + ln(sources+1)*0.1"""
    return round(
        math.log(event_count + 1) * 0.4
        + math.log(num_articles + 1) * 0.3
        + math.log(num_mentions + 1) * 0.2
        + math.log(num_sources + 1) * 0.1,
        4,
    )
```

Tests: `tests/test_event_enrichment_mapper.py`.

### Task 4.3 — `ClusterService`

`app/services/cluster_service.py` — full 8-step pipeline:

1. `_score_source_urls(since_sqldate)` — GROUP BY source_url, compute topic_score, filter < 0.5
2. `_collect_events(source_url)` — fetch events, compute per-event derived fields
3. `_collect_mentions(event_ids)` — fetch from `gdelt_mentions`
4. `_collect_gkg(mention_identifiers)` — fetch from `gdelt_gkg`
5. `_build_cluster(doc)` — assemble full cluster dict; `cluster_id = f"{YYYYMMDD}_{sha256(source_url)[:12]}"`
6. `build_and_materialise(since_sqldate)` — orchestrate, call `cluster_repo.bulk_upsert`, commit

Tests: `tests/test_cluster_service.py`.

**Commit:** `git commit -m "feat: add ClusterService, event_enrichment_mapper and domain exceptions"`

---

## Phase 5 — Scheduler Integration

**Task 5.1:** Add to `app/core/config.py`:

```python
enable_cluster_materialisation: bool = Field(default=True, ...)
cluster_interval_minutes: int = Field(default=60, ...)
```

**Task 5.2:** Create `app/scheduler/cluster_job.py`:

```python
async def run_cluster_job(session_factory) -> None:
    since_dt = datetime.now(UTC) - timedelta(days=30)
    since_sqldate = int(since_dt.strftime("%Y%m%d"))
    async with session_factory() as session:
        await ClusterService(session).build_and_materialise(since_sqldate)
```

**Task 5.3:** In `add_sync_job()` in `scheduler.py`, add guarded job registration:

```python
if settings.enable_cluster_materialisation:
    scheduler.add_job(
        run_cluster_job,
        trigger="interval",
        minutes=settings.cluster_interval_minutes,
        id="gdelt_cluster_materialisation",
        max_instances=1,
        args=[session_factory],
    )
```

**Tests:** `tests/test_scheduler.py` — verify job present when `enable_cluster_materialisation=True`, absent when `False`.

**Commit:** `git commit -m "feat: register cluster materialisation job in scheduler"`

---

## Phase 6 — API Route + Schemas

### Pydantic Schemas (`app/schemas/clusters.py`)

```python
class ClusterScore(BaseModel):
    events: int; num_articles: int; num_mentions: int; num_sources: int; topic_score: float | None

class ClusterEventEnrichment(BaseModel):
    dominant_event_types: list[str] = []
    dominant_quad_classes: list[str] = []
    avg_severity_score: float | None = None
    dominant_countries: list[str] = []
    dominant_locations: list[str] = []

class ClusterMentionsEnrichment(BaseModel):
    mention_count: int = 0
    distinct_mention_sources: list[str] = []
    first_mention_at: datetime | None = None
    last_mention_at: datetime | None = None

class ClusterGkgEnrichment(BaseModel):
    themes: list[str] = []; persons: list[str] = []
    organizations: list[str] = []; locations: list[str] = []
    document_tone_avg: float | None = None

class StoryClusterResponse(BaseModel):
    cluster_id: str; source_url: str
    score: ClusterScore; event_ids: list[str] = []
    event_enrichment: ClusterEventEnrichment
    mentions_enrichment: ClusterMentionsEnrichment
    gkg_enrichment: ClusterGkgEnrichment
    computed_at: datetime

class ClusterSearchResponse(BaseModel):
    clusters: list[StoryClusterResponse]
    total: int; limit: int; offset: int
```

### Route (`app/api/routes/clusters.py`)

```python
@router.get("/search", response_model=ClusterSearchResponse, dependencies=[Depends(require_api_key)])
async def search_clusters(
    min_score: float | None = Query(default=None, ge=0.0),
    country_code: str | None = Query(default=None, max_length=2),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_db_session),
) -> ClusterSearchResponse: ...
```

Register in `app/main.py`.

**Tests:** `tests/test_api_clusters.py` — 200+auth, 401+no-auth, min_score filter, pagination fields.

**Commit:** `git commit -m "feat: add GET /clusters/search route and Pydantic schemas"`

---

## Phase 7 — Ingestion Pipeline Extension

### Mapper helpers (`app/services/ingestion_service.py`)

```python
def _row_to_mention_dict(row: dict) -> dict:
    return {
        "global_event_id": row.get("GLOBALEVENTID"),
        "event_time_date": row.get("EventTimeDate"),
        "mention_time_date": row.get("MentionTimeDate"),
        "mention_type": row.get("MentionType"),
        "mention_source_name": row.get("MentionSourceName"),
        "mention_identifier": row.get("MentionIdentifier"),
        "mention_doc_len": row.get("MentionDocLen"),
        "mention_doc_tone": row.get("MentionDocTone"),
    }

def _row_to_gkg_dict(row: dict) -> dict:
    return {
        "gkg_record_id": row.get("GKGRECORDID"),
        "date": row.get("DATE"),
        "source_common_name": row.get("SourceCommonName"),
        "document_identifier": row.get("DocumentIdentifier"),
        "themes": row.get("V1Themes") or [],
        "persons": row.get("V1Persons") or [],
        "organizations": row.get("V1Organizations") or [],
        "locations": row.get("V1Locations") or [],
        "document_tone": row.get("AvgTone"),
    }
```

### Partial-success pattern

After committing events, in a separate try/except block:

```python
try:
    mentions_url, _ = await self._http_client.fetch_latest_mentions_url()
    mentions_rows = await self._http_client.download_mentions(mentions_url)
    mapped = [_row_to_mention_dict(r) for r in mentions_rows if r]
    if mapped:
        await mentions_repo.bulk_upsert(mapped)
        await session.commit()
except Exception:
    logger.exception("mentions_ingestion_error")  # non-throwing

try:
    gkg_url, _ = await self._http_client.fetch_latest_gkg_url()
    gkg_rows = await self._http_client.download_gkg(gkg_url)
    mapped_gkg = [_row_to_gkg_dict(r) for r in gkg_rows if r]
    if mapped_gkg:
        await gkg_repo.bulk_upsert(mapped_gkg)
        await session.commit()
except Exception:
    logger.exception("gkg_ingestion_error")  # non-throwing
```

**Tests:** Verify mentions inserted, verify events NOT rolled back on mentions failure, verify GKG inserted independently.

**Commit:** `git commit -m "feat: extend ingestion pipeline to ingest EVENTMENTIONS and GKG with partial-success isolation"`

---

## Phase 8 — Final Verification

```bash
# Full test suite
pytest -v

# Lint
ruff check .

# Migration upgrade
alembic upgrade head

# Migration downgrade (verify all three new migrations have working downgrade())
alembic downgrade -1
alembic downgrade -1
alembic downgrade -1
alembic upgrade head

# Manual API check
curl -H "X-API-Key: <key>" http://localhost:8000/clusters/search
curl http://localhost:8000/clusters/search  # expects 401
```

---

## Verification Checklist

- [ ] `alembic upgrade head` applies migrations 006, 007, 008 without errors
- [ ] `alembic downgrade -1` works for all three new migrations
- [ ] `pytest` — 0 failures, 0 errors
- [ ] `ruff check .` — 0 issues
- [ ] `GET /clusters/search` returns `200` when authenticated
- [ ] `GET /clusters/search` returns `401` without API key
- [ ] `ClusterService.build_and_materialise()` runs without error against local PostgreSQL
- [ ] Cluster materialisation job appears in scheduler job list (`/health` or logs)
- [ ] EVENTMENTIONS and GKG rows populated after an incremental ingestion run
- [ ] Events are NOT rolled back when mentions download fails (partial-success isolation)
