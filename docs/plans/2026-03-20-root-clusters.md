---
status: completed
phase: 5
updated: 2026-03-20
---

# Root Clusters Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

## Goal
Estrarre i mega cluster con `event_count > 5000` dal normale flusso di materializzazione e salvarli solo in `root_clusters`, escludendoli da `story_clusters`.

## Context & Decisions
| Decision | Rationale | Source |
|----------|-----------|--------|
| La separazione ROOT avviene dopo il merge finale | `event_count` post-merge e il segnale finale corretto; farlo prima rischia di classificare male cluster poi fusi | `ref:ses_2f75856fdffezVkn7rxLPYc4ru` |
| I ROOT vivono solo in `root_clusters` e non anche in `story_clusters` | Mantiene una separazione semantica netta e evita duplicazione/incoerenza | `ref:ses_2f75856fdffezVkn7rxLPYc4ru` |
| `root_clusters` riusa lo schema di `story_clusters` | Riduce trasformazioni, consente riuso schema API e minimizza rischio regressioni | `ref:ses_2f75856fdffezVkn7rxLPYc4ru` |
| `/root-clusters/search` supporta anche `country_code` | Mantiene parita funzionale con `/clusters/search` ed evita differenze di contratto non necessarie | `ref:ses_2f7491b32ffes3MIgmBFQE9ptj` |
| I rerun devono riconciliare i cluster che cambiano categoria | Un cluster che passa da story a root, o viceversa, deve essere rimosso dalla tabella opposta per mantenere mutua esclusione | `ref:ses_2f7491b32ffes3MIgmBFQE9ptj` |
| La retention dei cluster e fuori scope in questa iterazione | Oggi non esiste un caller di retention per `story_clusters` o `root_clusters`; introdurlo ora sarebbe YAGNI | `ref:ses_2f7491b32ffes3MIgmBFQE9ptj` |

## Architecture
Il pipeline continua a costruire e fondere i cluster come oggi, poi effettua una partizione post-merge basata sulla soglia ROOT. I cluster normali vengono upsertati in `story_clusters`, i mega cluster vengono upsertati in `root_clusters`, e ogni run elimina gli eventuali `cluster_id` dalla tabella opposta per garantire che ogni cluster viva in una sola categoria.

## Tech Stack
FastAPI, SQLAlchemy async, Alembic, Pydantic v2, pytest, Ruff

## Phase 1: Database foundation [COMPLETED]
- [x] 1.1 Scrivere test fallente per `RootCluster` e round-trip campi base
- [x] 1.2 Eseguire il test mirato e verificarne il fallimento
- [x] 1.3 Aggiungere `RootCluster` in `app/db/models.py`, speculare a `StoryCluster`
- [x] 1.4 Creare migration per `root_clusters`
- [x] 1.5 Aggiungere indice su `root_clusters.topic_score`
- [x] 1.6 Aggiungere migration GIN su `root_clusters.dominant_countries`
- [x] 1.7 Eseguire test model/repository mirati

### Task 1: Add `RootCluster` model and migration

**Files:**
- Modify: `app/db/models.py`
- Create: `alembic/versions/012_add_root_clusters.py`
- Create: `alembic/versions/013_add_gin_index_root_clusters_dominant_countries.py`
- Test: `tests/test_root_cluster_model.py`

**Step 1: Write the failing test**

```python
async def test_root_cluster_model_persists_core_fields(db_session) -> None:
    root = RootCluster(
        cluster_id="root-1",
        source_url="https://example.com/root",
        event_count=6001,
        num_articles=100,
        num_mentions=200,
        num_sources=50,
        topic_score=9.1,
        dominant_countries=["IR"],
        computed_at=datetime(2026, 3, 20, tzinfo=UTC),
    )
    db_session.add(root)
    await db_session.commit()

    rows = (await db_session.execute(select(RootCluster))).scalars().all()
    assert len(rows) == 1
    assert rows[0].event_count == 6001
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_root_cluster_model.py::test_root_cluster_model_persists_core_fields -v`
Expected: FAIL because `RootCluster` does not exist yet.

**Step 3: Write minimal implementation**
- Add `RootCluster` in `app/db/models.py`
- Mirror the current `StoryCluster` persisted fields
- Keep `cluster_id` unique and indexed
- Keep `computed_at` behavior aligned with `StoryCluster`

**Step 4: Add migrations**
- Create table `root_clusters`
- Match `story_clusters` columns, defaults, and constraints
- Add topic-score index
- Add GIN country index in a dedicated migration to preserve query parity

**Step 5: Run tests to verify pass**
- `pytest tests/test_root_cluster_model.py -v`
- `pytest tests/test_cluster_repository.py -v`

## Phase 2: Repository layer [COMPLETED]
- [x] 2.1 Scrivere test fallenti per upsert/search di `RootClusterRepository`
- [x] 2.2 Eseguire i test repository e verificarne il fallimento
- [x] 2.3 Creare `app/db/repositories/root_cluster_repository.py`
- [x] 2.4 Replicare `upsert`, `bulk_upsert`, `search`
- [x] 2.5 Supportare `country_code` come in `ClusterRepository`
- [x] 2.6 Aggiungere delete bulk per `cluster_id` di riconciliazione
- [x] 2.7 Eseguire i test repository mirati

### Task 2: Add `RootClusterRepository`

**Files:**
- Create: `app/db/repositories/root_cluster_repository.py`
- Modify: `app/db/repositories/cluster_repository.py`
- Test: `tests/test_root_cluster_repository.py`

**Step 1: Write the failing test**

```python
async def test_root_repository_upsert_and_search(db_session):
    repo = RootClusterRepository(db_session)
    await repo.upsert(
        {
            "cluster_id": "root-c1",
            "source_url": "https://example.com/root-c1",
            "event_count": 6001,
            "num_articles": 10,
            "num_mentions": 20,
            "num_sources": 5,
            "topic_score": 8.0,
            "dominant_countries": ["US"],
            "computed_at": datetime.now(UTC),
        }
    )
    await db_session.commit()

    rows, total = await repo.search(country_code="US")
    assert total == 1
    assert rows[0].cluster_id == "root-c1"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_root_cluster_repository.py::test_root_repository_upsert_and_search -v`
Expected: FAIL because repository is missing.

**Step 3: Write minimal implementation**
- Copy the structure of `ClusterRepository`
- Replace `StoryCluster` with `RootCluster`
- Preserve transient-key handling and dialect-aware upsert pattern
- Add a helper for deleting rows by `cluster_id` set, used for category flips

**Step 4: Run tests**
- `pytest tests/test_root_cluster_repository.py -v`

## Phase 3: Materialisation split [COMPLETED]
- [x] 3.1 Scrivere test fallente per spostamento ROOT sopra soglia
- [x] 3.2 Scrivere test fallente per permanenza dei cluster normali in `story_clusters`
- [x] 3.3 Scrivere test di riconciliazione story -> root su rerun
- [x] 3.4 Scrivere test di riconciliazione root -> story su rerun
- [x] 3.5 Aggiungere `root_cluster_min_event_count` a `app/core/config.py`
- [x] 3.6 Iniettare `RootClusterRepository` in `ClusterService`
- [x] 3.7 Partizionare i cluster post-merge in batch story/root
- [x] 3.8 Upsert separati sulle due tabelle e delete dalla tabella opposta
- [x] 3.9 Aggiungere log di partizione/materializzazione
- [x] 3.10 Eseguire i test service mirati

### Task 3: Split merged clusters by ROOT threshold

**Files:**
- Modify: `app/services/cluster_service.py`
- Modify: `app/core/config.py`
- Test: `tests/test_cluster_service.py`

**Step 1: Write the failing tests**

```python
async def test_build_and_materialise_moves_large_cluster_to_root_clusters_only(db_session) -> None:
    source_url = "https://example.com/root-story"
    for i in range(6001):
        db_session.add(
            _make_event(
                100000 + i,
                source_url=source_url,
                num_mentions=1,
                num_sources=1,
                num_articles=1,
            )
        )
    await db_session.commit()

    count = await ClusterService(db_session).build_and_materialise(20260301000000)
    await db_session.commit()

    story_rows = (await db_session.execute(select(StoryCluster))).scalars().all()
    root_rows = (await db_session.execute(select(RootCluster))).scalars().all()

    assert count == 1
    assert story_rows == []
    assert len(root_rows) == 1
    assert root_rows[0].event_count == 6001
```

Add explicit boundary tests for `5000` and `5001`, plus rerun tests where the same `cluster_id` switches category and disappears from the old table.

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_cluster_service.py -k "root or materialise" -v`
Expected: FAIL because all clusters still go to `story_clusters` and no reconciliation exists.

**Step 3: Add threshold setting**
- Add `root_cluster_min_event_count: int = 5000` in `app/core/config.py`
- Use `get_settings()` in `ClusterService`
- Rule: `cluster["event_count"] > root_cluster_min_event_count`

**Step 4: Implement partition and reconciliation**
- In `app/services/cluster_service.py:399`, after `merger.merge(...)`, split into:
  - `root_cluster_rows`
  - `story_cluster_rows`
- Upsert each batch only if non-empty
- Delete `story` ids from `root_clusters`
- Delete `root` ids from `story_clusters`
- Return total inserted as `len(root) + len(story)`

**Step 5: Add logging**
- `cluster_phase_partition`
- `story_clusters_materialised`
- `root_clusters_materialised`
- `cluster_phase_reconcile`

**Step 6: Run tests**
- `pytest tests/test_cluster_service.py -k "root or materialise" -v`
- `pytest tests/test_cluster_repository.py -v`
- `pytest tests/test_root_cluster_repository.py -v`

## Phase 4: API exposure [COMPLETED]
- [x] 4.1 Scrivere test fallente per `GET /root-clusters/search`
- [x] 4.2 Aggiungere route e mapping dei root clusters
- [x] 4.3 Riusare gli schema response esistenti dove possibile
- [x] 4.4 Correggere il mapping di `event_date_ref_start/end` nel mapper esistente
- [x] 4.5 Registrare la nuova route se necessario
- [x] 4.6 Eseguire i test API

### Task 4: Add ROOT search endpoint

**Files:**
- Modify: `app/api/routes/clusters.py`
- Modify: `app/main.py` if router registration changes
- Test: `tests/test_api_root_clusters.py`

**Step 1: Write the failing test**

```python
async def test_search_root_clusters_success(async_client, api_headers, db_session):
    db_session.add(
        RootCluster(
            cluster_id="root-a",
            source_url="https://example.com/root-a",
            event_count=7000,
            num_articles=5,
            num_mentions=8,
            num_sources=3,
            topic_score=8.2,
            event_ids=["1", "2"],
            dominant_event_types=["Combattimento"],
            dominant_quad_classes=["Conflitto materiale"],
            avg_severity_score=8.5,
            dominant_countries=["IR"],
            dominant_locations=["Tehran, Tehran, Iran"],
            mention_count=2,
            distinct_mention_sources=["example.com"],
            mention_identifiers=["https://example.com/root-a"],
            themes=["IRAN"],
            persons=["Person A"],
            organizations=["Org A"],
            gkg_locations=["Tehran, Tehran, Iran"],
            document_tone_avg=-4.2,
            computed_at=datetime(2026, 3, 10, tzinfo=UTC),
        )
    )
    await db_session.commit()

    response = await async_client.get(
        "/root-clusters/search?country_code=IR",
        headers=api_headers,
    )
    assert response.status_code == 200
    assert response.json()["clusters"][0]["cluster_id"] == "root-a"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_api_root_clusters.py::test_search_root_clusters_success -v`
Expected: FAIL because route does not exist.

**Step 3: Implement route**
- Add `search_root_clusters`
- Use `RootClusterRepository`
- Reuse existing response model because the payload is identical

**Step 4: Fix current mapper gap**
- In `app/api/routes/clusters.py:23`, include:
  - `event_date_ref_start=cluster.event_date_ref_start`
  - `event_date_ref_end=cluster.event_date_ref_end`

**Step 5: Run tests**
- `pytest tests/test_api_clusters.py -v`
- `pytest tests/test_api_root_clusters.py -v`

## Phase 5: Regression and operational safety [COMPLETED]
- [x] 5.1 Aggiungere test di regressione: i ROOT non appaiono in `/clusters/search`
- [x] 5.2 Aggiungere test di regressione: i cluster standard non appaiono in `/root-clusters/search`
- [x] 5.3 Verificare il job schedulato `app/scheduler/cluster_job.py` con persistenza separata
- [x] 5.4 Verificare il path CLI `run_cluster.py` con persistenza separata
- [x] 5.5 Eseguire lint, test mirati, poi suite completa

### Task 5: Verification and cleanup

**Files:**
- Modify: `tests/test_api_clusters.py`
- Modify: `tests/test_cluster_service.py`
- Possibly modify: `tests/test_api_root_clusters.py`

**Step 1: Write regression tests**
- `/clusters/search` excludes ROOT rows
- `/root-clusters/search` excludes standard rows
- service return count is documented and tested
- rerun reconciliation removes stale rows from the opposite table

**Step 2: Verify operational entrypoints**
- Run the scheduled path behavior through the codepath rooted in `app/scheduler/cluster_job.py`
- Run the manual CLI path in `run_cluster.py`
- Confirm both paths produce the same story/root split semantics

**Step 3: Run verification**
- `ruff check .`
- `pytest tests/test_cluster_service.py -v`
- `pytest tests/test_api_clusters.py -v`
- `pytest tests/test_api_root_clusters.py -v`
- `pytest tests/test_root_cluster_repository.py -v`
- `pytest`

## Notes
- 2026-03-20: Il punto di split corretto e `app/services/cluster_service.py:399`, subito dopo il merge `ref:ses_2f75856fdffezVkn7rxLPYc4ru`
- 2026-03-20: `app/db/models.py:202` e `app/db/repositories/cluster_repository.py:26` sono i template piu diretti per model/repository ROOT `ref:ses_2f75856fdffezVkn7rxLPYc4ru`
- 2026-03-20: `app/api/routes/clusters.py:23` oggi non mappa `event_date_ref_start/end` anche se lo schema li dichiara; conviene correggerlo nello stesso intervento `ref:ses_2f75856fdffezVkn7rxLPYc4ru`
- 2026-03-20: la review del piano ha richiesto riconciliazione dei category flip, decisione esplicita su `country_code`, e rimozione della retention dallo scope attuale `ref:ses_2f7491b32ffes3MIgmBFQE9ptj`
