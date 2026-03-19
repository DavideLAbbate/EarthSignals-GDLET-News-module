"""Tests for GkgRepository."""

from __future__ import annotations

import pytest

from app.db.repositories.gkg_repository import GkgRepository


async def test_bulk_upsert_inserts_gkg_rows(db_session):
    """bulk_upsert stores GKG rows retrievable by document_identifier."""
    repo = GkgRepository(db_session)
    rows = [
        {
            "document_identifier": "https://example.com/a",
            "themes": ["ARMEDCONFLICT"],
            "persons": ["Alice"],
            "organizations": [],
            "locations": ["Tehran"],
            "document_tone": -7.1,
        }
    ]
    await repo.bulk_upsert(rows)
    gkg_rows = await repo.get_by_document_identifiers(["https://example.com/a"])
    assert len(gkg_rows) == 1
    assert gkg_rows[0].document_tone == pytest.approx(-7.1)
    assert "ARMEDCONFLICT" in gkg_rows[0].themes


async def test_get_by_document_identifiers_returns_empty_for_unknown(db_session):
    """Returns [] when no GKG rows exist for the given identifiers."""
    repo = GkgRepository(db_session)
    result = await repo.get_by_document_identifiers(["https://nowhere.com"])
    assert result == []


async def test_get_by_document_identifiers_returns_empty_for_empty_input(db_session):
    """Returns [] when given an empty list."""
    repo = GkgRepository(db_session)
    result = await repo.get_by_document_identifiers([])
    assert result == []


async def test_bulk_upsert_empty_list_returns_zero(db_session):
    """bulk_upsert returns 0 for an empty list."""
    repo = GkgRepository(db_session)
    count = await repo.bulk_upsert([])
    assert count == 0


async def test_bulk_upsert_multiple_rows(db_session):
    """Multiple GKG rows are all stored and retrievable."""
    repo = GkgRepository(db_session)
    rows = [
        {"document_identifier": "https://a.com/1", "themes": ["WAR"], "document_tone": -5.0},
        {"document_identifier": "https://b.com/2", "themes": ["PEACE"], "document_tone": 2.0},
    ]
    await repo.bulk_upsert(rows)
    results = await repo.get_by_document_identifiers(["https://a.com/1", "https://b.com/2"])
    assert len(results) == 2
    tones = {r.document_identifier: r.document_tone for r in results}
    assert tones["https://a.com/1"] == pytest.approx(-5.0)
    assert tones["https://b.com/2"] == pytest.approx(2.0)


async def test_get_by_document_identifiers_chunks_large_identifier_lists(db_session):
    """get_by_document_identifiers must return all rows when the identifier list exceeds the
    SQLite limit.

    SQLite's _MAX_SQLITE_ARGS=999 means a list of 1001 identifiers crosses two chunks. All
    rows must be returned regardless of chunking.
    """
    repo = GkgRepository(db_session)
    identifiers = [f"https://chunk-test.com/{i}" for i in range(1001)]
    rows = [{"document_identifier": ident, "themes": ["TEST"]} for ident in identifiers]
    await repo.bulk_upsert(rows)

    gkg_rows = await repo.get_by_document_identifiers(identifiers)
    assert len(gkg_rows) == 1001
    returned = {r.document_identifier for r in gkg_rows}
    assert returned == set(identifiers)


async def test_delete_before_date_removes_old_rows(db_session):
    """delete_before_date removes rows with date older than cutoff."""
    repo = GkgRepository(db_session)
    rows = [
        {"document_identifier": "https://old.com", "date": 20260301000000},
        {"document_identifier": "https://new.com", "date": 20260310000000},
    ]
    await repo.bulk_upsert(rows)
    await db_session.commit()

    deleted = await repo.delete_before_date(20260305000000)
    assert deleted >= 1

    remaining = await repo.get_by_document_identifiers(["https://old.com", "https://new.com"])
    ids = {r.document_identifier for r in remaining}
    assert "https://old.com" not in ids
    assert "https://new.com" in ids
