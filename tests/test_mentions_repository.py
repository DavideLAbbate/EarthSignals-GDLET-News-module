"""Tests for MentionsRepository."""

from __future__ import annotations

from app.db.repositories.mentions_repository import MentionsRepository


async def test_bulk_upsert_inserts_rows(db_session):
    """bulk_upsert stores mention rows that can be retrieved by event ID."""
    repo = MentionsRepository(db_session)
    rows = [
        {
            "global_event_id": 1001,
            "mention_identifier": "https://example.com/a",
            "mention_doc_tone": -3.0,
            "mention_source_name": "example.com",
            "mention_type": 1,
        },
        {
            "global_event_id": 1001,
            "mention_identifier": "https://other.com/b",
            "mention_doc_tone": 1.5,
            "mention_source_name": "other.com",
            "mention_type": 1,
        },
    ]
    count = await repo.bulk_upsert(rows)
    assert count >= 1  # rowcount semantics differ across dialects
    mentions = await repo.get_by_event_ids([1001])
    assert len(mentions) == 2


async def test_get_by_event_ids_returns_empty_for_unknown(db_session):
    """get_by_event_ids returns [] when no mentions exist for the given IDs."""
    repo = MentionsRepository(db_session)
    result = await repo.get_by_event_ids([99999])
    assert result == []


async def test_get_by_event_ids_returns_empty_for_empty_input(db_session):
    """get_by_event_ids returns [] when the input list is empty."""
    repo = MentionsRepository(db_session)
    result = await repo.get_by_event_ids([])
    assert result == []


async def test_bulk_upsert_empty_list_returns_zero(db_session):
    """bulk_upsert returns 0 when given an empty list."""
    repo = MentionsRepository(db_session)
    count = await repo.bulk_upsert([])
    assert count == 0


async def test_get_by_event_ids_multiple_events(db_session):
    """get_by_event_ids returns mentions for multiple event IDs."""
    repo = MentionsRepository(db_session)
    rows = [
        {"global_event_id": 2001, "mention_identifier": "https://a.com/1"},
        {"global_event_id": 2002, "mention_identifier": "https://b.com/2"},
        {"global_event_id": 2003, "mention_identifier": "https://c.com/3"},
    ]
    await repo.bulk_upsert(rows)
    mentions = await repo.get_by_event_ids([2001, 2002])
    ids = {m.global_event_id for m in mentions}
    assert 2001 in ids
    assert 2002 in ids
    assert 2003 not in ids


async def test_get_by_event_ids_chunks_large_id_lists(db_session):
    """get_by_event_ids must return all rows even when the ID list exceeds the SQLite limit.

    SQLite's _MAX_SQLITE_ARGS=999 means a list of 1001 IDs crosses two chunks. All rows
    must be returned regardless of chunking.
    """
    repo = MentionsRepository(db_session)
    # Insert 1001 distinct event IDs — all with unique mention identifiers
    event_ids = list(range(5000, 6001))  # 1001 IDs
    rows = [
        {"global_event_id": eid, "mention_identifier": f"https://chunk-test.com/{eid}"}
        for eid in event_ids
    ]
    await repo.bulk_upsert(rows)

    mentions = await repo.get_by_event_ids(event_ids)
    assert len(mentions) == 1001
    returned_ids = {m.global_event_id for m in mentions}
    assert returned_ids == set(event_ids)


async def test_delete_before_dateadded_removes_old_rows(db_session):
    """delete_before_dateadded removes rows older than the cutoff."""
    repo = MentionsRepository(db_session)
    rows = [
        {
            "global_event_id": 3001,
            "mention_identifier": "https://old.com",
            "mention_time_date": 20260301000000,
        },
        {
            "global_event_id": 3002,
            "mention_identifier": "https://new.com",
            "mention_time_date": 20260310000000,
        },
    ]
    await repo.bulk_upsert(rows)
    await db_session.commit()

    deleted = await repo.delete_before_dateadded(20260305000000)
    assert deleted >= 1

    remaining = await repo.get_by_event_ids([3001, 3002])
    ids = {m.global_event_id for m in remaining}
    assert 3001 not in ids
    assert 3002 in ids
