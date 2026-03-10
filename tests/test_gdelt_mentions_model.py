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


async def test_gdelt_mention_optional_fields_nullable(db_session):
    row = GdeltMention(global_event_id=2000)
    db_session.add(row)
    await db_session.commit()

    result = await db_session.execute(
        select(GdeltMention).where(GdeltMention.global_event_id == 2000)
    )
    mention = result.scalar_one()
    assert mention.mention_identifier is None
    assert mention.mention_doc_tone is None
    assert mention.mention_type is None
