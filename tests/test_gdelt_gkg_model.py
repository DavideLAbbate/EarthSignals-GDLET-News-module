"""Tests for the GdeltGkg ORM model."""

from __future__ import annotations

import pytest
from sqlalchemy import select

from app.db.models import GdeltGkg


async def test_gdelt_gkg_insert_and_retrieve(db_session):
    row = GdeltGkg(
        document_identifier="https://example.com/article",
        themes=["ARMEDCONFLICT", "IRAN"],
        persons=["Mojtaba Khamenei"],
        organizations=["IRGC"],
        locations=["Tehran, Iran"],
        document_tone=-8.7,
    )
    db_session.add(row)
    await db_session.commit()

    result = await db_session.execute(
        select(GdeltGkg).where(GdeltGkg.document_identifier == "https://example.com/article")
    )
    rows = result.scalars().all()
    assert len(rows) == 1
    assert "ARMEDCONFLICT" in rows[0].themes
    assert rows[0].document_tone == pytest.approx(-8.7)


async def test_gdelt_gkg_json_fields_nullable(db_session):
    row = GdeltGkg(document_identifier="https://empty.com/article")
    db_session.add(row)
    await db_session.commit()

    result = await db_session.execute(
        select(GdeltGkg).where(GdeltGkg.document_identifier == "https://empty.com/article")
    )
    gkg = result.scalar_one()
    assert gkg.themes is None
    assert gkg.persons is None
    assert gkg.document_tone is None


async def test_gdelt_gkg_multiple_rows(db_session):
    rows = [
        GdeltGkg(document_identifier="https://a.com/1", themes=["WAR"], document_tone=-5.0),
        GdeltGkg(document_identifier="https://b.com/2", themes=["PEACE"], document_tone=2.0),
    ]
    db_session.add_all(rows)
    await db_session.commit()

    result = await db_session.execute(select(GdeltGkg))
    all_rows = result.scalars().all()
    assert len(all_rows) >= 2
