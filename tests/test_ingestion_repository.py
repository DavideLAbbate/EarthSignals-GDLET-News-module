"""Tests for ingestion repository."""

from __future__ import annotations

import pytest

from app.db.repositories import ingestion_repository


@pytest.mark.asyncio
async def test_create_ingestion_run(db_session):
    """Test creating an ingestion run."""
    run = await ingestion_repository.create_ingestion_run(
        db_session,
        ingestion_repository.IngestionType.BOOTSTRAP,
    )
    await db_session.commit()

    assert run.id is not None
    assert run.ingestion_type == "bootstrap"
    assert run.status == "running"
    assert run.events_ingested == 0
    assert run.watermark_dateadded is None


@pytest.mark.asyncio
async def test_update_ingestion_run_completed(db_session):
    """Test updating an ingestion run to completed."""
    run = await ingestion_repository.create_ingestion_run(
        db_session,
        ingestion_repository.IngestionType.INCREMENTAL,
    )
    await db_session.commit()

    await ingestion_repository.update_ingestion_run(
        db_session,
        run.id,
        ingestion_repository.IngestionStatus.COMPLETED,
        watermark_dateadded=20260301000000,
        events_ingested=100,
    )
    await db_session.commit()

    # Fetch the updated run
    from sqlalchemy import select
    from app.db.models import IngestionState

    result = await db_session.execute(select(IngestionState).where(IngestionState.id == run.id))
    updated = result.scalar_one()

    assert updated.status == "completed"
    assert updated.watermark_dateadded == 20260301000000
    assert updated.events_ingested == 100
    assert updated.completed_at is not None


@pytest.mark.asyncio
async def test_get_latest_successful_ingestion(db_session):
    """Test getting the latest successful ingestion."""
    # Create and complete a run
    run = await ingestion_repository.create_ingestion_run(
        db_session,
        ingestion_repository.IngestionType.BOOTSTRAP,
    )
    await db_session.commit()

    await ingestion_repository.update_ingestion_run(
        db_session,
        run.id,
        ingestion_repository.IngestionStatus.COMPLETED,
        watermark_dateadded=20260301000000,
        events_ingested=100,
    )
    await db_session.commit()

    # Get latest successful
    latest = await ingestion_repository.get_latest_successful_ingestion(
        db_session,
        ingestion_repository.IngestionType.BOOTSTRAP,
    )

    assert latest is not None
    assert latest.watermark_dateadded == 20260301000000


@pytest.mark.asyncio
async def test_is_bootstrap_complete(db_session):
    """Test checking if bootstrap is complete."""
    # Initially should not be complete
    assert await ingestion_repository.is_bootstrap_complete(db_session) is False

    # Create and complete bootstrap
    run = await ingestion_repository.create_ingestion_run(
        db_session,
        ingestion_repository.IngestionType.BOOTSTRAP,
    )
    await db_session.commit()

    await ingestion_repository.update_ingestion_run(
        db_session,
        run.id,
        ingestion_repository.IngestionStatus.COMPLETED,
    )
    await db_session.commit()

    # Now should be complete
    assert await ingestion_repository.is_bootstrap_complete(db_session) is True
