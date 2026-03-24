"""Tests for persistent cluster component ORM models."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import select

from app.db.models import ClusterComponent, ClusterComponentEvent
from app.core.config import Settings


def test_cluster_component_model_fields() -> None:
    row = ClusterComponent(
        component_id="01ARZ3NDEKTSV4RRFFQ69G5FAV",
        status="active",
        anchor_source_url="https://example.com/story",
        component_source_urls=["https://example.com/story", "https://mirror.example.com/story"],
        anchor_locked_at=datetime(2026, 3, 24, tzinfo=UTC),
        seed_event_ids=["1001", "1002"],
        first_seen_at=datetime(2026, 3, 24, tzinfo=UTC),
        last_seen_at=datetime(2026, 3, 24, tzinfo=UTC),
        current_cluster_id="cluster-1",
        current_table="story_clusters",
        has_gkg=True,
    )

    assert row.component_id == "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    assert row.status == "active"
    assert row.current_table == "story_clusters"
    assert len(row.component_source_urls) == 2


def test_cluster_component_model_has_soft_link_index() -> None:
    indexes = {
        tuple(column.name for column in index.columns)
        for index in ClusterComponent.__table__.indexes
    }

    assert ("current_table", "current_cluster_id") in indexes


def test_settings_expose_cluster_terminal_state_retention_default() -> None:
    settings = Settings.model_validate(
        {
            "anthropic_api_key": "test-anthropic-key",
            "database_url": "sqlite+aiosqlite:///:memory:",
            "api_key": "test-api-key",
        }
    )

    assert settings.cluster_terminal_state_retention_days == 7


def test_migration_015_adds_cluster_soft_link_index() -> None:
    content = Path(
        "alembic/versions/015_add_cluster_terminal_retention_and_soft_link_index.py"
    ).read_text(encoding="utf-8")

    assert "ix_cluster_components_current_table_cluster_id" in content
    assert "current_table" in content
    assert "current_cluster_id" in content
    assert "create_index" in content


def test_cluster_component_event_model_fields() -> None:
    row = ClusterComponentEvent(
        component_id="01ARZ3NDEKTSV4RRFFQ69G5FAV",
        event_id="1001",
        first_seen_at=datetime(2026, 3, 24, tzinfo=UTC),
        last_seen_at=datetime(2026, 3, 24, tzinfo=UTC),
        is_active=True,
    )

    assert row.component_id == "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    assert row.event_id == "1001"
    assert row.is_active is True


async def test_cluster_component_tables_round_trip(db_session) -> None:
    now = datetime(2026, 3, 24, tzinfo=UTC)
    component = ClusterComponent(
        component_id="comp-1",
        status="active",
        anchor_source_url="https://example.com/story",
        component_source_urls=["https://example.com/story"],
        anchor_locked_at=now,
        seed_event_ids=["1001"],
        first_seen_at=now,
        last_seen_at=now,
        current_cluster_id="cluster-1",
        current_table="story_clusters",
        has_gkg=False,
    )
    db_session.add(component)
    db_session.add(
        ClusterComponentEvent(
            component_id="comp-1",
            event_id="1001",
            first_seen_at=now,
            last_seen_at=now,
            is_active=True,
        )
    )
    await db_session.commit()

    rows = (await db_session.execute(select(ClusterComponent))).scalars().all()
    event_rows = (await db_session.execute(select(ClusterComponentEvent))).scalars().all()

    assert len(rows) == 1
    assert rows[0].seed_event_ids == ["1001"]
    assert rows[0].component_source_urls == ["https://example.com/story"]
    assert len(event_rows) == 1
    assert event_rows[0].component_id == "comp-1"
