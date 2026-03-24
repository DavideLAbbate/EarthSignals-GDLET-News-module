"""Tests for terminal cluster component cleanup."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select

from app.core.config import get_settings
from app.db.models import ClusterComponent, ClusterComponentEvent
from app.services.cluster_terminal_cleanup_service import run_cluster_terminal_cleanup


async def test_run_cluster_terminal_cleanup_deletes_old_terminal_components(
    db_session,
    monkeypatch,
) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "cluster_terminal_state_retention_days", 7)
    now = datetime(2026, 3, 25, 12, tzinfo=UTC)
    monkeypatch.setattr(
        "app.services.cluster_terminal_cleanup_service._utcnow",
        lambda: now,
    )

    old_terminal = datetime(2026, 3, 10, tzinfo=UTC)
    recent_terminal = datetime(2026, 3, 22, tzinfo=UTC)
    db_session.add_all(
        [
            ClusterComponent(
                component_id="merged-old",
                status="merged",
                anchor_source_url="https://example.com/merged-old",
                component_source_urls=["https://example.com/merged-old"],
                anchor_locked_at=old_terminal,
                first_seen_at=old_terminal,
                last_seen_at=old_terminal,
                has_gkg=False,
            ),
            ClusterComponent(
                component_id="split-old",
                status="split",
                anchor_source_url="https://example.com/split-old",
                component_source_urls=["https://example.com/split-old"],
                anchor_locked_at=old_terminal,
                first_seen_at=old_terminal,
                last_seen_at=old_terminal,
                has_gkg=False,
            ),
            ClusterComponent(
                component_id="merged-recent",
                status="merged",
                anchor_source_url="https://example.com/merged-recent",
                component_source_urls=["https://example.com/merged-recent"],
                anchor_locked_at=recent_terminal,
                first_seen_at=recent_terminal,
                last_seen_at=recent_terminal,
                has_gkg=False,
            ),
            ClusterComponent(
                component_id="active-old",
                status="active",
                anchor_source_url="https://example.com/active-old",
                component_source_urls=["https://example.com/active-old"],
                anchor_locked_at=old_terminal,
                first_seen_at=old_terminal,
                last_seen_at=old_terminal,
                has_gkg=False,
            ),
        ]
    )
    db_session.add_all(
        [
            ClusterComponentEvent(
                component_id="merged-old",
                event_id="1001",
                first_seen_at=old_terminal,
                last_seen_at=old_terminal,
                is_active=False,
            ),
            ClusterComponentEvent(
                component_id="split-old",
                event_id="1002",
                first_seen_at=old_terminal,
                last_seen_at=old_terminal,
                is_active=False,
            ),
            ClusterComponentEvent(
                component_id="merged-recent",
                event_id="1003",
                first_seen_at=recent_terminal,
                last_seen_at=recent_terminal,
                is_active=False,
            ),
            ClusterComponentEvent(
                component_id="active-old",
                event_id="1004",
                first_seen_at=old_terminal,
                last_seen_at=old_terminal,
                is_active=True,
            ),
        ]
    )
    await db_session.commit()

    result = await run_cluster_terminal_cleanup(db_session)

    component_ids = {
        row.component_id
        for row in (await db_session.execute(select(ClusterComponent))).scalars().all()
    }
    memberships = (await db_session.execute(select(ClusterComponentEvent))).scalars().all()

    assert result == {
        "deleted_components": 2,
        "deleted_memberships": 2,
        "cutoff_iso": "2026-03-18T12:00:00+00:00",
    }
    assert component_ids == {"merged-recent", "active-old"}
    assert {(row.component_id, row.event_id) for row in memberships} == {
        ("merged-recent", "1003"),
        ("active-old", "1004"),
    }


async def test_run_cluster_terminal_cleanup_is_idempotent(db_session, monkeypatch) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "cluster_terminal_state_retention_days", 7)
    monkeypatch.setattr(
        "app.services.cluster_terminal_cleanup_service._utcnow",
        lambda: datetime(2026, 3, 25, tzinfo=UTC),
    )

    result = await run_cluster_terminal_cleanup(db_session)

    assert result["deleted_components"] == 0
    assert result["deleted_memberships"] == 0
