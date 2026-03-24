"""Tests for ClusterComponentRepository."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select

from app.db.models import ClusterComponent, ClusterComponentEvent
from app.db.repositories.cluster_component_repository import ClusterComponentRepository


def _as_utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


async def test_repository_creates_component_with_events(db_session) -> None:
    repo = ClusterComponentRepository(db_session)

    component_id = await repo.create_component(
        anchor_source_url="https://example.com/story",
        component_source_urls=["https://example.com/story", "https://mirror.example.com/story"],
        seed_event_ids=["1001", "1002"],
        event_ids=["1001", "1002"],
        observed_at=datetime(2026, 3, 24, tzinfo=UTC),
        has_gkg=True,
        merge_evidence={"mention_overlap": 2},
    )
    await db_session.commit()

    component = await repo.get_by_component_id(component_id)
    memberships = (await db_session.execute(select(ClusterComponentEvent))).scalars().all()

    assert component is not None
    assert component.status == "active"
    assert component.seed_event_ids == ["1001", "1002"]
    assert component.component_source_urls == [
        "https://example.com/story",
        "https://mirror.example.com/story",
    ]
    assert component.has_gkg is True
    assert {membership.event_id for membership in memberships} == {"1001", "1002"}


async def test_repository_lists_active_and_stale_components_for_reconciliation(db_session) -> None:
    now = datetime(2026, 3, 24, tzinfo=UTC)
    db_session.add_all(
        [
            ClusterComponent(
                component_id="active-1",
                status="active",
                anchor_source_url="https://example.com/active",
                component_source_urls=["https://example.com/active"],
                anchor_locked_at=now,
                first_seen_at=now,
                last_seen_at=now,
                has_gkg=False,
            ),
            ClusterComponent(
                component_id="stale-1",
                status="stale",
                anchor_source_url="https://example.com/stale",
                component_source_urls=["https://example.com/stale"],
                anchor_locked_at=now,
                first_seen_at=now,
                last_seen_at=now,
                has_gkg=False,
            ),
            ClusterComponent(
                component_id="merged-1",
                status="merged",
                anchor_source_url="https://example.com/merged",
                component_source_urls=["https://example.com/merged"],
                anchor_locked_at=now,
                first_seen_at=now,
                last_seen_at=now,
                has_gkg=False,
            ),
        ]
    )
    await db_session.commit()

    repo = ClusterComponentRepository(db_session)
    rows = await repo.list_reconcilable_components()

    assert {row.component_id for row in rows} == {"active-1", "stale-1"}


async def test_repository_updates_current_materialization(db_session) -> None:
    now = datetime(2026, 3, 24, tzinfo=UTC)
    component = ClusterComponent(
        component_id="component-1",
        status="active",
        anchor_source_url="https://example.com/story",
        component_source_urls=["https://example.com/story"],
        anchor_locked_at=now,
        first_seen_at=now,
        last_seen_at=now,
        has_gkg=False,
    )
    db_session.add(component)
    await db_session.commit()

    repo = ClusterComponentRepository(db_session)
    await repo.update_current_materialization(
        component_id="component-1",
        cluster_id="cluster-123",
        table_name="story_clusters",
        computed_at=now,
    )
    await db_session.commit()

    refreshed = await repo.get_by_component_id("component-1")
    assert refreshed is not None
    assert refreshed.current_cluster_id == "cluster-123"
    assert refreshed.current_table == "story_clusters"
    assert refreshed.current_computed_at == now


async def test_repository_marks_component_merged_into(db_session) -> None:
    now = datetime(2026, 3, 24, tzinfo=UTC)
    component = ClusterComponent(
        component_id="component-1",
        status="active",
        anchor_source_url="https://example.com/story",
        component_source_urls=["https://example.com/story"],
        anchor_locked_at=now,
        first_seen_at=now,
        last_seen_at=now,
        has_gkg=False,
    )
    db_session.add(component)
    await db_session.commit()

    repo = ClusterComponentRepository(db_session)
    await repo.mark_merged_into("component-1", "component-2", now)
    await db_session.commit()

    refreshed = await repo.get_by_component_id("component-1")
    assert refreshed is not None
    assert refreshed.status == "merged"
    assert refreshed.merged_into_component_id == "component-2"
    assert refreshed.last_seen_at == now


async def test_repository_marks_component_merged_into_deactivates_memberships(db_session) -> None:
    now = datetime(2026, 3, 24, tzinfo=UTC)
    db_session.add(
        ClusterComponent(
            component_id="component-1",
            status="active",
            anchor_source_url="https://example.com/story",
            component_source_urls=["https://example.com/story"],
            anchor_locked_at=now,
            first_seen_at=now,
            last_seen_at=now,
            current_cluster_id="cluster-1",
            current_table="story_clusters",
            current_computed_at=now,
            has_gkg=False,
        )
    )
    db_session.add_all(
        [
            ClusterComponentEvent(
                component_id="component-1",
                event_id="1001",
                first_seen_at=now,
                last_seen_at=now,
                is_active=True,
            ),
            ClusterComponentEvent(
                component_id="component-1",
                event_id="1002",
                first_seen_at=now,
                last_seen_at=now,
                is_active=True,
            ),
        ]
    )
    await db_session.commit()

    repo = ClusterComponentRepository(db_session)
    later = datetime(2026, 3, 25, tzinfo=UTC)
    await repo.mark_merged_into("component-1", "component-2", later)
    await db_session.commit()

    refreshed = await repo.get_by_component_id("component-1")
    memberships = (
        (
            await db_session.execute(
                select(ClusterComponentEvent).where(
                    ClusterComponentEvent.component_id == "component-1"
                )
            )
        )
        .scalars()
        .all()
    )

    assert refreshed is not None
    assert refreshed.status == "merged"
    assert refreshed.current_cluster_id is None
    assert refreshed.current_table is None
    assert refreshed.current_computed_at is None
    assert all(membership.is_active is False for membership in memberships)
    assert all(_as_utc(membership.last_seen_at) == later for membership in memberships)


async def test_repository_marks_component_stale_with_missing_run_count(db_session) -> None:
    now = datetime(2026, 3, 24, tzinfo=UTC)
    component = ClusterComponent(
        component_id="component-1",
        status="active",
        anchor_source_url="https://example.com/story",
        component_source_urls=["https://example.com/story"],
        anchor_locked_at=now,
        first_seen_at=now,
        last_seen_at=now,
        has_gkg=False,
    )
    db_session.add(component)
    await db_session.commit()

    repo = ClusterComponentRepository(db_session)
    await repo.mark_stale("component-1", missing_run_count=3)
    await db_session.commit()

    refreshed = await repo.get_by_component_id("component-1")
    assert refreshed is not None
    assert refreshed.status == "stale"
    assert refreshed.missing_run_count == 3


async def test_repository_marks_component_split_deactivates_memberships(db_session) -> None:
    now = datetime(2026, 3, 24, tzinfo=UTC)
    db_session.add(
        ClusterComponent(
            component_id="component-1",
            status="active",
            anchor_source_url="https://example.com/story",
            component_source_urls=["https://example.com/story"],
            anchor_locked_at=now,
            first_seen_at=now,
            last_seen_at=now,
            current_cluster_id="cluster-1",
            current_table="story_clusters",
            current_computed_at=now,
            has_gkg=False,
        )
    )
    db_session.add(
        ClusterComponentEvent(
            component_id="component-1",
            event_id="1001",
            first_seen_at=now,
            last_seen_at=now,
            is_active=True,
        )
    )
    await db_session.commit()

    repo = ClusterComponentRepository(db_session)
    later = datetime(2026, 3, 25, tzinfo=UTC)
    await repo.mark_split("component-1", later)
    await db_session.commit()

    refreshed = await repo.get_by_component_id("component-1")
    membership = (
        (
            await db_session.execute(
                select(ClusterComponentEvent).where(
                    ClusterComponentEvent.component_id == "component-1"
                )
            )
        )
        .scalars()
        .one()
    )

    assert refreshed is not None
    assert refreshed.status == "split"
    assert refreshed.current_cluster_id is None
    assert refreshed.current_table is None
    assert refreshed.current_computed_at is None
    assert membership.is_active is False
    assert _as_utc(membership.last_seen_at) == later


async def test_repository_replaces_active_event_membership(db_session) -> None:
    now = datetime(2026, 3, 24, tzinfo=UTC)
    component = ClusterComponent(
        component_id="component-1",
        status="active",
        anchor_source_url="https://example.com/story",
        component_source_urls=["https://example.com/story"],
        anchor_locked_at=now,
        first_seen_at=now,
        last_seen_at=now,
        has_gkg=False,
    )
    db_session.add(component)
    db_session.add_all(
        [
            ClusterComponentEvent(
                component_id="component-1",
                event_id="1001",
                first_seen_at=now,
                last_seen_at=now,
                is_active=True,
            ),
            ClusterComponentEvent(
                component_id="component-1",
                event_id="1002",
                first_seen_at=now,
                last_seen_at=now,
                is_active=True,
            ),
        ]
    )
    await db_session.commit()

    repo = ClusterComponentRepository(db_session)
    later = datetime(2026, 3, 25, tzinfo=UTC)
    await repo.replace_active_event_membership("component-1", ["1002", "1003"], later)
    await db_session.commit()

    memberships = (
        (
            await db_session.execute(
                select(ClusterComponentEvent).where(
                    ClusterComponentEvent.component_id == "component-1"
                )
            )
        )
        .scalars()
        .all()
    )
    by_event_id = {membership.event_id: membership for membership in memberships}

    assert by_event_id["1001"].is_active is False
    assert _as_utc(by_event_id["1001"].last_seen_at) == later
    assert by_event_id["1002"].is_active is True
    assert _as_utc(by_event_id["1002"].first_seen_at) == now
    assert _as_utc(by_event_id["1002"].last_seen_at) == later
    assert by_event_id["1003"].is_active is True
    assert _as_utc(by_event_id["1003"].first_seen_at) == later


async def test_repository_mark_active_updates_component_sources(db_session) -> None:
    now = datetime(2026, 3, 24, tzinfo=UTC)
    component = ClusterComponent(
        component_id="component-1",
        status="stale",
        anchor_source_url="https://example.com/original",
        component_source_urls=["https://example.com/original"],
        anchor_locked_at=now,
        first_seen_at=now,
        last_seen_at=now,
        has_gkg=False,
    )
    db_session.add(component)
    await db_session.commit()

    repo = ClusterComponentRepository(db_session)
    await repo.mark_active(
        "component-1",
        datetime(2026, 3, 25, tzinfo=UTC),
        component_source_urls=["https://example.com/original", "https://example.com/new"],
    )
    await db_session.commit()

    refreshed = await repo.get_by_component_id("component-1")
    assert refreshed is not None
    assert refreshed.component_source_urls == [
        "https://example.com/original",
        "https://example.com/new",
    ]


async def test_repository_list_active_event_membership_excludes_terminal_components(
    db_session,
) -> None:
    now = datetime(2026, 3, 24, tzinfo=UTC)
    db_session.add_all(
        [
            ClusterComponent(
                component_id="active-1",
                status="active",
                anchor_source_url="https://example.com/active",
                component_source_urls=["https://example.com/active"],
                anchor_locked_at=now,
                first_seen_at=now,
                last_seen_at=now,
                has_gkg=False,
            ),
            ClusterComponent(
                component_id="stale-1",
                status="stale",
                anchor_source_url="https://example.com/stale",
                component_source_urls=["https://example.com/stale"],
                anchor_locked_at=now,
                first_seen_at=now,
                last_seen_at=now,
                has_gkg=False,
            ),
            ClusterComponent(
                component_id="merged-1",
                status="merged",
                anchor_source_url="https://example.com/merged",
                component_source_urls=["https://example.com/merged"],
                anchor_locked_at=now,
                first_seen_at=now,
                last_seen_at=now,
                has_gkg=False,
            ),
        ]
    )
    db_session.add_all(
        [
            ClusterComponentEvent(
                component_id="active-1",
                event_id="1001",
                first_seen_at=now,
                last_seen_at=now,
                is_active=True,
            ),
            ClusterComponentEvent(
                component_id="stale-1",
                event_id="1002",
                first_seen_at=now,
                last_seen_at=now,
                is_active=True,
            ),
            ClusterComponentEvent(
                component_id="merged-1",
                event_id="1003",
                first_seen_at=now,
                last_seen_at=now,
                is_active=True,
            ),
        ]
    )
    await db_session.commit()

    repo = ClusterComponentRepository(db_session)
    membership = await repo.list_active_event_membership()

    assert membership == {"active-1": {"1001"}, "stale-1": {"1002"}}


async def test_repository_deletes_terminal_components_before_cutoff(db_session) -> None:
    now = datetime(2026, 3, 25, tzinfo=UTC)
    old_terminal = datetime(2026, 3, 10, tzinfo=UTC)
    recent_terminal = datetime(2026, 3, 23, tzinfo=UTC)
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

    repo = ClusterComponentRepository(db_session)
    deleted = await repo.delete_terminal_components_before(now.replace(day=18))
    await db_session.commit()

    component_ids = {
        row.component_id
        for row in (await db_session.execute(select(ClusterComponent))).scalars().all()
    }
    memberships = (await db_session.execute(select(ClusterComponentEvent))).scalars().all()

    assert deleted == {"components": 2, "memberships": 2}
    assert component_ids == {"merged-recent", "active-old"}
    assert {(row.component_id, row.event_id) for row in memberships} == {
        ("merged-recent", "1003"),
        ("active-old", "1004"),
    }
