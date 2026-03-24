"""Repository for persistent cluster component identities and event membership."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import ClusterComponent, ClusterComponentEvent


class ClusterComponentRepository:
    """Data access layer for cluster component persistence and reconciliation state."""

    _RECONCILABLE_STATUSES: tuple[str, ...] = ("active", "stale")

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_component(
        self,
        *,
        anchor_source_url: str,
        component_source_urls: list[str],
        seed_event_ids: list[str] | None,
        event_ids: list[str],
        observed_at: datetime,
        has_gkg: bool,
        merge_evidence: list[Any] | dict[str, Any] | None = None,
    ) -> str:
        """Create a new active component and its active membership rows."""
        component_id = str(uuid.uuid4())
        component = ClusterComponent(
            component_id=component_id,
            status="active",
            anchor_source_url=anchor_source_url,
            component_source_urls=component_source_urls,
            anchor_locked_at=observed_at,
            seed_event_ids=seed_event_ids,
            first_seen_at=observed_at,
            last_seen_at=observed_at,
            missing_run_count=0,
            has_gkg=has_gkg,
            merge_evidence=merge_evidence,
        )
        self._session.add(component)
        self._session.add_all(
            [
                ClusterComponentEvent(
                    component_id=component_id,
                    event_id=event_id,
                    first_seen_at=observed_at,
                    last_seen_at=observed_at,
                    is_active=True,
                )
                for event_id in dict.fromkeys(event_ids)
            ]
        )
        await self._session.flush()
        return component_id

    async def get_by_component_id(self, component_id: str) -> ClusterComponent | None:
        """Return one component by immutable component_id."""
        result = await self._session.execute(
            select(ClusterComponent).where(ClusterComponent.component_id == component_id)
        )
        return result.scalar_one_or_none()

    async def list_reconcilable_components(self) -> list[ClusterComponent]:
        """Return active and stale components that can be matched against a new run."""
        result = await self._session.execute(
            select(ClusterComponent)
            .where(ClusterComponent.status.in_(self._RECONCILABLE_STATUSES))
            .order_by(ClusterComponent.first_seen_at.asc())
        )
        return list(result.scalars().all())

    async def list_active_event_membership(self) -> dict[str, set[str]]:
        """Return active event membership keyed by component_id."""
        result = await self._session.execute(
            select(ClusterComponentEvent)
            .join(
                ClusterComponent,
                ClusterComponent.component_id == ClusterComponentEvent.component_id,
            )
            .where(ClusterComponentEvent.is_active.is_(True))
            .where(ClusterComponent.status.in_(self._RECONCILABLE_STATUSES))
        )
        membership: dict[str, set[str]] = {}
        for row in result.scalars().all():
            membership.setdefault(row.component_id, set()).add(row.event_id)
        return membership

    async def _deactivate_active_memberships(
        self,
        component_id: str,
        observed_at: datetime,
    ) -> None:
        """Deactivate active memberships for a component while preserving history."""
        result = await self._session.execute(
            select(ClusterComponentEvent).where(
                ClusterComponentEvent.component_id == component_id,
                ClusterComponentEvent.is_active.is_(True),
            )
        )
        for membership in result.scalars().all():
            membership.is_active = False
            membership.last_seen_at = observed_at

    async def replace_active_event_membership(
        self,
        component_id: str,
        event_ids: list[str],
        observed_at: datetime,
    ) -> None:
        """Replace the active membership set while preserving membership history."""
        memberships_result = await self._session.execute(
            select(ClusterComponentEvent).where(ClusterComponentEvent.component_id == component_id)
        )
        memberships = list(memberships_result.scalars().all())
        membership_by_event_id = {membership.event_id: membership for membership in memberships}
        active_event_ids = set(dict.fromkeys(event_ids))

        for membership in memberships:
            if membership.event_id in active_event_ids:
                membership.is_active = True
                membership.last_seen_at = observed_at
            elif membership.is_active:
                membership.is_active = False
                membership.last_seen_at = observed_at

        for event_id in active_event_ids:
            if event_id in membership_by_event_id:
                continue
            self._session.add(
                ClusterComponentEvent(
                    component_id=component_id,
                    event_id=event_id,
                    first_seen_at=observed_at,
                    last_seen_at=observed_at,
                    is_active=True,
                )
            )

        component = await self.get_by_component_id(component_id)
        if component is not None:
            component.last_seen_at = observed_at
            component.missing_run_count = 0

    async def mark_active(
        self,
        component_id: str,
        observed_at: datetime,
        *,
        has_gkg: bool | None = None,
        merge_evidence: list[Any] | dict[str, Any] | None = None,
        component_source_urls: list[str] | None = None,
    ) -> None:
        """Mark a component active after matching it in the current run."""
        component = await self.get_by_component_id(component_id)
        if component is None:
            return

        component.status = "active"
        component.last_seen_at = observed_at
        component.missing_run_count = 0
        component.merged_into_component_id = None
        if has_gkg is not None:
            component.has_gkg = has_gkg
        if merge_evidence is not None:
            component.merge_evidence = merge_evidence
        if component_source_urls is not None:
            component.component_source_urls = component_source_urls

    async def mark_stale(self, component_id: str, missing_run_count: int) -> None:
        """Mark a component stale after enough missed runs."""
        component = await self.get_by_component_id(component_id)
        if component is None:
            return

        component.status = "stale"
        component.missing_run_count = missing_run_count

    async def update_missing_run_count(self, component_id: str, missing_run_count: int) -> None:
        """Update missing-run count without forcing a terminal status transition."""
        component = await self.get_by_component_id(component_id)
        if component is None:
            return

        component.missing_run_count = missing_run_count

    async def mark_merged_into(
        self,
        component_id: str,
        target_component_id: str,
        observed_at: datetime,
    ) -> None:
        """Mark a component as merged into another canonical component."""
        component = await self.get_by_component_id(component_id)
        if component is None:
            return

        component.status = "merged"
        component.merged_into_component_id = target_component_id
        component.last_seen_at = observed_at
        component.current_cluster_id = None
        component.current_table = None
        component.current_computed_at = None
        await self._deactivate_active_memberships(component_id, observed_at)

    async def mark_split(self, component_id: str, observed_at: datetime) -> None:
        """Mark a component as explicitly split in the latest reconciliation pass."""
        component = await self.get_by_component_id(component_id)
        if component is None:
            return

        component.status = "split"
        component.last_seen_at = observed_at
        component.current_cluster_id = None
        component.current_table = None
        component.current_computed_at = None
        await self._deactivate_active_memberships(component_id, observed_at)

    async def update_current_materialization(
        self,
        *,
        component_id: str,
        cluster_id: str | None,
        table_name: str | None,
        computed_at: datetime | None,
    ) -> None:
        """Update the latest soft reference to the materialized cluster row."""
        component = await self.get_by_component_id(component_id)
        if component is None:
            return

        component.current_cluster_id = cluster_id
        component.current_table = table_name
        component.current_computed_at = computed_at

    async def delete_terminal_components_before(self, cutoff: datetime) -> dict[str, int]:
        """Delete terminal components and memberships older than the cutoff."""
        result = await self._session.execute(
            select(ClusterComponent.component_id).where(
                ClusterComponent.status.in_(("merged", "split")),
                ClusterComponent.last_seen_at < cutoff,
            )
        )
        component_ids = list(result.scalars().all())
        if not component_ids:
            return {"components": 0, "memberships": 0}

        deleted_memberships = await self._session.execute(
            delete(ClusterComponentEvent).where(
                ClusterComponentEvent.component_id.in_(component_ids)
            )
        )
        deleted_components = await self._session.execute(
            delete(ClusterComponent).where(ClusterComponent.component_id.in_(component_ids))
        )
        return {
            "components": int(deleted_components.rowcount or 0),
            "memberships": int(deleted_memberships.rowcount or 0),
        }
