"""Builds and materialises enriched story clusters from events, mentions, and GKG."""

from __future__ import annotations

import time
from collections import Counter
from datetime import UTC, datetime
from hashlib import sha256
from statistics import mean
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.exceptions import ClusterBuildError
from app.core.logging import get_logger
from app.db.models import ClusterComponent, GdeltEvent, GdeltGkg, GdeltMention
from app.db.repositories.cluster_component_repository import ClusterComponentRepository
from app.db.repositories.cluster_repository import ClusterRepository
from app.db.repositories.gkg_repository import GkgRepository
from app.db.repositories.mentions_repository import MentionsRepository
from app.db.repositories.root_cluster_repository import RootClusterRepository
from app.integrations.event_enrichment_mapper import (
    compute_component_topic_score,
    compute_severity_score,
    compute_topic_score,
    get_event_root_code_label,
    get_quad_class_label,
)
from app.services.cluster_merger import ClusterMerger

logger = get_logger(__name__)

# asyncpg hard limit for bind parameters per statement; SQLite is even tighter.
_MAX_PG_ARGS = 32_767
_MAX_SQLITE_ARGS = 999


class ClusterService:
    """Orchestrates the story-cluster materialisation pipeline."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._mentions_repo = MentionsRepository(session)
        self._gkg_repo = GkgRepository(session)
        self._cluster_repo = ClusterRepository(session)
        self._cluster_component_repo = ClusterComponentRepository(session)
        self._root_cluster_repo = RootClusterRepository(session)

    async def _score_source_urls(
        self,
        since_date_added: int,
        until_date_added: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return candidate source URLs and their aggregated topic scores.

        Source URLs whose domain appears in ``cluster_source_domain_blocklist``
        are excluded before scoring. These are pure aggregators / content farms
        that produce no original journalism and pollute dominant_countries and
        themes with unrelated geographies and topics.
        """
        settings = get_settings()
        blocklist = settings.cluster_source_domain_blocklist
        section_segments = settings.cluster_section_path_segments
        require_mentions = settings.cluster_require_mentions

        filters = [
            GdeltEvent.date_added >= since_date_added,
            GdeltEvent.source_url.is_not(None),
        ]
        if until_date_added is not None:
            filters.append(GdeltEvent.date_added <= until_date_added)
        stmt = (
            select(
                GdeltEvent.source_url,
                func.count(GdeltEvent.global_event_id.distinct()).label("event_count"),
                func.coalesce(func.sum(GdeltEvent.num_articles), 0).label("num_articles"),
                func.coalesce(func.sum(GdeltEvent.num_mentions), 0).label("num_mentions"),
                func.coalesce(func.sum(GdeltEvent.num_sources), 0).label("num_sources"),
            )
            .where(*filters)
            .group_by(GdeltEvent.source_url)
        )
        result = await self._session.execute(stmt)

        candidates: list[dict[str, Any]] = []
        blocked = 0
        section_filtered = 0
        mention_filtered = 0
        for row in result.all():
            if not row.source_url:
                continue
            # Gate 0 — domain blocklist
            try:
                domain = row.source_url.split("//", 1)[1].split("/", 1)[0].lower()
            except IndexError:
                domain = ""
            if domain in blocklist:
                blocked += 1
                continue
            # Gate 1 — section/aggregator URL path pattern
            if _is_section_url(row.source_url, section_segments):
                section_filtered += 1
                continue
            # Gate 2 — zero-mention vitality gate
            if require_mentions and row.num_mentions == 0:
                mention_filtered += 1
                continue
            topic_score = compute_topic_score(
                event_count=row.event_count,
                num_articles=row.num_articles,
                num_mentions=row.num_mentions,
                num_sources=row.num_sources,
            )
            if topic_score < 3.6:
                continue
            candidates.append(
                {
                    "source_url": row.source_url,
                    "event_count": row.event_count,
                    "num_articles": row.num_articles,
                    "num_mentions": row.num_mentions,
                    "num_sources": row.num_sources,
                    "topic_score": topic_score,
                }
            )
        if blocked:
            logger.info(
                "cluster_candidates_blocked", blocked=blocked, blocklist_size=len(blocklist)
            )
        if section_filtered:
            logger.info("cluster_candidates_section_filtered", filtered=section_filtered)
        if mention_filtered:
            logger.info("cluster_candidates_mention_filtered", filtered=mention_filtered)

        candidates.sort(key=lambda item: item["topic_score"], reverse=True)
        return candidates

    async def _batch_collect_events(
        self,
        source_urls: list[str],
        since_date_added: int,
        until_date_added: int | None = None,
    ) -> dict[str, list[GdeltEvent]]:
        """Fetch events for all candidate source URLs in chunked batch queries.

        Each chunk issues one SELECT with up to _MAX_PG_ARGS − 1 bind parameters
        (one slot reserved for the date_added bound). Results are merged into a
        mapping of source_url → sorted list of GdeltEvent rows.
        """
        if not source_urls:
            return {}
        dialect = (
            self._session.bind.dialect.name if self._session.bind is not None else "postgresql"
        )
        # Reserve 1 slot for the date_added parameter (2 if until bound is present)
        slots_reserved = 2 if until_date_added is not None else 1
        chunk_size = (
            (_MAX_SQLITE_ARGS - slots_reserved)
            if dialect == "sqlite"
            else (_MAX_PG_ARGS - slots_reserved)
        )
        events_by_url: dict[str, list[GdeltEvent]] = {url: [] for url in source_urls}
        for start in range(0, len(source_urls), chunk_size):
            chunk = source_urls[start : start + chunk_size]
            filters = [
                GdeltEvent.source_url.in_(chunk),
                GdeltEvent.date_added >= since_date_added,
            ]
            if until_date_added is not None:
                filters.append(GdeltEvent.date_added <= until_date_added)
            result = await self._session.execute(
                select(GdeltEvent)
                .where(*filters)
                .order_by(GdeltEvent.date_added.asc(), GdeltEvent.global_event_id.asc())
            )
            for event in result.scalars().all():
                if event.source_url in events_by_url:
                    events_by_url[event.source_url].append(event)
        return events_by_url

    async def _batch_collect_mentions(self, event_ids: list[int]) -> dict[int, list[GdeltMention]]:
        """Fetch mentions for all event IDs in a single query.

        Returns a mapping of global_event_id → list of GdeltMention rows.
        """
        all_mentions = await self._mentions_repo.get_by_event_ids(event_ids)
        mentions_by_event: dict[int, list[GdeltMention]] = {}
        for mention in all_mentions:
            mentions_by_event.setdefault(mention.global_event_id, []).append(mention)
        return mentions_by_event

    async def _batch_collect_gkg(self, mention_identifiers: list[str]) -> dict[str, list[GdeltGkg]]:
        """Fetch GKG rows for all mention identifier URLs in a single query.

        Returns a mapping of document_identifier → list of GdeltGkg rows.
        """
        all_gkg = await self._gkg_repo.get_by_document_identifiers(mention_identifiers)
        gkg_by_identifier: dict[str, list[GdeltGkg]] = {}
        for row in all_gkg:
            if row.document_identifier:
                gkg_by_identifier.setdefault(row.document_identifier, []).append(row)
        return gkg_by_identifier

    async def _collect_windowed_events(
        self,
        since_date_added: int,
        until_date_added: int | None = None,
    ) -> list[GdeltEvent]:
        """Return windowed events that can participate in component discovery."""
        filters = [
            GdeltEvent.date_added >= since_date_added,
            GdeltEvent.source_url.is_not(None),
        ]
        if until_date_added is not None:
            filters.append(GdeltEvent.date_added <= until_date_added)
        result = await self._session.execute(
            select(GdeltEvent)
            .where(*filters)
            .order_by(GdeltEvent.date_added.asc(), GdeltEvent.global_event_id.asc())
        )
        return list(result.scalars().all())

    async def _collect_windowed_mentions(self, event_ids: list[int]) -> list[GdeltMention]:
        """Return mentions attached to the given windowed event IDs."""
        if not event_ids:
            return []
        return await self._mentions_repo.get_by_event_ids(event_ids)

    async def _build_candidate_components(
        self,
        since_date_added: int,
        until_date_added: int | None = None,
    ) -> list[dict[str, Any]]:
        """Build connected event/mention components for the candidate window."""
        events = await self._collect_windowed_events(since_date_added, until_date_added)
        if not events:
            return []

        events_by_id = {event.global_event_id: event for event in events}
        mentions = self._filter_component_mentions(
            await self._collect_windowed_mentions(list(events_by_id))
        )

        return self._derive_candidate_components(events_by_id, mentions)

    def _filter_component_mentions(self, mentions: list[GdeltMention]) -> list[GdeltMention]:
        """Filter mention nodes that should not participate in the component graph."""
        settings = get_settings()
        filtered_mentions: list[GdeltMention] = []
        for mention in mentions:
            identifier = mention.mention_identifier
            if not identifier:
                continue
            try:
                domain = identifier.split("//", 1)[1].split("/", 1)[0].lower()
            except IndexError:
                domain = ""
            if domain in settings.cluster_source_domain_blocklist:
                continue
            if _is_section_url(identifier, settings.cluster_section_path_segments):
                continue
            filtered_mentions.append(mention)
        return filtered_mentions

    def _derive_candidate_components(
        self,
        events_by_id: dict[int, GdeltEvent],
        mentions: list[GdeltMention],
    ) -> list[dict[str, Any]]:
        """Derive connected components from preloaded windowed events and mentions."""
        if not events_by_id:
            return []

        mention_to_event_ids: dict[str, set[int]] = {}
        event_to_mentions: dict[int, set[str]] = {event_id: set() for event_id in events_by_id}
        for mention in mentions:
            if not mention.mention_identifier or mention.global_event_id not in events_by_id:
                continue
            mention_to_event_ids.setdefault(mention.mention_identifier, set()).add(
                mention.global_event_id
            )
            event_to_mentions.setdefault(mention.global_event_id, set()).add(
                mention.mention_identifier
            )

        components: list[dict[str, Any]] = []
        remaining_event_ids = set(events_by_id)
        while remaining_event_ids:
            root_event_id = remaining_event_ids.pop()
            stack = [root_event_id]
            component_event_ids = {root_event_id}
            component_mentions: set[str] = set()
            component_edges: set[tuple[int, str]] = set()
            expanded_mentions: set[str] = set()

            while stack:
                current_event_id = stack.pop()
                for mention_identifier in event_to_mentions.get(current_event_id, set()):
                    component_edges.add((current_event_id, mention_identifier))
                    component_mentions.add(mention_identifier)
                    if mention_identifier in expanded_mentions:
                        continue
                    expanded_mentions.add(mention_identifier)
                    for neighbor_event_id in mention_to_event_ids.get(mention_identifier, set()):
                        component_edges.add((neighbor_event_id, mention_identifier))
                        if neighbor_event_id in component_event_ids:
                            continue
                        component_event_ids.add(neighbor_event_id)
                        remaining_event_ids.discard(neighbor_event_id)
                        stack.append(neighbor_event_id)

            if len(component_event_ids) < 2:
                continue

            components.append(
                {
                    "event_ids": component_event_ids,
                    "mention_identifiers": component_mentions,
                    "edges": component_edges,
                    "source_urls": {
                        events_by_id[event_id].source_url
                        for event_id in component_event_ids
                        if events_by_id[event_id].source_url
                    },
                }
            )

        return components

    def _compute_component_metrics(
        self,
        component: dict[str, Any],
        events: list[GdeltEvent],
    ) -> dict[str, float | int]:
        """Compute explicit structural metrics for a component candidate."""
        source_urls = {event.source_url for event in events if event.source_url}
        domains = {
            parsed.netloc.lower()
            for parsed in (urlparse(source_url) for source_url in source_urls)
            if parsed.netloc
        }
        event_count = len(component["event_ids"])
        mention_count = len(component["mention_identifiers"])
        max_possible_edges = event_count * mention_count
        edge_count = len(component.get("edges", set()))
        date_added_values = [event.date_added for event in events if event.date_added is not None]
        event_time_span_hours = 0.0
        if len(date_added_values) >= 2:
            start_dt = _parse_gdelt_timestamp(min(date_added_values))
            end_dt = _parse_gdelt_timestamp(max(date_added_values))
            event_time_span_hours = (end_dt - start_dt).total_seconds() / 3600

        return {
            "event_id_count": event_count,
            "source_url_count": len(source_urls),
            "domain_count": len(domains),
            "component_density": (edge_count / max_possible_edges) if max_possible_edges else 0.0,
            "event_time_span_hours": event_time_span_hours,
        }

    def _evaluate_component_gates(self, metrics: dict[str, float | int]) -> tuple[bool, list[str]]:
        """Return whether a component passes admission plus the failed gate names."""
        settings = get_settings()
        failed_gates: list[str] = []

        if metrics["event_id_count"] < settings.cluster_candidate_min_event_ids:
            failed_gates.append("min_event_ids")
        if metrics["source_url_count"] < settings.cluster_candidate_min_source_urls:
            failed_gates.append("min_source_urls")
        if metrics["domain_count"] < settings.cluster_candidate_min_domains:
            failed_gates.append("min_domains")
        if metrics["event_time_span_hours"] > settings.cluster_candidate_max_event_span_hours:
            failed_gates.append("max_event_span_hours")
        if metrics["component_density"] < settings.cluster_candidate_min_density:
            failed_gates.append("min_density")

        return not failed_gates, failed_gates

    def _admit_component_candidates(
        self,
        components: list[dict[str, Any]],
        events_by_id: dict[int, GdeltEvent],
    ) -> list[dict[str, Any]]:
        """Return admitted component candidates and log explicit rejection reasons."""
        admitted: list[dict[str, Any]] = []
        for component in components:
            component_events = [
                events_by_id[event_id] for event_id in sorted(component["event_ids"])
            ]
            metrics = self._compute_component_metrics(component, component_events)
            if int(metrics["event_id_count"]) < 2:
                logger.info(
                    "cluster_component_rejected",
                    component_id=_component_identifier(component),
                    metrics=metrics,
                    failed_gates=["singleton_component"],
                )
                continue
            accepted, failed_gates = self._evaluate_component_gates(metrics)
            if not accepted:
                logger.info(
                    "cluster_component_rejected",
                    component_id=_component_identifier(component),
                    metrics=metrics,
                    failed_gates=failed_gates,
                )
                continue

            admitted.append(
                {
                    "component": component,
                    "events": component_events,
                    "metrics": metrics,
                    "topic_score": compute_component_topic_score(
                        event_id_count=int(metrics["event_id_count"]),
                        source_url_count=int(metrics["source_url_count"]),
                        domain_count=int(metrics["domain_count"]),
                    ),
                }
            )

        return admitted

    def _build_cluster(
        self,
        doc: dict[str, Any],
        events: list[GdeltEvent],
        mentions: list[GdeltMention],
        gkg_rows: list[GdeltGkg],
    ) -> dict[str, Any]:
        """Build a single materialised cluster row from one admitted component.

        ``gkg_rows`` may contain rows for all source URLs represented in the component.
        Missing GKG coverage for some URLs is acceptable; the cluster is enriched from
        whatever component-local GKG rows are available without reaching outside the
        component boundary.
        """
        source_url = doc["source_url"]
        # cluster_id is derived from the sorted component event IDs so the same story
        # keeps the same identity across reruns even if its representative source_url
        # changes or the strongest URL shifts over time.
        cluster_id = doc["cluster_id"]

        event_ids = [str(event.global_event_id) for event in events]
        event_type_labels = [
            get_event_root_code_label(event.event_root_code)
            for event in events
            if event.event_root_code is not None
        ]
        quad_labels = [get_quad_class_label(event.quad_class) for event in events]
        severities = [
            compute_severity_score(event.quad_class, event.goldstein_scale, event.avg_tone)
            for event in events
        ]
        # Use only action_geo_country_code (FIPS 2-char) to avoid mixing standards:
        # actor1/2_country_code are CAMEO/ISO-3 (3-char) while action_geo_country_code
        # is FIPS (2-char). Combining them causes the same country to be counted separately
        # ("US" vs "USA") and breaks country-code filtering in the clusters API.
        country_values = [
            event.action_geo_country_code for event in events if event.action_geo_country_code
        ]
        location_values = [
            event.action_geo_full_name for event in events if event.action_geo_full_name
        ]

        mention_sources = sorted({m.mention_source_name for m in mentions if m.mention_source_name})
        mention_identifiers = sorted(
            {m.mention_identifier for m in mentions if m.mention_identifier}
        )
        mention_times = [
            _parse_gdelt_timestamp(m.mention_time_date)
            for m in mentions
            if m.mention_time_date is not None
        ]

        themes = _sorted_unique(item for row in gkg_rows for item in (row.themes or []))
        persons = _sorted_unique(item for row in gkg_rows for item in (row.persons or []))
        organizations = _sorted_unique(
            item for row in gkg_rows for item in (row.organizations or [])
        )
        gkg_locations = _sorted_unique(item for row in gkg_rows for item in (row.locations or []))
        tones = [row.document_tone for row in gkg_rows if row.document_tone is not None]

        return {
            "cluster_id": cluster_id,
            "source_url": source_url,
            "event_count": doc["event_count"],
            "num_articles": doc["num_articles"],
            "num_mentions": doc["num_mentions"],
            "num_sources": doc["num_sources"],
            "topic_score": doc["topic_score"],
            "event_ids": event_ids,
            "event_date_ref_start": min(e.sql_date for e in events) if events else None,
            "event_date_ref_end": max(e.sql_date for e in events) if events else None,
            "dominant_event_types": _top_values(event_type_labels),
            "dominant_quad_classes": _top_values(quad_labels),
            "avg_severity_score": round(mean(severities), 2) if severities else None,
            "dominant_countries": _top_values(country_values),
            "dominant_locations": _top_values(location_values),
            "mention_count": len(mention_identifiers),
            "distinct_mention_sources": mention_sources,
            "mention_identifiers": mention_identifiers,
            "first_mention_at": min(mention_times) if mention_times else None,
            "last_mention_at": max(mention_times) if mention_times else None,
            "themes": themes,
            "persons": persons,
            "organizations": organizations,
            "gkg_locations": gkg_locations,
            # gkg_doc_count tracks how many GKG documents contribute to document_tone_avg so
            # ClusterMerger can compute a weighted mean rather than a mean-of-means after fusion
            "gkg_doc_count": len(tones),
            "document_tone_avg": round(mean(tones), 2) if tones else None,
            "computed_at": datetime.now(UTC),
        }

    async def _build_fallback_source_url_cluster_rows(
        self,
        since_date_added: int,
        until_date_added: int | None,
    ) -> list[dict[str, Any]]:
        """Build legacy single-source cluster rows when no component candidates survive."""
        candidates = await self._score_source_urls(since_date_added, until_date_added)
        if not candidates:
            return []

        source_urls = [candidate["source_url"] for candidate in candidates]
        events_by_url = await self._batch_collect_events(
            source_urls, since_date_added, until_date_added
        )
        event_ids = [event.global_event_id for events in events_by_url.values() for event in events]
        mentions_by_event = await self._batch_collect_mentions(event_ids)
        source_gkg_by_url = await self._batch_collect_gkg(source_urls)

        cluster_rows: list[dict[str, Any]] = []
        for candidate in candidates:
            source_url = candidate["source_url"]
            events = events_by_url.get(source_url, [])
            mentions = [
                mention
                for event in events
                for mention in mentions_by_event.get(event.global_event_id, [])
            ]
            cluster_row = self._build_cluster(
                {
                    "source_url": source_url,
                    "cluster_id": _component_cluster_id(
                        [event.global_event_id for event in events]
                    ),
                    "event_count": len(events),
                    "num_articles": sum(event.num_articles or 0 for event in events),
                    "num_mentions": sum(event.num_mentions or 0 for event in events),
                    "num_sources": sum(event.num_sources or 0 for event in events),
                    "topic_score": candidate["topic_score"],
                },
                events,
                mentions,
                source_gkg_by_url.get(source_url, []),
            )
            cluster_row["component_source_urls"] = [source_url]
            cluster_row["component_domains"] = [urlparse(source_url).netloc.lower()]
            cluster_rows.append(cluster_row)

        return cluster_rows

    def _build_reconciliation_payloads(
        self,
        story_cluster_rows: list[dict[str, Any]],
        root_cluster_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Return current-run payloads for persistent-component reconciliation."""
        payloads: list[dict[str, Any]] = []
        for table_name, rows in (
            ("story_clusters", story_cluster_rows),
            ("root_clusters", root_cluster_rows),
        ):
            for row in rows:
                payloads.append(
                    {
                        "cluster_id": row["cluster_id"],
                        "table_name": table_name,
                        "source_url": row["source_url"],
                        "event_ids": {str(event_id) for event_id in (row.get("event_ids") or [])},
                        "computed_at": row.get("computed_at"),
                        "has_gkg": bool(row.get("gkg_doc_count") or row.get("themes")),
                        "merge_evidence": row.get("merge_evidence"),
                        "component_source_urls": list(
                            row.get("component_source_urls") or [row["source_url"]]
                        ),
                    }
                )
        return payloads

    def _event_overlap(
        self,
        current_event_ids: set[str],
        historical_event_ids: set[str],
    ) -> dict[str, float | int]:
        """Return overlap metrics between current and historical component membership."""
        overlap_count = len(current_event_ids & historical_event_ids)
        historical_ratio = (
            overlap_count / len(historical_event_ids) if historical_event_ids else 0.0
        )
        current_ratio = overlap_count / len(current_event_ids) if current_event_ids else 0.0
        return {
            "count": overlap_count,
            "historical_ratio": historical_ratio,
            "current_ratio": current_ratio,
        }

    def _choose_canonical_component(self, candidates: list[dict[str, Any]]) -> dict[str, Any]:
        """Choose the oldest persistent component as the canonical continuity key."""
        return min(
            candidates,
            key=lambda candidate: (
                candidate["first_seen_at"],
                candidate["component_id"],
            ),
        )

    def _find_matching_components(
        self,
        current_payload: dict[str, Any],
        persisted_components: list[dict[str, Any]],
        active_membership: dict[str, set[str]],
    ) -> list[dict[str, Any]]:
        """Return historical components sharing at least one active event with the current payload."""
        matches: list[dict[str, Any]] = []
        current_event_ids = current_payload["event_ids"]
        current_source_url = current_payload.get("source_url")
        for component in persisted_components:
            overlap = self._event_overlap(
                current_event_ids,
                active_membership.get(component["component_id"], set()),
            )
            if int(overlap["count"]) == 0:
                continue
            matches.append({**component, "overlap": overlap})
        if not matches and current_source_url:
            for component in persisted_components:
                source_urls = set(component.get("component_source_urls") or [])
                if (
                    current_source_url != component.get("anchor_source_url")
                    and current_source_url not in source_urls
                ):
                    continue
                matches.append(
                    {
                        **component,
                        "overlap": {"count": 0, "historical_ratio": 0.0, "current_ratio": 0.0},
                    }
                )
        matches.sort(
            key=lambda match: (
                -int(match["overlap"]["count"]),
                match["first_seen_at"],
                match["component_id"],
            )
        )
        return matches

    def _find_split_candidates(
        self,
        historical_component: dict[str, Any],
        current_payloads: list[dict[str, Any]],
        active_membership: dict[str, set[str]],
        *,
        overlap_min: int,
        overlap_ratio: float,
    ) -> list[dict[str, Any]]:
        """Return current payloads that capture enough historical membership to indicate a split."""
        historical_event_ids = active_membership.get(historical_component["component_id"], set())
        split_candidates: list[dict[str, Any]] = []
        for payload in current_payloads:
            overlap = self._event_overlap(payload["event_ids"], historical_event_ids)
            if int(overlap["count"]) < overlap_min:
                continue
            if float(overlap["historical_ratio"]) < overlap_ratio:
                continue
            split_candidates.append({**payload, "overlap": overlap})
        split_candidates.sort(
            key=lambda candidate: (
                -float(candidate["overlap"]["historical_ratio"]),
                -int(candidate["overlap"]["count"]),
                candidate["cluster_id"],
            )
        )
        return split_candidates

    def _should_mark_component_split(
        self,
        historical_component: dict[str, Any],
        current_payloads: list[dict[str, Any]],
        active_membership: dict[str, set[str]],
        *,
        overlap_min: int,
        continuity_ratio: float,
    ) -> bool:
        """Return whether history branches without a dominant continuity match."""
        historical_event_ids = active_membership.get(historical_component["component_id"], set())
        meaningful_overlaps = [
            self._event_overlap(payload["event_ids"], historical_event_ids)
            for payload in current_payloads
        ]
        meaningful_overlaps = [
            overlap for overlap in meaningful_overlaps if int(overlap["count"]) >= overlap_min
        ]
        if len(meaningful_overlaps) < 2:
            return False
        return (
            max(float(overlap["historical_ratio"]) for overlap in meaningful_overlaps)
            < continuity_ratio
        )

    def _serialize_reconcilable_component(self, component: Any) -> dict[str, Any]:
        """Return the reconciliation fields needed from a persisted component row."""
        return {
            "component_id": component.component_id,
            "first_seen_at": component.first_seen_at,
            "anchor_source_url": component.anchor_source_url,
            "component_source_urls": list(component.component_source_urls or []),
            "status": component.status,
            "missing_run_count": component.missing_run_count,
            "current_cluster_id": component.current_cluster_id,
            "current_table": component.current_table,
        }

    async def _delete_previous_materialization_if_needed(
        self,
        component: dict[str, Any],
        payload: dict[str, Any],
    ) -> None:
        """Remove a previous materialized row when the component now points elsewhere."""
        previous_cluster_id = component.get("current_cluster_id")
        previous_table = component.get("current_table")
        if not previous_cluster_id or not previous_table:
            return
        if previous_cluster_id == payload["cluster_id"] and previous_table == payload["table_name"]:
            return
        if previous_table == "story_clusters":
            await self._cluster_repo.delete_by_cluster_ids({previous_cluster_id})
        elif previous_table == "root_clusters":
            await self._root_cluster_repo.delete_by_cluster_ids({previous_cluster_id})

    async def _reconcile_persistent_components(
        self,
        reconciliation_payloads: list[dict[str, Any]],
    ) -> None:
        """Reconcile current materialized rows against persistent component state."""
        settings = get_settings()
        split_observed_at = max(
            (
                payload["computed_at"]
                for payload in reconciliation_payloads
                if payload.get("computed_at")
            ),
            default=datetime.now(UTC),
        )
        persisted_components = [
            self._serialize_reconcilable_component(component)
            for component in await self._cluster_component_repo.list_reconcilable_components()
        ]
        active_membership = await self._cluster_component_repo.list_active_event_membership()
        split_component_ids: set[str] = set()
        for component in persisted_components:
            if not self._should_mark_component_split(
                component,
                reconciliation_payloads,
                active_membership,
                overlap_min=settings.cluster_component_split_overlap_min,
                continuity_ratio=settings.cluster_component_split_overlap_ratio,
            ):
                continue
            split_component_ids.add(component["component_id"])
            await self._delete_previous_materialization_if_needed(
                component,
                {
                    "cluster_id": None,
                    "table_name": None,
                },
            )
            await self._cluster_component_repo.mark_split(
                component["component_id"], split_observed_at
            )

        matched_component_ids: set[str] = set()
        for payload in reconciliation_payloads:
            observed_at = payload["computed_at"] or datetime.now(UTC)
            matches = self._find_matching_components(
                payload,
                [
                    component
                    for component in persisted_components
                    if component["component_id"] not in split_component_ids
                ],
                active_membership,
            )
            if not matches:
                component_id = await self._cluster_component_repo.create_component(
                    anchor_source_url=payload["source_url"],
                    component_source_urls=payload["component_source_urls"],
                    seed_event_ids=sorted(payload["event_ids"]),
                    event_ids=sorted(payload["event_ids"]),
                    observed_at=observed_at,
                    has_gkg=payload["has_gkg"],
                    merge_evidence=payload["merge_evidence"],
                )
                payload["component_id"] = component_id
                if not payload["has_gkg"]:
                    logger.info(
                        "cluster_component_no_gkg",
                        component_id=component_id,
                        cluster_id=payload["cluster_id"],
                        source_urls=payload["component_source_urls"],
                    )
                persisted_components.append(
                    {
                        "component_id": component_id,
                        "first_seen_at": observed_at,
                        "anchor_source_url": payload["source_url"],
                        "component_source_urls": payload["component_source_urls"],
                        "status": "active",
                        "missing_run_count": 0,
                        "current_cluster_id": None,
                        "current_table": None,
                    }
                )
                active_membership[component_id] = set(payload["event_ids"])
                matched_component_ids.add(component_id)
                continue

            canonical = self._choose_canonical_component(matches)
            payload["component_id"] = canonical["component_id"]
            matched_component_ids.add(canonical["component_id"])
            await self._delete_previous_materialization_if_needed(canonical, payload)
            await self._cluster_component_repo.mark_active(
                canonical["component_id"],
                observed_at,
                has_gkg=payload["has_gkg"],
                merge_evidence=payload["merge_evidence"],
                component_source_urls=payload["component_source_urls"],
            )
            if not payload["has_gkg"]:
                logger.info(
                    "cluster_component_no_gkg",
                    component_id=canonical["component_id"],
                    cluster_id=payload["cluster_id"],
                    source_urls=payload["component_source_urls"],
                )
            await self._cluster_component_repo.replace_active_event_membership(
                canonical["component_id"],
                sorted(payload["event_ids"]),
                observed_at,
            )
            active_membership[canonical["component_id"]] = set(payload["event_ids"])

            for match in matches:
                if match["component_id"] == canonical["component_id"]:
                    continue
                await self._delete_previous_materialization_if_needed(
                    match,
                    {
                        "cluster_id": None,
                        "table_name": None,
                    },
                )
                await self._cluster_component_repo.mark_merged_into(
                    match["component_id"],
                    canonical["component_id"],
                    observed_at,
                )
                matched_component_ids.add(match["component_id"])
                active_membership.pop(match["component_id"], None)

        for component in persisted_components:
            component_id = component["component_id"]
            if component_id in matched_component_ids or component_id in split_component_ids:
                continue
            missing_run_count = int(component["missing_run_count"]) + 1
            if missing_run_count >= settings.cluster_component_stale_after_missing_runs:
                await self._cluster_component_repo.mark_stale(component_id, missing_run_count)
            else:
                await self._cluster_component_repo.update_missing_run_count(
                    component_id,
                    missing_run_count,
                )

    async def _validate_materialized_consistency(self) -> None:
        """Fail the current run if materialized tables or soft links are inconsistent."""
        story_ids = await self._cluster_repo.list_cluster_ids()
        root_ids = await self._root_cluster_repo.list_cluster_ids()
        overlap_ids = story_ids & root_ids
        if overlap_ids:
            raise ClusterBuildError(
                "Materialized cluster consistency check failed",
                detail=f"duplicate cluster_id across story/root tables: {sorted(overlap_ids)[0]}",
            )

        result = await self._session.execute(
            select(ClusterComponent).where(ClusterComponent.status == "active")
        )
        for component in result.scalars().all():
            if not component.current_cluster_id or not component.current_table:
                raise ClusterBuildError(
                    "Materialized cluster consistency check failed",
                    detail=f"active component missing soft link: {component.component_id}",
                )
            if component.current_table == "story_clusters":
                exists = await self._cluster_repo.exists_by_cluster_id(component.current_cluster_id)
            elif component.current_table == "root_clusters":
                exists = await self._root_cluster_repo.exists_by_cluster_id(
                    component.current_cluster_id
                )
            else:
                raise ClusterBuildError(
                    "Materialized cluster consistency check failed",
                    detail=f"active component points to unknown table: {component.component_id}",
                )
            if not exists:
                raise ClusterBuildError(
                    "Materialized cluster consistency check failed",
                    detail=f"active component points to missing cluster row: {component.component_id}",
                )

    async def build_and_materialise(
        self,
        since_dt: datetime | int,
        until_dt: datetime | int | None = None,
    ) -> int:
        """Build and persist story clusters for admitted component candidates.

        Filters events by ``date_added`` (GDELT YYYYMMDDHHMMSS integer) to honour
        sub-day precision — e.g. a 36-hour rolling window starting mid-day.

        ``until_dt`` is optional. When provided, only events within the closed
        [since_dt, until_dt] window are considered. Useful for backfill runs that
        need to process a fixed slice without bleeding into later data.

        Uses batched event, mention, and GKG collection so candidate discovery and
        materialisation operate on connected components rather than legacy source URLs.
        """
        since_date_added = _normalize_since_date_added(since_dt)
        until_date_added = _normalize_since_date_added(until_dt) if until_dt is not None else None
        t0 = time.monotonic()
        try:
            windowed_events = await self._collect_windowed_events(
                since_date_added, until_date_added
            )
            if not windowed_events:
                await self._reconcile_persistent_components([])
                await self._validate_materialized_consistency()
                return 0

            events_by_id = {event.global_event_id: event for event in windowed_events}
            t_events = time.monotonic()
            logger.info(
                "cluster_phase_events",
                total_events=len(windowed_events),
                elapsed_s=round(t_events - t0, 2),
            )

            windowed_mentions = self._filter_component_mentions(
                await self._collect_windowed_mentions(list(events_by_id))
            )
            mentions_by_event: dict[int, list[GdeltMention]] = {}
            for mention in windowed_mentions:
                mentions_by_event.setdefault(mention.global_event_id, []).append(mention)
            t_mentions = time.monotonic()
            logger.info(
                "cluster_phase_mentions",
                total_mentions=len(windowed_mentions),
                elapsed_s=round(t_mentions - t_events, 2),
            )

            components = self._derive_candidate_components(events_by_id, windowed_mentions)
            admitted_candidates = self._admit_component_candidates(components, events_by_id)
            logger.info(
                "cluster_phase_components",
                discovered=len(components),
                admitted=len(admitted_candidates),
                elapsed_s=round(time.monotonic() - t_mentions, 2),
            )

            cluster_rows: list[dict[str, Any]] = []
            if admitted_candidates:
                component_source_urls = sorted(
                    {
                        source_url
                        for candidate in admitted_candidates
                        for source_url in candidate["component"]["source_urls"]
                    }
                )
                source_gkg_by_url = await self._batch_collect_gkg(component_source_urls)
                total_gkg = sum(len(v) for v in source_gkg_by_url.values())
                t_gkg = time.monotonic()
                logger.info(
                    "cluster_phase_gkg",
                    unique_identifiers=len(component_source_urls),
                    total_gkg=total_gkg,
                    elapsed_s=round(t_gkg - t_mentions, 2),
                )

                # ── Assemble per-component cluster dicts (pure Python, no more I/O) ──
                for candidate in admitted_candidates:
                    component = candidate["component"]
                    events = candidate["events"]
                    component_urls = sorted(component["source_urls"])
                    representative_url = component_urls[0]
                    mentions: list[GdeltMention] = [
                        mention
                        for event in events
                        for mention in mentions_by_event.get(event.global_event_id, [])
                    ]
                    source_gkg_rows = [
                        row
                        for source_url in component_urls
                        for row in source_gkg_by_url.get(source_url, [])
                    ]
                    cluster_row = self._build_cluster(
                        {
                            "source_url": representative_url,
                            "cluster_id": _component_cluster_id(component["event_ids"]),
                            "event_count": len(component["event_ids"]),
                            "num_articles": sum(event.num_articles or 0 for event in events),
                            "num_mentions": sum(event.num_mentions or 0 for event in events),
                            "num_sources": sum(event.num_sources or 0 for event in events),
                            "topic_score": candidate["topic_score"],
                        },
                        events,
                        mentions,
                        source_gkg_rows,
                    )
                    cluster_row["component_source_urls"] = component_urls
                    cluster_row["component_domains"] = sorted(
                        {
                            parsed.netloc.lower()
                            for parsed in (urlparse(source_url) for source_url in component_urls)
                            if parsed.netloc
                        }
                    )
                    cluster_rows.append(cluster_row)
            else:
                t_gkg = time.monotonic()
                cluster_rows = await self._build_fallback_source_url_cluster_rows(
                    since_date_added,
                    until_date_added,
                )
                logger.info(
                    "cluster_phase_gkg",
                    unique_identifiers=len({row["source_url"] for row in cluster_rows}),
                    total_gkg=sum(row.get("gkg_doc_count") or 0 for row in cluster_rows),
                    elapsed_s=round(time.monotonic() - t_mentions, 2),
                )
            t_build = time.monotonic()
            logger.info(
                "cluster_phase_build",
                cluster_rows=len(cluster_rows),
                elapsed_s=round(t_build - t_gkg, 2),
            )

            # ── Merge semantically related clusters ──────────────────────────────
            # Merge tuning stays explicit in settings so stricter overlap, Jaccard, and
            # temporal gates can be tuned without changing the merger implementation.
            merger = ClusterMerger(
                mention_overlap_min=get_settings().cluster_merge_mention_overlap_min,
                jaccard_threshold=get_settings().cluster_merge_jaccard_threshold,
                max_themes_for_jaccard=get_settings().cluster_merge_max_themes_for_jaccard,
                max_merge_day_gap=get_settings().cluster_max_merge_day_gap,
                max_theme_df=get_settings().cluster_merge_max_theme_df,
            )
            cluster_rows = merger.merge(cluster_rows)
            t_merge = time.monotonic()
            logger.info(
                "cluster_phase_merge",
                merged_rows=len(cluster_rows),
                elapsed_s=round(t_merge - t_build, 2),
            )

            settings = get_settings()
            root_cluster_rows = [
                cluster
                for cluster in cluster_rows
                if cluster["event_count"] > settings.root_cluster_min_event_count
            ]
            story_cluster_rows = [
                cluster
                for cluster in cluster_rows
                if cluster["event_count"] <= settings.root_cluster_min_event_count
            ]
            logger.info(
                "cluster_phase_partition",
                root_clusters=len(root_cluster_rows),
                story_clusters=len(story_cluster_rows),
                threshold=settings.root_cluster_min_event_count,
            )

            reconciliation_payloads = self._build_reconciliation_payloads(
                story_cluster_rows, root_cluster_rows
            )
            await self._reconcile_persistent_components(reconciliation_payloads)

            inserted = 0
            if story_cluster_rows:
                inserted += await self._cluster_repo.bulk_upsert(story_cluster_rows)
                logger.info("story_clusters_materialised", count=len(story_cluster_rows))

            if root_cluster_rows:
                inserted += await self._root_cluster_repo.bulk_upsert(root_cluster_rows)
                logger.info("root_clusters_materialised", count=len(root_cluster_rows))

            deleted_from_root = await self._root_cluster_repo.delete_by_cluster_ids(
                {cluster["cluster_id"] for cluster in story_cluster_rows}
            )
            deleted_from_story = await self._cluster_repo.delete_by_cluster_ids(
                {cluster["cluster_id"] for cluster in root_cluster_rows}
            )
            logger.info(
                "cluster_phase_reconcile",
                deleted_from_root=deleted_from_root,
                deleted_from_story=deleted_from_story,
            )

            for payload in reconciliation_payloads:
                if not payload.get("component_id"):
                    continue
                await self._cluster_component_repo.update_current_materialization(
                    component_id=payload["component_id"],
                    cluster_id=payload["cluster_id"],
                    table_name=payload["table_name"],
                    computed_at=payload["computed_at"],
                )

            await self._validate_materialized_consistency()

            t_upsert = time.monotonic()
            logger.info(
                "clusters_materialised",
                count=inserted,
                since_date_added=since_date_added,
                elapsed_s=round(t_upsert - t_merge, 2),
                total_elapsed_s=round(t_upsert - t0, 2),
            )
            return inserted
        except Exception as exc:  # pragma: no cover - covered via raised domain error
            raise ClusterBuildError("Failed to build story clusters", detail=str(exc)) from exc


def _top_values(values: list[str], limit: int = 5) -> list[str]:
    """Return the most common non-empty values in deterministic order."""
    counts = Counter(value for value in values if value and value != "Sconosciuto")
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [value for value, _count in ranked[:limit]]


def _sorted_unique(values: Any) -> list[str]:
    """Return sorted unique string values from an iterable."""
    return sorted({value for value in values if value})


def _normalize_since_date_added(value: datetime | int) -> int:
    """Convert datetime or CLI-style integer inputs into DATEADDED format."""
    if isinstance(value, datetime):
        return int(value.strftime("%Y%m%d%H%M%S"))

    normalized = str(value)
    if len(normalized) == 8:
        return int(f"{normalized}000000")
    if len(normalized) == 14:
        return int(normalized)

    raise ValueError("since_dt must be a datetime or a YYYYMMDD / YYYYMMDDHHMMSS integer")


def _parse_gdelt_timestamp(value: int) -> datetime:
    """Convert a GDELT YYYYMMDDHHMMSS integer timestamp into UTC datetime."""
    return datetime.strptime(str(value), "%Y%m%d%H%M%S").replace(tzinfo=UTC)


def _component_identifier(component: dict[str, Any]) -> str:
    """Return a stable identifier for logging component-level decisions."""
    return _component_cluster_id(component["event_ids"])


def _component_cluster_id(event_ids: set[int] | list[int]) -> str:
    """Return a deterministic cluster identifier from sorted event IDs."""
    normalized = ",".join(str(event_id) for event_id in sorted(event_ids))
    return sha256(normalized.encode()).hexdigest()[:24]


def _is_section_url(url: str, segments: tuple[str, ...]) -> bool:
    """Return True if the URL path contains any of the given section path segments.

    Matching is case-insensitive. An empty ``segments`` tuple always returns False.
    Used to discard aggregator/archive/tag pages before cluster scoring.
    """
    if not segments:
        return False
    url_lower = url.lower()
    return any(seg.lower() in url_lower for seg in segments)
