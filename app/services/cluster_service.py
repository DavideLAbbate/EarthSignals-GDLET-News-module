"""Builds and materialises enriched story clusters from events, mentions, and GKG."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from hashlib import sha256
from statistics import mean
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import ClusterBuildError
from app.core.logging import get_logger
from app.db.models import GdeltEvent, GdeltGkg, GdeltMention
from app.db.repositories.cluster_repository import ClusterRepository
from app.db.repositories.gkg_repository import GkgRepository
from app.db.repositories.mentions_repository import MentionsRepository
from app.integrations.event_enrichment_mapper import (
    compute_severity_score,
    compute_topic_score,
    get_event_root_code_label,
    get_quad_class_label,
)

logger = get_logger(__name__)


class ClusterService:
    """Orchestrates the story-cluster materialisation pipeline."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._mentions_repo = MentionsRepository(session)
        self._gkg_repo = GkgRepository(session)
        self._cluster_repo = ClusterRepository(session)

    async def _score_source_urls(self, since_sqldate: int) -> list[dict[str, Any]]:
        """Return candidate source URLs and their aggregated topic scores."""
        stmt = (
            select(
                GdeltEvent.source_url,
                func.count(GdeltEvent.global_event_id.distinct()).label("event_count"),
                func.coalesce(func.sum(GdeltEvent.num_articles), 0).label("num_articles"),
                func.coalesce(func.sum(GdeltEvent.num_mentions), 0).label("num_mentions"),
                func.coalesce(func.sum(GdeltEvent.num_sources), 0).label("num_sources"),
            )
            .where(
                GdeltEvent.sql_date >= since_sqldate,
                GdeltEvent.source_url.is_not(None),
            )
            .group_by(GdeltEvent.source_url)
        )
        result = await self._session.execute(stmt)

        candidates: list[dict[str, Any]] = []
        for row in result.all():
            if not row.source_url:
                continue
            topic_score = compute_topic_score(
                event_count=row.event_count,
                num_articles=row.num_articles,
                num_mentions=row.num_mentions,
                num_sources=row.num_sources,
            )
            if topic_score < 4.0:
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

        candidates.sort(key=lambda item: item["topic_score"], reverse=True)
        return candidates

    async def _collect_events(self, source_url: str, since_sqldate: int) -> list[GdeltEvent]:
        """Return all recent events for the given source URL."""
        result = await self._session.execute(
            select(GdeltEvent)
            .where(
                GdeltEvent.source_url == source_url,
                GdeltEvent.sql_date >= since_sqldate,
            )
            .order_by(GdeltEvent.date_added.asc(), GdeltEvent.global_event_id.asc())
        )
        return list(result.scalars().all())

    async def _collect_mentions(self, event_ids: list[int]) -> list[GdeltMention]:
        """Return all mentions associated with the given event IDs."""
        return await self._mentions_repo.get_by_event_ids(event_ids)

    async def _collect_gkg(self, mention_identifiers: list[str]) -> list[GdeltGkg]:
        """Return all GKG rows associated with the given mention URLs."""
        return await self._gkg_repo.get_by_document_identifiers(mention_identifiers)

    def _build_cluster(
        self,
        doc: dict[str, Any],
        events: list[GdeltEvent],
        mentions: list[GdeltMention],
        gkg_rows: list[GdeltGkg],
    ) -> dict[str, Any]:
        """Build a single materialised cluster row."""
        source_url = doc["source_url"]
        cluster_id = f"{datetime.now(UTC):%Y%m%d}_{sha256(source_url.encode()).hexdigest()[:12]}"

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
        country_values = [
            value
            for event in events
            for value in (
                event.action_geo_country_code,
                event.actor1_country_code,
                event.actor2_country_code,
            )
            if value
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
            "dominant_event_types": _top_values(event_type_labels),
            "dominant_quad_classes": _top_values(quad_labels),
            "avg_severity_score": round(mean(severities), 2) if severities else None,
            "dominant_countries": _top_values(country_values),
            "dominant_locations": _top_values(location_values),
            "mention_count": len(mentions),
            "distinct_mention_sources": mention_sources,
            "mention_identifiers": mention_identifiers,
            "first_mention_at": min(mention_times) if mention_times else None,
            "last_mention_at": max(mention_times) if mention_times else None,
            "themes": themes,
            "persons": persons,
            "organizations": organizations,
            "gkg_locations": gkg_locations,
            "document_tone_avg": round(mean(tones), 2) if tones else None,
            "computed_at": datetime.now(UTC),
        }

    async def build_and_materialise(self, since_sqldate: int) -> int:
        """Build and persist story clusters for candidate source URLs."""
        try:
            candidates = await self._score_source_urls(since_sqldate)
            cluster_rows: list[dict[str, Any]] = []

            for candidate in candidates:
                events = await self._collect_events(candidate["source_url"], since_sqldate)
                event_ids = [event.global_event_id for event in events]
                mentions = await self._collect_mentions(event_ids)
                mention_identifiers = [
                    mention_identifier
                    for mention_identifier in {
                        mention.mention_identifier
                        for mention in mentions
                        if mention.mention_identifier
                    }
                ]
                gkg_rows = await self._collect_gkg(mention_identifiers)
                cluster_rows.append(self._build_cluster(candidate, events, mentions, gkg_rows))

            if not cluster_rows:
                return 0

            inserted = await self._cluster_repo.bulk_upsert(cluster_rows)
            logger.info("clusters_materialised", count=inserted, since_sqldate=since_sqldate)
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


def _parse_gdelt_timestamp(value: int) -> datetime:
    """Convert a GDELT YYYYMMDDHHMMSS integer timestamp into UTC datetime."""
    return datetime.strptime(str(value), "%Y%m%d%H%M%S").replace(tzinfo=UTC)
