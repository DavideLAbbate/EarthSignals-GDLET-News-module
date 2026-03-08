"""PostgreSQL query compiler for local event store."""

from __future__ import annotations

from typing import Any

from sqlalchemy import Select, and_, func, select

from app.db.models import GdeltEvent
from app.schemas.filters import NormalizedFilters


class PostgresQueryCompiler:
    """
    Compiles NormalizedFilters to PostgreSQL queries.

    This compiler transforms backend-agnostic filters into SQLAlchemy
    queries against the local gdelt_events table.
    """

    def compile(
        self,
        filters: NormalizedFilters,
        limit: int = 100,
        offset: int = 0,
    ) -> Select:
        """Compile filters to a SELECT query."""
        conditions = self._build_conditions(filters)

        stmt = (
            select(GdeltEvent)
            .where(and_(*conditions) if conditions else True)
            .order_by(GdeltEvent.date_added.desc())
            .limit(limit)
            .offset(offset)
        )

        return stmt

    def compile_count(self, filters: NormalizedFilters) -> Select:
        """Compile filters to a COUNT query."""
        conditions = self._build_conditions(filters)

        stmt = select(func.count(GdeltEvent.global_event_id)).where(
            and_(*conditions) if conditions else True
        )

        return stmt

    def _build_conditions(
        self,
        filters: NormalizedFilters,
    ) -> list[Any]:
        """Build list of WHERE conditions from filters."""
        conditions: list[Any] = []

        # Date range filter (using sqldate integer format)
        if filters.date_from_sqldate:
            conditions.append(GdeltEvent.sql_date >= filters.date_from_sqldate)
        if filters.date_to_sqldate:
            conditions.append(GdeltEvent.sql_date <= filters.date_to_sqldate)

        # Actor1 country filter (handles both string and list)
        if filters.actor1_country_code:
            if isinstance(filters.actor1_country_code, str):
                conditions.append(GdeltEvent.actor1_country_code == filters.actor1_country_code)
            else:
                conditions.append(GdeltEvent.actor1_country_code.in_(filters.actor1_country_code))

        # Actor2 country filter (handles both string and list)
        if filters.actor2_country_code:
            if isinstance(filters.actor2_country_code, str):
                conditions.append(GdeltEvent.actor2_country_code == filters.actor2_country_code)
            else:
                conditions.append(GdeltEvent.actor2_country_code.in_(filters.actor2_country_code))

        # Action geo country filter
        if filters.geo_country_codes:
            conditions.append(GdeltEvent.action_geo_country_code.in_(filters.geo_country_codes))

        # Event codes (specific)
        if filters.event_codes:
            conditions.append(GdeltEvent.event_code.in_(filters.event_codes))

        # Event root codes
        if filters.event_root_codes:
            conditions.append(GdeltEvent.event_root_code.in_(filters.event_root_codes))

        # QuadClass (conflict type)
        if filters.quad_classes:
            conditions.append(GdeltEvent.quad_class.in_(filters.quad_classes))

        # Goldstein scale range
        if filters.goldstein_min is not None:
            conditions.append(GdeltEvent.goldstein_scale >= filters.goldstein_min)
        if filters.goldstein_max is not None:
            conditions.append(GdeltEvent.goldstein_scale <= filters.goldstein_max)

        # AvgTone (sentiment) range
        if filters.tone_min is not None:
            conditions.append(GdeltEvent.avg_tone >= filters.tone_min)
        if filters.tone_max is not None:
            conditions.append(GdeltEvent.avg_tone <= filters.tone_max)

        # Mention/source/article thresholds
        if filters.min_mentions is not None:
            conditions.append(GdeltEvent.num_mentions >= filters.min_mentions)
        if filters.min_sources is not None:
            conditions.append(GdeltEvent.num_sources >= filters.min_sources)
        if filters.min_articles is not None:
            conditions.append(GdeltEvent.num_articles >= filters.min_articles)

        return conditions
