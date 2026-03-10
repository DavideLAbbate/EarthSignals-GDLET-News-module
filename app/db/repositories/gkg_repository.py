"""Repository for gdelt_gkg — insert and query GKG rows."""

from __future__ import annotations

import math
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.db.models import GdeltGkg
from app.db.repositories._upsert import make_insert_ignore

logger = get_logger(__name__)

# asyncpg hard limit: 32767 bound parameters per statement.
_MAX_PG_ARGS = 32_767
_MAX_SQLITE_ARGS = 999
_GKG_COLUMNS = len(GdeltGkg.__table__.columns)


def _chunk_size(session: AsyncSession) -> int:
    """Return the max rows per INSERT for the active dialect."""
    dialect = session.bind.dialect.name if session.bind is not None else "postgresql"
    max_args = _MAX_SQLITE_ARGS if dialect == "sqlite" else _MAX_PG_ARGS
    return max(1, math.floor(max_args / _GKG_COLUMNS))


class GkgRepository:
    """Data access layer for the gdelt_gkg table."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def bulk_upsert(self, rows: list[dict[str, Any]]) -> int:
        """Insert GKG rows, ignoring duplicates on document_identifier.

        Chunks rows to stay below the asyncpg 32 767-parameter limit (10 columns
        per row → max ~3 276 rows per statement). Uses dialect-aware INSERT IGNORE
        / ON CONFLICT DO NOTHING.

        Returns the number of rows submitted.
        """
        if not rows:
            return 0
        size = _chunk_size(self._session)
        inserted_total = 0
        for start in range(0, len(rows), size):
            chunk = rows[start : start + size]
            stmt = make_insert_ignore(self._session, GdeltGkg, chunk)
            result = await self._session.execute(stmt)
            inserted_total += (
                result.rowcount
                if result.rowcount is not None and result.rowcount >= 0
                else len(chunk)
            )
        logger.info("gkg_upserted", count=inserted_total)
        return inserted_total

    async def get_by_document_identifiers(self, identifiers: list[str]) -> list[GdeltGkg]:
        """Return GKG rows matching any of the given document_identifier URLs."""
        if not identifiers:
            return []
        result = await self._session.execute(
            select(GdeltGkg).where(GdeltGkg.document_identifier.in_(identifiers))
        )
        return list(result.scalars().all())

    async def delete_before_date(self, cutoff_date: int) -> int:
        """Delete GKG rows whose date is older than cutoff_date (YYYYMMDDHHMMSS).

        Returns the number of rows deleted.
        """
        result = await self._session.execute(delete(GdeltGkg).where(GdeltGkg.date < cutoff_date))
        return result.rowcount or 0
