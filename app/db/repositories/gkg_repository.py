"""Repository for gdelt_gkg — insert and query GKG rows."""

from __future__ import annotations

from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.db.models import GdeltGkg
from app.db.repositories._upsert import make_insert_ignore

logger = get_logger(__name__)


class GkgRepository:
    """Data access layer for the gdelt_gkg table."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def bulk_upsert(self, rows: list[dict[str, Any]]) -> int:
        """Insert GKG rows, ignoring duplicates on document_identifier.

        Uses dialect-aware INSERT IGNORE / ON CONFLICT DO NOTHING.
        Returns the number of rows submitted.
        """
        if not rows:
            return 0
        stmt = make_insert_ignore(self._session, GdeltGkg, rows)
        result = await self._session.execute(stmt)
        inserted = (
            result.rowcount if result.rowcount is not None and result.rowcount >= 0 else len(rows)
        )
        logger.info("gkg_upserted", count=inserted)
        return inserted

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
