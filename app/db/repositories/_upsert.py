"""Dialect-aware INSERT helper for PostgreSQL and SQLite test environments.

Provides a factory that builds an INSERT … ON CONFLICT DO NOTHING statement
using the correct dialect-specific API, matching the pattern used in
event_repository._get_insert_chunk_size().
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession


def make_insert_ignore(
    session: AsyncSession,
    model: Any,
    rows: list[dict[str, Any]],
) -> Any:
    """Build an INSERT-ignore statement for the current database dialect.

    - PostgreSQL: ``INSERT … ON CONFLICT DO NOTHING``
    - SQLite: ``INSERT OR IGNORE …``

    Both silently skip rows that would violate a unique constraint.

    Args:
        session: The active async session (used to detect the dialect).
        model: The SQLAlchemy ORM model class (table target).
        rows: List of row dicts to insert.

    Returns:
        A SQLAlchemy Insert statement ready for ``session.execute()``.
    """
    dialect_name = session.bind.dialect.name if session.bind is not None else "postgresql"
    if dialect_name == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        return sqlite_insert(model).values(rows).prefix_with("OR IGNORE")

    from sqlalchemy.dialects.postgresql import insert as pg_insert

    return pg_insert(model).values(rows).on_conflict_do_nothing()
