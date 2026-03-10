"""Regression tests for Alembic migration compatibility."""

from __future__ import annotations

from pathlib import Path


def test_sqlite_compatible_migrations_avoid_raw_op_alter_column() -> None:
    """SQLite-targeted verification should not rely on raw op.alter_column in migrations."""
    migration_paths = [
        Path("alembic/versions/002_latest_dateadded_bigint.py"),
        Path("alembic/versions/004_add_event_enrichment_fields.py"),
        Path("alembic/versions/005_expand_event_enrichment_payload.py"),
    ]

    for migration_path in migration_paths:
        content = migration_path.read_text(encoding="utf-8")
        assert "batch_alter_table" in content
        assert "\nop.alter_column(" not in content
