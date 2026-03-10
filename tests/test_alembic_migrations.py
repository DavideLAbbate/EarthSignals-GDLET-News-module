"""Regression tests for Alembic migration compatibility."""

from __future__ import annotations

from pathlib import Path


def test_migration_009_creates_source_url_index_on_gdelt_events() -> None:
    """Migration 009 must add an index on gdelt_events.source_url for cluster query performance."""
    content = Path("alembic/versions/009_add_source_url_index.py").read_text(encoding="utf-8")
    assert "ix_gdelt_events_source_url" in content
    assert "source_url" in content
    assert "gdelt_events" in content
    assert "create_index" in content


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
