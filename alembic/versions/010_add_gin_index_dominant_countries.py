"""Add GIN index on story_clusters.dominant_countries for country-code search.

The country-code filter in ClusterRepository.search uses the PostgreSQL JSONB
containment operator (@>). Without an index, this resolves via a full table scan.
A GIN index on the jsonb_path_ops operator class allows index-only scans for the
@> operator and is significantly smaller than the default jsonb_ops index.

SQLite (used in tests) does not support GIN indexes; the migration is a no-op there.

Revision ID: 010
Revises: 009
Create Date: 2026-03-19
"""

from __future__ import annotations

from alembic import op

revision = "010"
down_revision = "009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        "ix_story_clusters_dominant_countries_gin "
        "ON story_clusters USING gin "
        "((dominant_countries::jsonb) jsonb_path_ops)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_story_clusters_dominant_countries_gin")
