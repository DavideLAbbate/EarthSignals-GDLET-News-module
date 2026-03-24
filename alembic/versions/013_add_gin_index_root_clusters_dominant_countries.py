"""Add GIN index on root_clusters.dominant_countries.

Revision ID: 013
Revises: 012
Create Date: 2026-03-20
"""

from __future__ import annotations

from alembic import op

revision = "013"
down_revision = "012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "ix_root_clusters_dominant_countries_gin "
        "ON root_clusters USING gin "
        "((dominant_countries::jsonb) jsonb_path_ops)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_root_clusters_dominant_countries_gin")
