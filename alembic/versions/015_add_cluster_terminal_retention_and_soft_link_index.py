"""Add cluster terminal cleanup support index.

Revision ID: 015
Revises: 014
Create Date: 2026-03-25
"""

from __future__ import annotations

from alembic import op

revision = "015"
down_revision = "014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_cluster_components_current_table_cluster_id",
        "cluster_components",
        ["current_table", "current_cluster_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_cluster_components_current_table_cluster_id",
        table_name="cluster_components",
    )
