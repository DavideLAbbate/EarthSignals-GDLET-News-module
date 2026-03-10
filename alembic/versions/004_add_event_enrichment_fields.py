"""Add Phase 1 event enrichment fields to gdelt_events

Revision ID: 004
Revises: 003
Create Date: 2026-03-09

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("gdelt_events", sa.Column("article_title", sa.Text(), nullable=True))
    op.add_column("gdelt_events", sa.Column("article_summary", sa.Text(), nullable=True))
    op.add_column("gdelt_events", sa.Column("sources", sa.JSON(), nullable=True))
    op.add_column(
        "gdelt_events",
        sa.Column(
            "enrichment_status", sa.String(length=20), nullable=False, server_default="pending"
        ),
    )
    op.add_column(
        "gdelt_events", sa.Column("enriched_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column("gdelt_events", sa.Column("enrichment_error", sa.Text(), nullable=True))
    with op.batch_alter_table("gdelt_events") as batch_op:
        batch_op.alter_column("enrichment_status", server_default=None)


def downgrade() -> None:
    op.drop_column("gdelt_events", "enrichment_error")
    op.drop_column("gdelt_events", "enriched_at")
    op.drop_column("gdelt_events", "enrichment_status")
    op.drop_column("gdelt_events", "sources")
    op.drop_column("gdelt_events", "article_summary")
    op.drop_column("gdelt_events", "article_title")
