"""Add LLM enrichment fields to story_clusters and root_clusters.

Revision ID: 016
Revises: 015
Create Date: 2026-03-25
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "016"
down_revision = "015"
branch_labels = None
depends_on = None

_ENRICHMENT_COLUMNS = [
    sa.Column("article_title", sa.Text, nullable=True),
    sa.Column("article_summary", sa.Text, nullable=True),
    sa.Column("cited_sources", sa.JSON, nullable=True),
    sa.Column("main_topics", sa.JSON, nullable=True),
    sa.Column("keywords", sa.JSON, nullable=True),
    sa.Column("entities", sa.JSON, nullable=True),
    sa.Column(
        "enrichment_status",
        sa.String(20),
        nullable=False,
        server_default="pending",
    ),
    sa.Column("enriched_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("enrichment_error", sa.Text, nullable=True),
]


def upgrade() -> None:
    for col in _ENRICHMENT_COLUMNS:
        op.add_column("story_clusters", col)
        op.add_column("root_clusters", col)

    op.create_index(
        "ix_story_clusters_enrichment_status",
        "story_clusters",
        ["enrichment_status"],
    )
    op.create_index(
        "ix_root_clusters_enrichment_status",
        "root_clusters",
        ["enrichment_status"],
    )


def downgrade() -> None:
    op.drop_index("ix_root_clusters_enrichment_status", "root_clusters")
    op.drop_index("ix_story_clusters_enrichment_status", "story_clusters")

    for col in _ENRICHMENT_COLUMNS:
        op.drop_column("root_clusters", col.name)
        op.drop_column("story_clusters", col.name)
