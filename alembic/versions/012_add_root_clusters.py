"""Add root_clusters table.

Revision ID: 012
Revises: 011
Create Date: 2026-03-20
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "012"
down_revision = "011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "root_clusters",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("cluster_id", sa.String(length=100), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("event_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("num_articles", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("num_mentions", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("num_sources", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("topic_score", sa.Float(), nullable=True),
        sa.Column("event_ids", sa.JSON(), nullable=True),
        sa.Column("dominant_event_types", sa.JSON(), nullable=True),
        sa.Column("dominant_quad_classes", sa.JSON(), nullable=True),
        sa.Column("avg_severity_score", sa.Float(), nullable=True),
        sa.Column("dominant_countries", sa.JSON(), nullable=True),
        sa.Column("dominant_locations", sa.JSON(), nullable=True),
        sa.Column("mention_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("distinct_mention_sources", sa.JSON(), nullable=True),
        sa.Column("mention_identifiers", sa.JSON(), nullable=True),
        sa.Column("first_mention_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_mention_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("themes", sa.JSON(), nullable=True),
        sa.Column("persons", sa.JSON(), nullable=True),
        sa.Column("organizations", sa.JSON(), nullable=True),
        sa.Column("gkg_locations", sa.JSON(), nullable=True),
        sa.Column("document_tone_avg", sa.Float(), nullable=True),
        sa.Column("event_date_ref_start", sa.Integer(), nullable=True),
        sa.Column("event_date_ref_end", sa.Integer(), nullable=True),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("cluster_id", name="uq_root_clusters_cluster_id"),
    )
    op.create_index("ix_root_clusters_cluster_id", "root_clusters", ["cluster_id"], unique=True)
    op.create_index("ix_root_clusters_topic_score", "root_clusters", ["topic_score"])


def downgrade() -> None:
    op.drop_index("ix_root_clusters_topic_score", table_name="root_clusters")
    op.drop_index("ix_root_clusters_cluster_id", table_name="root_clusters")
    op.drop_table("root_clusters")
