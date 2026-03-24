"""Add persistent cluster component tables.

Revision ID: 014
Revises: 013
Create Date: 2026-03-24
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "014"
down_revision = "013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cluster_components",
        sa.Column("component_id", sa.String(length=36), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("anchor_source_url", sa.Text(), nullable=False),
        sa.Column("component_source_urls", sa.JSON(), nullable=False),
        sa.Column("anchor_locked_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("seed_event_ids", sa.JSON(), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("missing_run_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("merged_into_component_id", sa.String(length=36), nullable=True),
        sa.Column("current_cluster_id", sa.String(length=100), nullable=True),
        sa.Column("current_table", sa.String(length=30), nullable=True),
        sa.Column("current_computed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("has_gkg", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("merge_evidence", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("component_id"),
    )
    op.create_index("ix_cluster_components_status", "cluster_components", ["status"])
    op.create_index(
        "ix_cluster_components_merged_into_component_id",
        "cluster_components",
        ["merged_into_component_id"],
    )

    op.create_table(
        "cluster_component_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("component_id", sa.String(length=36), nullable=False),
        sa.Column("event_id", sa.String(length=32), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_cluster_component_events_component_id",
        "cluster_component_events",
        ["component_id"],
    )
    op.create_index(
        "ix_cluster_component_events_event_id",
        "cluster_component_events",
        ["event_id"],
    )
    op.create_index(
        "uq_cluster_component_events_component_event",
        "cluster_component_events",
        ["component_id", "event_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index(
        "uq_cluster_component_events_component_event",
        table_name="cluster_component_events",
    )
    op.drop_index("ix_cluster_component_events_event_id", table_name="cluster_component_events")
    op.drop_index(
        "ix_cluster_component_events_component_id",
        table_name="cluster_component_events",
    )
    op.drop_table("cluster_component_events")

    op.drop_index(
        "ix_cluster_components_merged_into_component_id",
        table_name="cluster_components",
    )
    op.drop_index("ix_cluster_components_status", table_name="cluster_components")
    op.drop_table("cluster_components")
