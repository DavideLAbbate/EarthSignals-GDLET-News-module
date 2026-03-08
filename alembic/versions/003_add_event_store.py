"""Add event store tables: gdelt_events and ingestion_state

Creates the local GDELT event cache table with composite indexes
for efficient geo and event root code queries. Also creates the
ingestion state tracking table for monitoring bootstrap/incremental
ingestion jobs.

Revision ID: 003
Revises: 002
Create Date: 2026-03-08

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "gdelt_events",
        sa.Column("global_event_id", sa.BigInteger(), primary_key=True, autoincrement=False),
        sa.Column("sql_date", sa.Integer(), nullable=False),
        sa.Column("date_added", sa.BigInteger(), nullable=False),
        sa.Column("actor1_country_code", sa.String(3), nullable=True),
        sa.Column("actor2_country_code", sa.String(3), nullable=True),
        sa.Column("event_code", sa.String(4), nullable=True),
        sa.Column("event_base_code", sa.String(3), nullable=True),
        sa.Column("event_root_code", sa.String(2), nullable=True),
        sa.Column("quad_class", sa.Integer(), nullable=True),
        sa.Column("goldstein_scale", sa.Float(), nullable=True),
        sa.Column("avg_tone", sa.Float(), nullable=True),
        sa.Column("num_mentions", sa.Integer(), nullable=True),
        sa.Column("num_sources", sa.Integer(), nullable=True),
        sa.Column("num_articles", sa.Integer(), nullable=True),
        sa.Column("action_geo_full_name", sa.String(500), nullable=True),
        sa.Column("action_geo_country_code", sa.String(2), nullable=True),
        sa.Column("source_url", sa.Text(), nullable=True),
    )
    op.create_index("ix_gdelt_events_sql_date", "gdelt_events", ["sql_date"])
    op.create_index("ix_gdelt_events_date_added", "gdelt_events", ["date_added"])
    op.create_index("ix_gdelt_events_event_root_code", "gdelt_events", ["event_root_code"])
    op.create_index(
        "ix_gdelt_events_action_geo_country_code", "gdelt_events", ["action_geo_country_code"]
    )
    op.create_index(
        "ix_gdelt_events_geo_date",
        "gdelt_events",
        ["action_geo_country_code", "sql_date"],
    )
    op.create_index(
        "ix_gdelt_events_event_date",
        "gdelt_events",
        ["event_root_code", "sql_date"],
    )

    op.create_table(
        "ingestion_state",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("ingestion_type", sa.String(20), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="running"),
        sa.Column("watermark_dateadded", sa.BigInteger(), nullable=True),
        sa.Column("events_ingested", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("ingestion_state")
    op.drop_index("ix_gdelt_events_event_date", table_name="gdelt_events")
    op.drop_index("ix_gdelt_events_geo_date", table_name="gdelt_events")
    op.drop_index("ix_gdelt_events_action_geo_country_code", table_name="gdelt_events")
    op.drop_index("ix_gdelt_events_event_root_code", table_name="gdelt_events")
    op.drop_index("ix_gdelt_events_date_added", table_name="gdelt_events")
    op.drop_index("ix_gdelt_events_sql_date", table_name="gdelt_events")
    op.drop_table("gdelt_events")
