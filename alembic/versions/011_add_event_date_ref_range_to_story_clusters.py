"""Add event_date_ref_start and event_date_ref_end columns to story_clusters.

These columns track the calendar span of the underlying GDELT events
(sql_date min/max, YYYYMMDD integer) for each cluster, enabling the
time-proximity merge gate introduced in the cluster merge redesign.

Both columns are nullable so that pre-existing rows are not broken.

Revision ID: 011
Revises: 010
Create Date: 2026-03-19
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "011"
down_revision = "010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("story_clusters", sa.Column("event_date_ref_start", sa.Integer(), nullable=True))
    op.add_column("story_clusters", sa.Column("event_date_ref_end", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("story_clusters", "event_date_ref_end")
    op.drop_column("story_clusters", "event_date_ref_start")
