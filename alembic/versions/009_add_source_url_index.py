"""Add index on gdelt_events.source_url for cluster pipeline performance.

The cluster scoring query groups and filters by source_url across the full
gdelt_events table. Without an index, this resolves via a full table scan
which becomes prohibitively slow at production data volumes.

Revision ID: 009
Revises: 008
Create Date: 2026-03-10
"""

from __future__ import annotations

from alembic import op

revision = "009"
down_revision = "008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_gdelt_events_source_url", "gdelt_events", ["source_url"])


def downgrade() -> None:
    op.drop_index("ix_gdelt_events_source_url", table_name="gdelt_events")
