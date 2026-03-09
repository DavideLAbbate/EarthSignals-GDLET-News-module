"""Expand event enrichment payload storage

Revision ID: 005
Revises: 004
Create Date: 2026-03-09

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "gdelt_events",
        "sources",
        existing_type=sa.JSON(),
        existing_nullable=True,
        new_column_name="cited_sources",
    )
    op.add_column("gdelt_events", sa.Column("main_topics", sa.JSON(), nullable=True))
    op.add_column("gdelt_events", sa.Column("keywords", sa.JSON(), nullable=True))
    op.add_column("gdelt_events", sa.Column("entities", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("gdelt_events", "entities")
    op.drop_column("gdelt_events", "keywords")
    op.drop_column("gdelt_events", "main_topics")
    op.alter_column(
        "gdelt_events",
        "cited_sources",
        existing_type=sa.JSON(),
        existing_nullable=True,
        new_column_name="sources",
    )
