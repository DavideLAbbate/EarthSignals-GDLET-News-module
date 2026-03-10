"""Add gdelt_mentions table.

Revision ID: 006
Revises: 005
Create Date: 2026-03-10
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "gdelt_mentions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("global_event_id", sa.BigInteger(), nullable=False),
        sa.Column("event_time_date", sa.BigInteger(), nullable=True),
        sa.Column("mention_time_date", sa.BigInteger(), nullable=True),
        sa.Column("mention_type", sa.Integer(), nullable=True),
        sa.Column("mention_source_name", sa.String(length=200), nullable=True),
        sa.Column("mention_identifier", sa.Text(), nullable=True),
        sa.Column("sent_count", sa.Integer(), nullable=True),
        sa.Column("mention_doc_len", sa.Integer(), nullable=True),
        sa.Column("mention_doc_tone", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_gdelt_mentions_global_event_id", "gdelt_mentions", ["global_event_id"])
    op.create_index(
        "ix_gdelt_mentions_mention_identifier", "gdelt_mentions", ["mention_identifier"]
    )
    op.create_index(
        "ix_gdelt_mentions_event_mention",
        "gdelt_mentions",
        ["global_event_id", "mention_identifier"],
    )


def downgrade() -> None:
    op.drop_index("ix_gdelt_mentions_event_mention", table_name="gdelt_mentions")
    op.drop_index("ix_gdelt_mentions_mention_identifier", table_name="gdelt_mentions")
    op.drop_index("ix_gdelt_mentions_global_event_id", table_name="gdelt_mentions")
    op.drop_table("gdelt_mentions")
