"""Add gdelt_gkg table.

Revision ID: 007
Revises: 006
Create Date: 2026-03-10
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "gdelt_gkg",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("gkg_record_id", sa.String(length=50), nullable=True),
        sa.Column("date", sa.BigInteger(), nullable=True),
        sa.Column("source_common_name", sa.String(length=200), nullable=True),
        sa.Column("document_identifier", sa.Text(), nullable=True),
        sa.Column("themes", sa.JSON(), nullable=True),
        sa.Column("persons", sa.JSON(), nullable=True),
        sa.Column("organizations", sa.JSON(), nullable=True),
        sa.Column("locations", sa.JSON(), nullable=True),
        sa.Column("document_tone", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_gdelt_gkg_gkg_record_id", "gdelt_gkg", ["gkg_record_id"])
    op.create_index("ix_gdelt_gkg_document_identifier", "gdelt_gkg", ["document_identifier"])


def downgrade() -> None:
    op.drop_index("ix_gdelt_gkg_document_identifier", table_name="gdelt_gkg")
    op.drop_index("ix_gdelt_gkg_gkg_record_id", table_name="gdelt_gkg")
    op.drop_table("gdelt_gkg")
