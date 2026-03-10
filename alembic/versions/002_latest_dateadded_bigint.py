"""Fix latest_dateadded column type: Integer → BigInteger

GDELT's DATEADDED field is YYYYMMDDHHMMSS (14 digits, up to ~20260307010000)
which exceeds PostgreSQL INTEGER range (~2.1 billion). Must be BIGINT.

Revision ID: 002
Revises: 001
Create Date: 2026-03-07

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("sync_state") as batch_op:
        batch_op.alter_column(
            "latest_dateadded",
            existing_type=sa.Integer(),
            type_=sa.BigInteger(),
            existing_nullable=True,
        )


def downgrade() -> None:
    with op.batch_alter_table("sync_state") as batch_op:
        batch_op.alter_column(
            "latest_dateadded",
            existing_type=sa.BigInteger(),
            type_=sa.Integer(),
            existing_nullable=True,
        )
