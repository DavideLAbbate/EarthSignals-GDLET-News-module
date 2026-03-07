"""Initial schema: SyncState and FilterMappingCache

Revision ID: 001
Revises:
Create Date: 2026-03-07

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── sync_state ────────────────────────────────────────────────────────
    op.create_table(
        "sync_state",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("latest_sqldate", sa.Integer(), nullable=True),
        sa.Column("latest_dateadded", sa.BigInteger(), nullable=True),
        sa.Column("top_countries", sa.JSON(), nullable=True),
        sa.Column("top_event_root_codes", sa.JSON(), nullable=True),
        sa.Column("mapping_version", sa.String(length=50), nullable=False),
        sa.Column("synced_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sync_status", sa.String(length=20), nullable=False, server_default="success"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_sync_state_synced_at"), "sync_state", ["synced_at"], unique=False)

    # ── filter_mapping_cache ──────────────────────────────────────────────
    op.create_table(
        "filter_mapping_cache",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("cache_key", sa.String(length=64), nullable=False),
        sa.Column("raw_input", sa.JSON(), nullable=False),
        sa.Column("normalized_filters", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("cache_key"),
    )
    op.create_index(
        op.f("ix_filter_mapping_cache_cache_key"),
        "filter_mapping_cache",
        ["cache_key"],
        unique=True,
    )
    op.create_index(
        op.f("ix_filter_mapping_cache_expires_at"),
        "filter_mapping_cache",
        ["expires_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_filter_mapping_cache_expires_at"), table_name="filter_mapping_cache")
    op.drop_index(op.f("ix_filter_mapping_cache_cache_key"), table_name="filter_mapping_cache")
    op.drop_table("filter_mapping_cache")
    op.drop_index(op.f("ix_sync_state_synced_at"), table_name="sync_state")
    op.drop_table("sync_state")
