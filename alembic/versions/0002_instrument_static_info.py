"""Add instrument static info tables

Revision ID: 0002_instrument_static_info
Revises: 0001_initial_schema
Create Date: 2026-05-14 02:30:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0002_instrument_static_info"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "instruments",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("canonical_symbol", sa.Text(), nullable=False),
        sa.Column("display_symbol", sa.Text(), nullable=False),
        sa.Column("asset_type", sa.Text(), nullable=False),
        sa.Column("country", sa.Text(), nullable=True),
        sa.Column("exchange", sa.Text(), nullable=True),
        sa.Column("currency", sa.Text(), nullable=True),
        sa.Column("name_en", sa.Text(), nullable=True),
        sa.Column("name_zh", sa.Text(), nullable=True),
        sa.Column("sector", sa.Text(), nullable=True),
        sa.Column("industry", sa.Text(), nullable=True),
        sa.Column("sub_industry", sa.Text(), nullable=True),
        sa.Column("business_summary", sa.Text(), nullable=True),
        sa.Column("website", sa.Text(), nullable=True),
        sa.Column("ipo_date", sa.Text(), nullable=True),
        sa.Column("delist_date", sa.Text(), nullable=True),
        sa.Column("listing_status", sa.Text(), nullable=True),
        sa.Column("source", sa.Text(), nullable=True),
        sa.Column("source_updated_at", sa.Text(), nullable=True),
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_instruments")),
        sa.UniqueConstraint("canonical_symbol", name=op.f("uq_instruments_canonical_symbol")),
    )
    op.create_index("idx_instruments_asset_type", "instruments", ["asset_type"], unique=False)
    op.create_index("idx_instruments_country", "instruments", ["country"], unique=False)
    op.create_index("idx_instruments_exchange", "instruments", ["exchange"], unique=False)
    op.create_index("idx_instruments_industry", "instruments", ["industry"], unique=False)

    op.create_table(
        "instrument_aliases",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("alias_type", sa.Text(), nullable=False),
        sa.Column("alias_value", sa.Text(), nullable=False),
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name=op.f("fk_instrument_aliases_instrument_id_instruments"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_instrument_aliases")),
        sa.UniqueConstraint("alias_type", "alias_value", name=op.f("uq_instrument_aliases_alias_type")),
    )
    op.create_index("idx_instrument_aliases_instrument_id", "instrument_aliases", ["instrument_id"], unique=False)
    op.create_index("idx_instrument_aliases_alias_value", "instrument_aliases", ["alias_value"], unique=False)


def downgrade() -> None:
    op.drop_index("idx_instrument_aliases_alias_value", table_name="instrument_aliases")
    op.drop_index("idx_instrument_aliases_instrument_id", table_name="instrument_aliases")
    op.drop_table("instrument_aliases")
    op.drop_index("idx_instruments_industry", table_name="instruments")
    op.drop_index("idx_instruments_exchange", table_name="instruments")
    op.drop_index("idx_instruments_country", table_name="instruments")
    op.drop_index("idx_instruments_asset_type", table_name="instruments")
    op.drop_table("instruments")
