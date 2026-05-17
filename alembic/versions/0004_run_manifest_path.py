"""Add manifest_path to runs

Revision ID: 0004_run_manifest_path
Revises: 0003_consolidate_webapi_schema
Create Date: 2026-05-17 09:15:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "0004_run_manifest_path"
down_revision = "0003_consolidate_webapi_schema"
branch_labels = None
depends_on = None


def has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = inspect(bind)
    return column_name in {column["name"] for column in inspector.get_columns(table_name)}


def upgrade() -> None:
    if not has_column("runs", "manifest_path"):
        op.add_column("runs", sa.Column("manifest_path", sa.Text(), nullable=True))


def downgrade() -> None:
    if has_column("runs", "manifest_path"):
        op.drop_column("runs", "manifest_path")
