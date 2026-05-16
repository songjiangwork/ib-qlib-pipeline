"""Consolidate manual webapi schema migrations

Revision ID: 0003_consolidate_webapi_schema
Revises: 0002_instrument_static_info
Create Date: 2026-05-16 09:30:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "0003_consolidate_webapi_schema"
down_revision = "0002_instrument_static_info"
branch_labels = None
depends_on = None


def has_table(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = inspect(bind)
    return table_name in set(inspector.get_table_names())


def has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = inspect(bind)
    return column_name in {column["name"] for column in inspector.get_columns(table_name)}


def add_column_if_missing(table_name: str, column: sa.Column) -> None:
    if not has_column(table_name, column.name):
        op.add_column(table_name, column)


def create_index_if_missing(index_name: str, table_name: str, columns: list[str]) -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    existing = {idx["name"] for idx in inspector.get_indexes(table_name)}
    if index_name not in existing:
        op.create_index(index_name, table_name, columns)


def upgrade() -> None:
    if not has_table("universes"):
        op.create_table(
            "universes",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("key", sa.Text(), nullable=False),
            sa.Column("name", sa.Text(), nullable=False),
            sa.Column("symbols_file", sa.Text(), nullable=False),
            sa.Column("symbol_count", sa.Integer(), nullable=False),
            sa.Column("description", sa.Text(), nullable=True),
            sa.Column("details_json", sa.Text(), nullable=True),
            sa.Column("created_at", sa.Text(), nullable=False),
            sa.Column("updated_at", sa.Text(), nullable=False),
            sa.PrimaryKeyConstraint("id", name=op.f("pk_universes")),
            sa.UniqueConstraint("key", name=op.f("uq_universes_key")),
        )

    if not has_table("universe_symbols"):
        op.create_table(
            "universe_symbols",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("universe_id", sa.Integer(), nullable=False),
            sa.Column("symbol", sa.Text(), nullable=False),
            sa.Column("ib_symbol", sa.Text(), nullable=False),
            sa.Column("sort_order", sa.Integer(), nullable=False),
            sa.Column("source_line", sa.Text(), nullable=False),
            sa.Column("created_at", sa.Text(), nullable=False),
            sa.Column("updated_at", sa.Text(), nullable=False),
            sa.ForeignKeyConstraint(
                ["universe_id"],
                ["universes.id"],
                name=op.f("fk_universe_symbols_universe_id_universes"),
                ondelete="CASCADE",
            ),
            sa.PrimaryKeyConstraint("id", name=op.f("pk_universe_symbols")),
            sa.UniqueConstraint("universe_id", "symbol", name=op.f("uq_universe_symbols_universe_id")),
        )
    create_index_if_missing("idx_universe_symbols_universe_order", "universe_symbols", ["universe_id", "sort_order"])
    create_index_if_missing("idx_universe_symbols_symbol", "universe_symbols", ["symbol"])

    if not has_table("strategies"):
        op.create_table(
            "strategies",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("universe_id", sa.Integer(), nullable=True),
            sa.Column("key", sa.Text(), nullable=False),
            sa.Column("name", sa.Text(), nullable=False),
            sa.Column("strategy_type", sa.Text(), nullable=False),
            sa.Column("description", sa.Text(), nullable=True),
            sa.Column("entry_rule", sa.Text(), nullable=True),
            sa.Column("exit_rule", sa.Text(), nullable=True),
            sa.Column("signal_timing", sa.Text(), nullable=True),
            sa.Column("execution_timing", sa.Text(), nullable=True),
            sa.Column("trade_price_basis", sa.Text(), nullable=True),
            sa.Column("buy_top_n", sa.Integer(), nullable=True),
            sa.Column("hold_top_n", sa.Integer(), nullable=True),
            sa.Column("hold_days", sa.Integer(), nullable=True),
            sa.Column("target_notional", sa.Float(), nullable=True),
            sa.Column("initial_capital", sa.Float(), nullable=True),
            sa.Column("max_open_positions", sa.Integer(), nullable=True),
            sa.Column("max_position_notional", sa.Float(), nullable=True),
            sa.Column("max_position_pct", sa.Float(), nullable=True),
            sa.Column("fee_bps", sa.Float(), nullable=True),
            sa.Column("slippage_bps", sa.Float(), nullable=True),
            sa.Column("gap_up_limit_pct", sa.Float(), nullable=True),
            sa.Column("gap_down_limit_pct", sa.Float(), nullable=True),
            sa.Column("allow_reentry", sa.Boolean(), nullable=True),
            sa.Column("details_json", sa.Text(), nullable=True),
            sa.Column("config_json", sa.Text(), nullable=True),
            sa.Column("created_at", sa.Text(), nullable=False),
            sa.Column("updated_at", sa.Text(), nullable=False),
            sa.ForeignKeyConstraint(
                ["universe_id"],
                ["universes.id"],
                name=op.f("fk_strategies_universe_id_universes"),
                ondelete="SET NULL",
            ),
            sa.PrimaryKeyConstraint("id", name=op.f("pk_strategies")),
            sa.UniqueConstraint("key", name=op.f("uq_strategies_key")),
        )

    add_column_if_missing("models", sa.Column("universe_id", sa.Integer(), nullable=True))
    create_index_if_missing("idx_models_universe_id", "models", ["universe_id", "key"])

    add_column_if_missing("strategies", sa.Column("universe_id", sa.Integer(), nullable=True))
    add_column_if_missing("strategies", sa.Column("description", sa.Text(), nullable=True))
    add_column_if_missing("strategies", sa.Column("entry_rule", sa.Text(), nullable=True))
    add_column_if_missing("strategies", sa.Column("exit_rule", sa.Text(), nullable=True))
    add_column_if_missing("strategies", sa.Column("signal_timing", sa.Text(), nullable=True))
    add_column_if_missing("strategies", sa.Column("execution_timing", sa.Text(), nullable=True))
    add_column_if_missing("strategies", sa.Column("trade_price_basis", sa.Text(), nullable=True))
    add_column_if_missing("strategies", sa.Column("buy_top_n", sa.Integer(), nullable=True))
    add_column_if_missing("strategies", sa.Column("hold_top_n", sa.Integer(), nullable=True))
    add_column_if_missing("strategies", sa.Column("hold_days", sa.Integer(), nullable=True))
    add_column_if_missing("strategies", sa.Column("target_notional", sa.Float(), nullable=True))
    add_column_if_missing("strategies", sa.Column("initial_capital", sa.Float(), nullable=True))
    add_column_if_missing("strategies", sa.Column("max_open_positions", sa.Integer(), nullable=True))
    add_column_if_missing("strategies", sa.Column("max_position_notional", sa.Float(), nullable=True))
    add_column_if_missing("strategies", sa.Column("max_position_pct", sa.Float(), nullable=True))
    add_column_if_missing("strategies", sa.Column("fee_bps", sa.Float(), nullable=True))
    add_column_if_missing("strategies", sa.Column("slippage_bps", sa.Float(), nullable=True))
    add_column_if_missing("strategies", sa.Column("gap_up_limit_pct", sa.Float(), nullable=True))
    add_column_if_missing("strategies", sa.Column("gap_down_limit_pct", sa.Float(), nullable=True))
    add_column_if_missing("strategies", sa.Column("allow_reentry", sa.Boolean(), nullable=True))
    add_column_if_missing("strategies", sa.Column("details_json", sa.Text(), nullable=True))
    add_column_if_missing("strategies", sa.Column("config_json", sa.Text(), nullable=True))
    create_index_if_missing("idx_strategies_universe_id", "strategies", ["universe_id", "key"])

    add_column_if_missing("runs", sa.Column("model_id", sa.Integer(), nullable=True))
    add_column_if_missing("runs", sa.Column("universe_id", sa.Integer(), nullable=True))
    create_index_if_missing("idx_runs_model_id", "runs", ["model_id", "signal_date"])
    create_index_if_missing("idx_runs_universe_id", "runs", ["universe_id", "signal_date"])

    add_column_if_missing("portfolio_runs", sa.Column("universe_id", sa.Integer(), nullable=True))
    add_column_if_missing("portfolio_runs", sa.Column("strategy_id", sa.Integer(), nullable=True))
    add_column_if_missing("portfolio_runs", sa.Column("strategy_config_json", sa.Text(), nullable=True))
    create_index_if_missing("idx_portfolio_runs_universe_id", "portfolio_runs", ["universe_id", "end_signal_date"])

    add_column_if_missing(
        "schedules",
        sa.Column("schedule_type", sa.Text(), nullable=False, server_default=sa.text("'ranking'")),
    )
    add_column_if_missing("schedules", sa.Column("pipeline_start_date", sa.Text(), nullable=True))
    add_column_if_missing(
        "schedules",
        sa.Column("pipeline_include_portfolio", sa.Boolean(), nullable=False, server_default=sa.text("1")),
    )

    add_column_if_missing("jobs", sa.Column("worker_id", sa.Text(), nullable=True))
    add_column_if_missing("jobs", sa.Column("pid", sa.Integer(), nullable=True))
    add_column_if_missing("jobs", sa.Column("heartbeat_at", sa.Text(), nullable=True))
    add_column_if_missing("jobs", sa.Column("locked_at", sa.Text(), nullable=True))
    add_column_if_missing("jobs", sa.Column("cancel_requested_at", sa.Text(), nullable=True))
    add_column_if_missing("jobs", sa.Column("cancel_reason", sa.Text(), nullable=True))

    if not has_table("job_log_lines"):
        op.create_table(
            "job_log_lines",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("job_id", sa.Integer(), nullable=False),
            sa.Column("step_id", sa.Integer(), nullable=True),
            sa.Column("line_no", sa.Integer(), nullable=False),
            sa.Column("stream", sa.Text(), nullable=True),
            sa.Column("content", sa.Text(), nullable=False),
            sa.Column("created_at", sa.Text(), nullable=False),
            sa.ForeignKeyConstraint(
                ["job_id"],
                ["jobs.id"],
                name=op.f("fk_job_log_lines_job_id_jobs"),
                ondelete="CASCADE",
            ),
            sa.ForeignKeyConstraint(
                ["step_id"],
                ["job_steps.id"],
                name=op.f("fk_job_log_lines_step_id_job_steps"),
                ondelete="CASCADE",
            ),
            sa.PrimaryKeyConstraint("id", name=op.f("pk_job_log_lines")),
        )
    create_index_if_missing("idx_job_log_lines_job_id", "job_log_lines", ["job_id", "id"])
    create_index_if_missing("idx_job_log_lines_step_id", "job_log_lines", ["step_id", "id"])


def downgrade() -> None:
    op.drop_index("idx_job_log_lines_step_id", table_name="job_log_lines")
    op.drop_index("idx_job_log_lines_job_id", table_name="job_log_lines")
    op.drop_table("job_log_lines")
    op.drop_index("idx_portfolio_runs_universe_id", table_name="portfolio_runs")
    op.drop_index("idx_runs_universe_id", table_name="runs")
    op.drop_index("idx_strategies_universe_id", table_name="strategies")
    op.drop_index("idx_models_universe_id", table_name="models")
    op.drop_index("idx_universe_symbols_symbol", table_name="universe_symbols")
    op.drop_index("idx_universe_symbols_universe_order", table_name="universe_symbols")
    op.drop_table("universe_symbols")
    op.drop_table("strategies")
    op.drop_table("universes")
