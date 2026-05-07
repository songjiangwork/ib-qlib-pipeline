"""Initial schema baseline

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-05-07 16:50:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "models",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("model_class", sa.Text(), nullable=False),
        sa.Column("module_path", sa.Text(), nullable=False),
        sa.Column("workflow_base", sa.Text(), nullable=True),
        sa.Column("details_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_models")),
        sa.UniqueConstraint("key", name=op.f("uq_models_key")),
    )

    op.create_table(
        "schedules",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("schedule_type", sa.Text(), server_default="ranking", nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default="1", nullable=False),
        sa.Column("timezone", sa.Text(), nullable=False),
        sa.Column("day_of_week", sa.Text(), server_default="mon-fri", nullable=False),
        sa.Column("hour", sa.Integer(), nullable=False),
        sa.Column("minute", sa.Integer(), nullable=False),
        sa.Column("client_id", sa.Integer(), server_default="151", nullable=False),
        sa.Column("lookback_days", sa.Integer(), server_default="7", nullable=False),
        sa.Column("workflow_base", sa.Text(), nullable=False),
        sa.Column("pipeline_start_date", sa.Text(), nullable=True),
        sa.Column("pipeline_include_portfolio", sa.Boolean(), server_default="1", nullable=False),
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.Text(), nullable=False),
        sa.Column("last_triggered_at", sa.Text(), nullable=True),
        sa.Column("last_run_id", sa.Integer(), nullable=True),
        sa.Column("last_run_status", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_schedules")),
    )

    op.create_table(
        "runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("schedule_id", sa.Integer(), nullable=True),
        sa.Column("model_id", sa.Integer(), nullable=True),
        sa.Column("trigger_source", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("client_id", sa.Integer(), nullable=False),
        sa.Column("lookback_days", sa.Integer(), nullable=False),
        sa.Column("workflow_base", sa.Text(), nullable=False),
        sa.Column("command", sa.Text(), nullable=False),
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.Column("started_at", sa.Text(), nullable=True),
        sa.Column("finished_at", sa.Text(), nullable=True),
        sa.Column("signal_date", sa.Text(), nullable=True),
        sa.Column("ranking_csv_path", sa.Text(), nullable=True),
        sa.Column("html_report_path", sa.Text(), nullable=True),
        sa.Column("experiment_id", sa.Text(), nullable=True),
        sa.Column("recorder_id", sa.Text(), nullable=True),
        sa.Column("row_count", sa.Integer(), nullable=True),
        sa.Column("log_output", sa.Text(), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["model_id"], ["models.id"], name=op.f("fk_runs_model_id_models"), ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["schedule_id"], ["schedules.id"], name=op.f("fk_runs_schedule_id_schedules"), ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_runs")),
    )
    op.create_index("idx_runs_created_at", "runs", ["created_at"], unique=False)
    op.create_index("idx_runs_signal_date", "runs", ["signal_date"], unique=False)
    op.create_index("idx_runs_model_id", "runs", ["model_id", "signal_date"], unique=False)

    op.create_table(
        "recommendations",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("percentile", sa.Float(), nullable=True),
        sa.Column("entry_price", sa.Float(), nullable=True),
        sa.Column("signal_date", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["runs.id"], name=op.f("fk_recommendations_run_id_runs"), ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_recommendations")),
        sa.UniqueConstraint("run_id", "rank", name=op.f("uq_recommendations_run_id")),
    )
    op.create_index("idx_recommendations_run_id", "recommendations", ["run_id", "rank"], unique=False)
    op.create_index("idx_recommendations_symbol_date", "recommendations", ["symbol", "signal_date"], unique=False)

    op.create_table(
        "portfolio_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("strategy", sa.Text(), nullable=False),
        sa.Column("buy_top_n", sa.Integer(), nullable=False),
        sa.Column("hold_top_n", sa.Integer(), nullable=False),
        sa.Column("target_notional", sa.Float(), nullable=False),
        sa.Column("start_signal_date", sa.Text(), nullable=False),
        sa.Column("end_signal_date", sa.Text(), nullable=True),
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_portfolio_runs")),
    )
    op.create_index("idx_portfolio_runs_created_at", "portfolio_runs", ["created_at"], unique=False)

    op.create_table(
        "portfolio_lots",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("portfolio_run_id", sa.Integer(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("entry_run_id", sa.Integer(), nullable=False),
        sa.Column("entry_signal_date", sa.Text(), nullable=False),
        sa.Column("entry_trade_date", sa.Text(), nullable=False),
        sa.Column("entry_rank", sa.Integer(), nullable=False),
        sa.Column("entry_price_open", sa.Float(), nullable=False),
        sa.Column("shares", sa.Integer(), nullable=False),
        sa.Column("target_notional", sa.Float(), nullable=False),
        sa.Column("exit_run_id", sa.Integer(), nullable=True),
        sa.Column("exit_signal_date", sa.Text(), nullable=True),
        sa.Column("exit_trade_date", sa.Text(), nullable=True),
        sa.Column("exit_rank", sa.Integer(), nullable=True),
        sa.Column("exit_price_open", sa.Float(), nullable=True),
        sa.Column("realized_pnl", sa.Float(), nullable=True),
        sa.Column("realized_return_pct", sa.Float(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["entry_run_id"], ["runs.id"], name=op.f("fk_portfolio_lots_entry_run_id_runs"), ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["exit_run_id"], ["runs.id"], name=op.f("fk_portfolio_lots_exit_run_id_runs"), ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["portfolio_run_id"], ["portfolio_runs.id"], name=op.f("fk_portfolio_lots_portfolio_run_id_portfolio_runs"), ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_portfolio_lots")),
    )
    op.create_index("idx_portfolio_lots_run_symbol", "portfolio_lots", ["portfolio_run_id", "symbol", "status"], unique=False)
    op.create_index("idx_portfolio_lots_entry_date", "portfolio_lots", ["entry_trade_date"], unique=False)

    op.create_table(
        "portfolio_marks",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("portfolio_lot_id", sa.Integer(), nullable=False),
        sa.Column("trade_date", sa.Text(), nullable=False),
        sa.Column("close_price", sa.Float(), nullable=False),
        sa.Column("market_value", sa.Float(), nullable=False),
        sa.Column("unrealized_pnl", sa.Float(), nullable=False),
        sa.Column("unrealized_return_pct", sa.Float(), nullable=False),
        sa.Column("is_in_top20", sa.Integer(), nullable=False),
        sa.Column("is_in_top10", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["portfolio_lot_id"], ["portfolio_lots.id"], name=op.f("fk_portfolio_marks_portfolio_lot_id_portfolio_lots"), ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_portfolio_marks")),
        sa.UniqueConstraint("portfolio_lot_id", "trade_date", name=op.f("uq_portfolio_marks_portfolio_lot_id")),
    )
    op.create_index("idx_portfolio_marks_lot_date", "portfolio_marks", ["portfolio_lot_id", "trade_date"], unique=False)
    op.create_index("idx_portfolio_marks_trade_date", "portfolio_marks", ["trade_date"], unique=False)

    op.create_table(
        "jobs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("job_type", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.Column("started_at", sa.Text(), nullable=True),
        sa.Column("finished_at", sa.Text(), nullable=True),
        sa.Column("requested_by", sa.Text(), nullable=True),
        sa.Column("log_output", sa.Text(), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_jobs")),
    )
    op.create_index("idx_jobs_created_at", "jobs", ["created_at"], unique=False)

    op.create_table(
        "job_steps",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("job_id", sa.Integer(), nullable=False),
        sa.Column("step_order", sa.Integer(), nullable=False),
        sa.Column("step_name", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("command", sa.Text(), nullable=True),
        sa.Column("started_at", sa.Text(), nullable=True),
        sa.Column("finished_at", sa.Text(), nullable=True),
        sa.Column("log_output", sa.Text(), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], name=op.f("fk_job_steps_job_id_jobs"), ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_job_steps")),
    )
    op.create_index("idx_job_steps_job_order", "job_steps", ["job_id", "step_order"], unique=False)


def downgrade() -> None:
    op.drop_index("idx_job_steps_job_order", table_name="job_steps")
    op.drop_table("job_steps")
    op.drop_index("idx_jobs_created_at", table_name="jobs")
    op.drop_table("jobs")
    op.drop_index("idx_portfolio_marks_trade_date", table_name="portfolio_marks")
    op.drop_index("idx_portfolio_marks_lot_date", table_name="portfolio_marks")
    op.drop_table("portfolio_marks")
    op.drop_index("idx_portfolio_lots_entry_date", table_name="portfolio_lots")
    op.drop_index("idx_portfolio_lots_run_symbol", table_name="portfolio_lots")
    op.drop_table("portfolio_lots")
    op.drop_index("idx_portfolio_runs_created_at", table_name="portfolio_runs")
    op.drop_table("portfolio_runs")
    op.drop_index("idx_recommendations_symbol_date", table_name="recommendations")
    op.drop_index("idx_recommendations_run_id", table_name="recommendations")
    op.drop_table("recommendations")
    op.drop_index("idx_runs_model_id", table_name="runs")
    op.drop_index("idx_runs_signal_date", table_name="runs")
    op.drop_index("idx_runs_created_at", table_name="runs")
    op.drop_table("runs")
    op.drop_table("schedules")
    op.drop_table("models")
