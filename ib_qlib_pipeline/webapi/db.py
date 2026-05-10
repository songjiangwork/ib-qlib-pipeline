from __future__ import annotations

from pathlib import Path

from sqlalchemy import inspect, text

from ..dborm.base import Base
from ..dborm import models as _models  # noqa: F401
from ..dborm.session import create_engine_for_path


RANKING_DB_TABLES = [
    "universes",
    "universe_symbols",
    "models",
    "strategies",
    "schedules",
    "runs",
    "recommendations",
    "portfolio_runs",
    "portfolio_lots",
    "portfolio_marks",
    "jobs",
    "job_steps",
]


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine_for_path(db_path)
    tables = [Base.metadata.tables[name] for name in RANKING_DB_TABLES]
    Base.metadata.create_all(bind=engine, tables=tables, checkfirst=True)
    _migrate_schema(engine)


def _column_names(engine, table_name: str) -> set[str]:
    inspector = inspect(engine)
    return {str(column["name"]) for column in inspector.get_columns(table_name)}


def _migrate_schema(engine) -> None:
    with engine.begin() as conn:
        model_columns = _column_names(engine, "models")
        if "universe_id" not in model_columns:
            conn.execute(text("ALTER TABLE models ADD COLUMN universe_id INTEGER REFERENCES universes(id) ON DELETE SET NULL"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_models_universe_id ON models(universe_id, key)"))

        strategy_columns = _column_names(engine, "strategies")
        if "universe_id" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN universe_id INTEGER REFERENCES universes(id) ON DELETE SET NULL"))
        if "description" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN description TEXT"))
        if "entry_rule" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN entry_rule TEXT"))
        if "exit_rule" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN exit_rule TEXT"))
        if "signal_timing" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN signal_timing TEXT"))
        if "execution_timing" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN execution_timing TEXT"))
        if "trade_price_basis" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN trade_price_basis TEXT"))
        if "buy_top_n" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN buy_top_n INTEGER"))
        if "hold_top_n" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN hold_top_n INTEGER"))
        if "hold_days" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN hold_days INTEGER"))
        if "target_notional" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN target_notional REAL"))
        if "initial_capital" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN initial_capital REAL"))
        if "max_open_positions" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN max_open_positions INTEGER"))
        if "max_position_notional" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN max_position_notional REAL"))
        if "max_position_pct" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN max_position_pct REAL"))
        if "fee_bps" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN fee_bps REAL"))
        if "slippage_bps" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN slippage_bps REAL"))
        if "gap_up_limit_pct" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN gap_up_limit_pct REAL"))
        if "gap_down_limit_pct" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN gap_down_limit_pct REAL"))
        if "allow_reentry" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN allow_reentry BOOLEAN"))
        if "details_json" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN details_json TEXT"))
        if "config_json" not in strategy_columns:
            conn.execute(text("ALTER TABLE strategies ADD COLUMN config_json TEXT"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_strategies_universe_id ON strategies(universe_id, key)"))

        runs_columns = _column_names(engine, "runs")
        if "model_id" not in runs_columns:
            conn.execute(text("ALTER TABLE runs ADD COLUMN model_id INTEGER REFERENCES models(id) ON DELETE SET NULL"))
        if "universe_id" not in runs_columns:
            conn.execute(text("ALTER TABLE runs ADD COLUMN universe_id INTEGER REFERENCES universes(id) ON DELETE SET NULL"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_runs_model_id ON runs(model_id, signal_date DESC)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_runs_universe_id ON runs(universe_id, signal_date DESC)"))

        portfolio_run_columns = _column_names(engine, "portfolio_runs")
        if "universe_id" not in portfolio_run_columns:
            conn.execute(text("ALTER TABLE portfolio_runs ADD COLUMN universe_id INTEGER REFERENCES universes(id) ON DELETE SET NULL"))
        if "strategy_id" not in portfolio_run_columns:
            conn.execute(text("ALTER TABLE portfolio_runs ADD COLUMN strategy_id INTEGER REFERENCES strategies(id) ON DELETE SET NULL"))
        if "strategy_config_json" not in portfolio_run_columns:
            conn.execute(text("ALTER TABLE portfolio_runs ADD COLUMN strategy_config_json TEXT"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_portfolio_runs_universe_id ON portfolio_runs(universe_id, end_signal_date DESC)"))

        schedule_columns = _column_names(engine, "schedules")
        if "schedule_type" not in schedule_columns:
            conn.execute(text("ALTER TABLE schedules ADD COLUMN schedule_type TEXT NOT NULL DEFAULT 'ranking'"))
        if "pipeline_start_date" not in schedule_columns:
            conn.execute(text("ALTER TABLE schedules ADD COLUMN pipeline_start_date TEXT"))
        if "pipeline_include_portfolio" not in schedule_columns:
            conn.execute(text("ALTER TABLE schedules ADD COLUMN pipeline_include_portfolio INTEGER NOT NULL DEFAULT 1"))
