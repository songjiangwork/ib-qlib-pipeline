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
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_portfolio_runs_universe_id ON portfolio_runs(universe_id, end_signal_date DESC)"))

        schedule_columns = _column_names(engine, "schedules")
        if "schedule_type" not in schedule_columns:
            conn.execute(text("ALTER TABLE schedules ADD COLUMN schedule_type TEXT NOT NULL DEFAULT 'ranking'"))
        if "pipeline_start_date" not in schedule_columns:
            conn.execute(text("ALTER TABLE schedules ADD COLUMN pipeline_start_date TEXT"))
        if "pipeline_include_portfolio" not in schedule_columns:
            conn.execute(text("ALTER TABLE schedules ADD COLUMN pipeline_include_portfolio INTEGER NOT NULL DEFAULT 1"))
