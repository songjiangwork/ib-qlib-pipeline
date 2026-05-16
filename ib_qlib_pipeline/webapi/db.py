from __future__ import annotations

from pathlib import Path

from ..dborm.base import Base
from ..dborm import models as _models  # noqa: F401
from ..dborm.session import create_engine_for_path


RANKING_DB_TABLES = [
    "universes",
    "universe_symbols",
    "instruments",
    "instrument_aliases",
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
    "job_log_lines",
]


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine_for_path(db_path)
    tables = [Base.metadata.tables[name] for name in RANKING_DB_TABLES]
    Base.metadata.create_all(bind=engine, tables=tables, checkfirst=True)


def _migrate_schema(_engine) -> None:
    """Deprecated no-op.

    Historical schema upgrades are managed by Alembic revisions.
    This function remains only as a compatibility placeholder for callers
    that may still import it during tests or local experiments.
    """
    return None
