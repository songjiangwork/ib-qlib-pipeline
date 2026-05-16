from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import alembic.command as command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

from ib_qlib_pipeline.dborm.session import create_engine_for_path
from ib_qlib_pipeline.webapi.db import init_db


class DbBootstrapTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmp.name) / "ranking_service.db"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_init_db_creates_expected_tables_and_is_idempotent(self) -> None:
        with patch("ib_qlib_pipeline.webapi.db._migrate_schema", side_effect=AssertionError("should not run")):
            init_db(self.db_path)
            init_db(self.db_path)

        inspector = inspect(create_engine_for_path(self.db_path))
        tables = set(inspector.get_table_names())
        self.assertIn("universes", tables)
        self.assertIn("universe_symbols", tables)
        self.assertIn("instruments", tables)
        self.assertIn("instrument_aliases", tables)
        self.assertIn("models", tables)
        self.assertIn("strategies", tables)
        self.assertIn("runs", tables)
        self.assertIn("portfolio_runs", tables)
        self.assertIn("jobs", tables)
        self.assertIn("job_log_lines", tables)
        self.assertNotIn("prices_daily", tables)

        run_columns = {column["name"] for column in inspector.get_columns("runs")}
        self.assertIn("model_id", run_columns)
        self.assertIn("universe_id", run_columns)

        strategy_columns = {column["name"] for column in inspector.get_columns("strategies")}
        self.assertIn("max_position_notional", strategy_columns)
        self.assertIn("max_position_pct", strategy_columns)
        self.assertIn("gap_up_limit_pct", strategy_columns)

        portfolio_run_columns = {column["name"] for column in inspector.get_columns("portfolio_runs")}
        self.assertIn("strategy_id", portfolio_run_columns)
        self.assertIn("strategy_config_json", portfolio_run_columns)

        schedule_columns = {column["name"] for column in inspector.get_columns("schedules")}
        self.assertIn("schedule_type", schedule_columns)
        self.assertIn("pipeline_start_date", schedule_columns)
        self.assertIn("pipeline_include_portfolio", schedule_columns)

        job_columns = {column["name"] for column in inspector.get_columns("jobs")}
        self.assertIn("worker_id", job_columns)
        self.assertIn("pid", job_columns)
        self.assertIn("heartbeat_at", job_columns)
        self.assertIn("locked_at", job_columns)
        self.assertIn("cancel_requested_at", job_columns)
        self.assertIn("cancel_reason", job_columns)

    def test_alembic_upgrade_head_migrates_legacy_schema(self) -> None:
        engine = create_engine(f"sqlite:///{self.db_path}")
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)"))
            conn.execute(text("INSERT INTO alembic_version(version_num) VALUES ('0002_instrument_static_info')"))
            conn.execute(text("CREATE TABLE models (id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT NOT NULL, name TEXT NOT NULL, model_class TEXT NOT NULL, module_path TEXT NOT NULL, workflow_base TEXT, details_json TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"))
            conn.execute(text("CREATE TABLE schedules (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, enabled BOOLEAN NOT NULL DEFAULT 1, timezone TEXT NOT NULL, day_of_week TEXT NOT NULL DEFAULT 'mon-fri', hour INTEGER NOT NULL, minute INTEGER NOT NULL, client_id INTEGER NOT NULL DEFAULT 151, lookback_days INTEGER NOT NULL DEFAULT 7, workflow_base TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, last_triggered_at TEXT, last_run_id INTEGER, last_run_status TEXT)"))
            conn.execute(text("CREATE TABLE runs (id INTEGER PRIMARY KEY AUTOINCREMENT, schedule_id INTEGER, trigger_source TEXT NOT NULL, status TEXT NOT NULL, client_id INTEGER NOT NULL, lookback_days INTEGER NOT NULL, workflow_base TEXT NOT NULL, command TEXT NOT NULL, created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT, signal_date TEXT, ranking_csv_path TEXT, html_report_path TEXT, experiment_id TEXT, recorder_id TEXT, row_count INTEGER, log_output TEXT, error_text TEXT)"))
            conn.execute(text("CREATE INDEX idx_runs_created_at ON runs(created_at)"))
            conn.execute(text("CREATE INDEX idx_runs_signal_date ON runs(signal_date)"))
            conn.execute(text("CREATE TABLE recommendations (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER NOT NULL, rank INTEGER NOT NULL, symbol TEXT NOT NULL, score REAL NOT NULL, percentile REAL, entry_price REAL, signal_date TEXT NOT NULL)"))
            conn.execute(text("CREATE TABLE portfolio_runs (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, strategy TEXT NOT NULL, buy_top_n INTEGER NOT NULL, hold_top_n INTEGER NOT NULL, target_notional REAL NOT NULL, start_signal_date TEXT NOT NULL, end_signal_date TEXT, created_at TEXT NOT NULL, notes TEXT)"))
            conn.execute(text("CREATE INDEX idx_portfolio_runs_created_at ON portfolio_runs(created_at)"))
            conn.execute(text("CREATE TABLE jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, job_type TEXT NOT NULL, title TEXT NOT NULL, status TEXT NOT NULL, payload_json TEXT, created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT, requested_by TEXT, log_output TEXT, error_text TEXT)"))
            conn.execute(text("CREATE INDEX idx_jobs_created_at ON jobs(created_at)"))
            conn.execute(text("CREATE TABLE job_steps (id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER NOT NULL, step_order INTEGER NOT NULL, step_name TEXT NOT NULL, status TEXT NOT NULL, command TEXT, started_at TEXT, finished_at TEXT, log_output TEXT, error_text TEXT)"))
            conn.execute(text("CREATE INDEX idx_job_steps_job_order ON job_steps(job_id, step_order)"))
            conn.execute(text("CREATE TABLE instruments (id INTEGER PRIMARY KEY AUTOINCREMENT, canonical_symbol TEXT NOT NULL, display_symbol TEXT NOT NULL, asset_type TEXT NOT NULL, country TEXT, exchange TEXT, currency TEXT, name_en TEXT, name_zh TEXT, sector TEXT, industry TEXT, sub_industry TEXT, business_summary TEXT, website TEXT, ipo_date TEXT, delist_date TEXT, listing_status TEXT, source TEXT, source_updated_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"))
            conn.execute(text("CREATE UNIQUE INDEX uq_instruments_canonical_symbol ON instruments(canonical_symbol)"))
            conn.execute(text("CREATE TABLE instrument_aliases (id INTEGER PRIMARY KEY AUTOINCREMENT, instrument_id INTEGER NOT NULL, alias_type TEXT NOT NULL, alias_value TEXT NOT NULL, created_at TEXT NOT NULL)"))

        cfg = Config(str(Path(__file__).resolve().parent.parent / "alembic.ini"))
        cfg.set_main_option("script_location", str(Path(__file__).resolve().parent.parent / "alembic"))
        with patch.dict("os.environ", {"RANKING_API_DB_PATH": str(self.db_path)}):
            command.upgrade(cfg, "head")

        inspector = inspect(create_engine_for_path(self.db_path))
        tables = set(inspector.get_table_names())
        self.assertIn("universes", tables)
        self.assertIn("strategies", tables)
        self.assertIn("universe_symbols", tables)
        self.assertIn("job_log_lines", tables)

        model_columns = {column["name"] for column in inspector.get_columns("models")}
        self.assertIn("universe_id", model_columns)

        run_columns = {column["name"] for column in inspector.get_columns("runs")}
        self.assertIn("model_id", run_columns)
        self.assertIn("universe_id", run_columns)

        portfolio_run_columns = {column["name"] for column in inspector.get_columns("portfolio_runs")}
        self.assertIn("strategy_id", portfolio_run_columns)
        self.assertIn("strategy_config_json", portfolio_run_columns)

        schedule_columns = {column["name"] for column in inspector.get_columns("schedules")}
        self.assertIn("schedule_type", schedule_columns)
        self.assertIn("pipeline_start_date", schedule_columns)
        self.assertIn("pipeline_include_portfolio", schedule_columns)

        job_columns = {column["name"] for column in inspector.get_columns("jobs")}
        self.assertIn("worker_id", job_columns)
        self.assertIn("pid", job_columns)
        self.assertIn("heartbeat_at", job_columns)
        self.assertIn("locked_at", job_columns)
        self.assertIn("cancel_requested_at", job_columns)
        self.assertIn("cancel_reason", job_columns)

        indexes = {idx["name"] for idx in inspector.get_indexes("job_log_lines")}
        self.assertIn("idx_job_log_lines_job_id", indexes)
        self.assertIn("idx_job_log_lines_step_id", indexes)

    def test_consolidation_revision_exists(self) -> None:
        revision_path = Path(__file__).resolve().parent.parent / "alembic" / "versions" / "0003_consolidate_webapi_schema.py"
        self.assertTrue(revision_path.exists())
        content = revision_path.read_text(encoding="utf-8")
        self.assertIn("job_log_lines", content)
        self.assertIn("worker_id", content)
        self.assertIn("universes", content)
        self.assertIn("strategies", content)
