from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from sqlalchemy import inspect

from ib_qlib_pipeline.dborm.session import create_engine_for_path
from ib_qlib_pipeline.webapi.db import init_db


class DbBootstrapTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmp.name) / "ranking_service.db"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_init_db_creates_expected_tables_and_is_idempotent(self) -> None:
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
