from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from ib_qlib_pipeline.webapi.db import init_db
from ib_qlib_pipeline.webapi.job_store import (
    append_job_log,
    append_job_step_output,
    create_job,
    create_job_step,
    get_job,
    list_jobs,
    mark_job_finished,
    mark_job_running,
    next_job_step_order,
    update_job_step,
)
from ib_qlib_pipeline.webapi.model_store import (
    ensure_default_models,
    get_model,
    get_model_by_key,
    list_models,
    resolve_or_create_model_for_workflow,
)
from ib_qlib_pipeline.webapi.portfolio_store import (
    close_portfolio_lot,
    create_portfolio_run,
    get_portfolio_run,
    insert_portfolio_lot,
    insert_portfolio_mark,
    list_portfolio_lots,
    list_portfolio_marks,
    list_portfolio_runs,
    load_open_lots,
    update_portfolio_run_end_date,
)
from ib_qlib_pipeline.webapi.run_store import insert_completed_run
from ib_qlib_pipeline.webapi.schedule_store import (
    create_schedule,
    delete_schedule,
    get_schedule,
    list_schedules,
    update_schedule,
)


class StoreTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmp.name) / "test.db"
        init_db(self.db_path)
        ensure_default_models(self.db_path)
        self.project_root = Path(__file__).resolve().parent.parent

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _ranking_df(self, signal_date: str = "2026-05-06") -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "rank": 1,
                    "symbol": "AAPL",
                    "score": 0.1,
                    "percentile": 99.0,
                    "close": 200.0,
                    "signal_date": signal_date,
                }
            ]
        )

    def _insert_completed_run(self, signal_date: str = "2026-05-06", model_id: int = 1) -> int:
        return insert_completed_run(
            db_path=self.db_path,
            model_id=model_id,
            trigger_source="backfill",
            client_id=151,
            lookback_days=7,
            workflow_base="examples/workflow_us_lgb_2020_port.yaml",
            command="cmd",
            ranking_df=self._ranking_df(signal_date),
            ranking_csv_path=Path("reports/r.csv"),
            html_report_path=Path("reports/r.html"),
            experiment_id="exp1",
            recorder_id="rec1",
            log_output="ok",
        )

    def test_model_store_default_models_and_lookup(self) -> None:
        models = list_models(self.db_path)
        self.assertEqual(6, len(models))
        self.assertEqual("LightGBM_Default", get_model_by_key(self.db_path, "lgb")["name"])
        self.assertEqual("XGBoost_5D", get_model_by_key(self.db_path, "xgb_5d")["name"])

        model_id = resolve_or_create_model_for_workflow(
            self.db_path,
            self.project_root,
            "examples/workflow_us_catboost_2020_port_next_open_5d.yaml",
        )
        self.assertEqual("CatBoost_5D", get_model(self.db_path, model_id)["name"])

    def test_run_store_insert_completed_run(self) -> None:
        run_id = self._insert_completed_run()
        self.assertGreater(run_id, 0)

    def test_portfolio_store_lifecycle(self) -> None:
        run_id = self._insert_completed_run()
        portfolio_run_id = create_portfolio_run(
            db_path=self.db_path,
            name="test-run",
            strategy="top10-hold20",
            buy_top_n=10,
            hold_top_n=20,
            target_notional=10000.0,
            start_signal_date="2026-05-06",
            end_signal_date="2026-05-06",
            notes="test",
        )

        lot_id = insert_portfolio_lot(
            db_path=self.db_path,
            portfolio_run_id=portfolio_run_id,
            symbol="AAPL",
            entry_run_id=run_id,
            entry_signal_date="2026-05-06",
            entry_trade_date="2026-05-07",
            entry_rank=1,
            entry_price_open=200.0,
            shares=50,
            target_notional=10000.0,
        )
        insert_portfolio_mark(
            db_path=self.db_path,
            portfolio_lot_id=lot_id,
            trade_date="2026-05-07",
            close_price=205.0,
            market_value=10250.0,
            unrealized_pnl=250.0,
            unrealized_return_pct=2.5,
            is_in_top20=True,
            is_in_top10=True,
        )
        self.assertEqual(1, sum(len(v) for v in load_open_lots(self.db_path, portfolio_run_id).values()))

        close_portfolio_lot(
            db_path=self.db_path,
            lot_id=lot_id,
            exit_run_id=run_id,
            exit_signal_date="2026-05-07",
            exit_trade_date="2026-05-08",
            exit_rank=12,
            exit_price_open=210.0,
        )
        update_portfolio_run_end_date(
            db_path=self.db_path,
            portfolio_run_id=portfolio_run_id,
            end_signal_date="2026-05-07",
        )
        insert_portfolio_mark(
            db_path=self.db_path,
            portfolio_lot_id=lot_id,
            trade_date="2026-05-08",
            close_price=210.0,
            market_value=10500.0,
            unrealized_pnl=500.0,
            unrealized_return_pct=5.0,
            is_in_top20=False,
            is_in_top10=False,
        )

        portfolio_run = get_portfolio_run(self.db_path, portfolio_run_id)
        lots = list_portfolio_lots(self.db_path, portfolio_run_id)
        marks = list_portfolio_marks(self.db_path, lot_id)
        runs = list_portfolio_runs(self.db_path)

        self.assertEqual(0, sum(len(v) for v in load_open_lots(self.db_path, portfolio_run_id).values()))
        self.assertEqual("LightGBM_Default", portfolio_run["model_name"])
        self.assertEqual(1, portfolio_run["closed_lot_count"])
        self.assertEqual(0, portfolio_run["open_lot_count"])
        self.assertAlmostEqual(5.0, portfolio_run["avg_return_pct"])
        self.assertAlmostEqual(100.0, portfolio_run["win_rate_pct"])
        self.assertEqual("closed", lots[0]["status"])
        self.assertEqual(2, len(marks))
        self.assertEqual(1, len(runs))

    def test_schedule_store_crud(self) -> None:
        created = create_schedule(
            self.db_path,
            {
                "name": "daily close",
                "schedule_type": "daily_close_pipeline",
                "enabled": True,
                "timezone": "America/Denver",
                "day_of_week": "mon-fri",
                "hour": 14,
                "minute": 40,
                "client_id": 151,
                "lookback_days": 7,
                "workflow_base": None,
                "pipeline_start_date": "2026-05-01",
                "pipeline_include_portfolio": True,
            },
            "examples/workflow_us_lgb_2020_port.yaml",
        )
        updated = update_schedule(self.db_path, created["id"], {"enabled": False, "minute": 45})
        fetched = get_schedule(self.db_path, created["id"])

        self.assertEqual(1, len(list_schedules(self.db_path)))
        self.assertFalse(updated["enabled"])
        self.assertEqual(45, fetched["minute"])
        self.assertTrue(delete_schedule(self.db_path, created["id"]))
        self.assertEqual([], list_schedules(self.db_path))

    def test_job_store_lifecycle(self) -> None:
        job_id = create_job(
            self.db_path,
            job_type="daily_close_pipeline",
            title="Daily Close",
            payload_json="{}",
        )
        mark_job_running(self.db_path, job_id, "2026-05-07T00:00:00Z")
        step_id = create_job_step(
            self.db_path,
            job_id=job_id,
            step_order=next_job_step_order(self.db_path, job_id),
            step_name="refresh_data",
            status="running",
            command="python run.py",
            started_at="2026-05-07T00:00:01Z",
        )
        append_job_step_output(self.db_path, step_id=step_id, line="line1")
        append_job_step_output(self.db_path, step_id=step_id, line="line2")
        update_job_step(
            self.db_path,
            step_id=step_id,
            status="succeeded",
            finished_at="2026-05-07T00:00:02Z",
            log_output="line1\nline2",
            error_text=None,
        )
        append_job_log(self.db_path, job_id=job_id, section_name="refresh_data", content="line1\nline2")
        mark_job_finished(
            self.db_path,
            job_id,
            status="succeeded",
            finished_at="2026-05-07T00:00:03Z",
            error_text=None,
        )

        jobs = list_jobs(self.db_path)
        job = get_job(self.db_path, job_id)
        self.assertEqual(1, len(jobs))
        self.assertEqual("succeeded", job["status"])
        self.assertEqual(1, len(job["steps"]))
        self.assertEqual("succeeded", job["steps"][0]["status"])
        self.assertIn("[refresh_data]", job["log_output"])


if __name__ == "__main__":
    unittest.main()
