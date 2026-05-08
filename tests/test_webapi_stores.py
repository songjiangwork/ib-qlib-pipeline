from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from ib_qlib_pipeline.dborm.models import Run
from ib_qlib_pipeline.dborm.session import create_session_factory_for_path
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
from ib_qlib_pipeline.webapi.service import RankingBackendService
from ib_qlib_pipeline.webapi.settings import Settings


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

    def _make_service(self) -> RankingBackendService:
        settings = Settings(
            project_root=self.project_root,
            db_path=self.db_path,
            timezone="America/Denver",
            default_workflow_base="examples/workflow_us_lgb_2020_port.yaml",
            run_script_path=self.project_root / "run_daily_ranking.sh",
            api_host="127.0.0.1",
            api_port=8001,
        )
        return RankingBackendService(settings)

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

    def test_service_run_queries(self) -> None:
        service = self._make_service()
        first_run_id = self._insert_completed_run(signal_date="2026-05-05", model_id=1)
        second_run_id = self._insert_completed_run(signal_date="2026-05-06", model_id=2)

        all_runs = service.list_runs(limit=10)
        succeeded_0505 = service.list_runs(limit=10, status="succeeded", signal_date="2026-05-05")
        run = service.get_run(second_run_id)
        recs = service.get_run_recommendations(second_run_id)

        self.assertEqual(2, len(all_runs))
        self.assertEqual(second_run_id, all_runs[0]["id"])
        self.assertEqual(1, len(succeeded_0505))
        self.assertEqual(first_run_id, succeeded_0505[0]["id"])
        self.assertEqual("XGBoost_Default", run["model_name"])
        self.assertEqual("xgb", run["model_key"])
        self.assertEqual(1, len(recs))
        self.assertEqual("AAPL", recs[0]["symbol"])

    def test_service_create_run_record_updates_schedule(self) -> None:
        service = self._make_service()
        schedule = create_schedule(
            self.db_path,
            {
                "name": "ranking",
                "schedule_type": "ranking",
                "enabled": True,
                "timezone": "America/Denver",
                "day_of_week": "mon-fri",
                "hour": 14,
                "minute": 40,
                "client_id": 151,
                "lookback_days": 7,
                "workflow_base": "examples/workflow_us_lgb_2020_port.yaml",
                "pipeline_start_date": None,
                "pipeline_include_portfolio": True,
            },
            "examples/workflow_us_lgb_2020_port.yaml",
        )

        run_id = service._create_run_record(
            schedule_id=schedule["id"],
            trigger_source="schedule",
            client_id=151,
            lookback_days=7,
            workflow_base="examples/workflow_us_lgb_2020_port.yaml",
        )

        run = service.get_run(run_id)
        updated_schedule = get_schedule(self.db_path, schedule["id"])
        self.assertEqual("queued", run["status"])
        self.assertEqual("schedule", run["trigger_source"])
        self.assertEqual(run_id, updated_schedule["last_run_id"])
        self.assertEqual("queued", updated_schedule["last_run_status"])

    def test_service_mark_run_failed_updates_schedule(self) -> None:
        service = self._make_service()
        schedule = create_schedule(
            self.db_path,
            {
                "name": "ranking",
                "schedule_type": "ranking",
                "enabled": True,
                "timezone": "America/Denver",
                "day_of_week": "mon-fri",
                "hour": 14,
                "minute": 40,
                "client_id": 151,
                "lookback_days": 7,
                "workflow_base": "examples/workflow_us_lgb_2020_port.yaml",
                "pipeline_start_date": None,
                "pipeline_include_portfolio": True,
            },
            "examples/workflow_us_lgb_2020_port.yaml",
        )
        run_id = service._create_run_record(
            schedule_id=schedule["id"],
            trigger_source="schedule",
            client_id=151,
            lookback_days=7,
            workflow_base="examples/workflow_us_lgb_2020_port.yaml",
        )

        service._mark_run_failed(run_id, schedule["id"], "boom")

        run = service.get_run(run_id)
        updated_schedule = get_schedule(self.db_path, schedule["id"])
        self.assertEqual("failed", run["status"])
        self.assertEqual("boom", run["error_text"])
        self.assertEqual("failed", updated_schedule["last_run_status"])

    def test_service_backfill_legacy_run_models_assigns_default_lgb(self) -> None:
        service = self._make_service()
        session = create_session_factory_for_path(self.db_path)()
        try:
            legacy_run = Run(
                schedule_id=None,
                model_id=None,
                trigger_source="manual",
                status="queued",
                client_id=151,
                lookback_days=7,
                workflow_base="examples/workflow_us_lgb_2020_port.yaml",
                command="cmd",
                created_at="2026-05-07T00:00:00+00:00",
            )
            session.add(legacy_run)
            session.commit()
            session.refresh(legacy_run)
            run_id = int(legacy_run.id)
        finally:
            session.close()

        service._backfill_legacy_run_models()

        run = service.get_run(run_id)
        self.assertEqual(get_model_by_key(self.db_path, "lgb")["id"], run["model_id"])
        self.assertEqual("LightGBM_Default", run["model_name"])

    def test_service_start_job_marks_schedule_queued(self) -> None:
        service = self._make_service()
        schedule = create_schedule(
            self.db_path,
            {
                "name": "ranking",
                "schedule_type": "ranking",
                "enabled": True,
                "timezone": "America/Denver",
                "day_of_week": "mon-fri",
                "hour": 14,
                "minute": 40,
                "client_id": 151,
                "lookback_days": 7,
                "workflow_base": "examples/workflow_us_lgb_2020_port.yaml",
                "pipeline_start_date": None,
                "pipeline_include_portfolio": True,
            },
            "examples/workflow_us_lgb_2020_port.yaml",
        )

        class FakeThread:
            def __init__(self, *args, **kwargs) -> None:
                self.args = args
                self.kwargs = kwargs

            def start(self) -> None:
                return None

        with patch("ib_qlib_pipeline.webapi.service.threading.Thread", FakeThread):
            job = service._start_job(
                job_type="refresh_data",
                title="Refresh",
                payload={"start_date": "2026-05-01", "client_id": 151},
                target=lambda *_args, **_kwargs: None,
                schedule_id=schedule["id"],
            )

        updated_schedule = get_schedule(self.db_path, schedule["id"])
        self.assertEqual("queued", job["status"])
        self.assertEqual("queued", updated_schedule["last_run_status"])
        self.assertIsNotNone(updated_schedule["last_triggered_at"])
        if service._run_lock.locked():
            service._run_lock.release()

    def test_service_execute_job_thread_success_updates_schedule_and_releases_lock(self) -> None:
        service = self._make_service()
        schedule = create_schedule(
            self.db_path,
            {
                "name": "ranking",
                "schedule_type": "ranking",
                "enabled": True,
                "timezone": "America/Denver",
                "day_of_week": "mon-fri",
                "hour": 14,
                "minute": 40,
                "client_id": 151,
                "lookback_days": 7,
                "workflow_base": "examples/workflow_us_lgb_2020_port.yaml",
                "pipeline_start_date": None,
                "pipeline_include_portfolio": True,
            },
            "examples/workflow_us_lgb_2020_port.yaml",
        )
        job_id = create_job(
            self.db_path,
            job_type="refresh_data",
            title="Refresh",
            payload_json='{"start_date":"2026-05-01","client_id":151}',
        )

        service._run_lock.acquire()
        service._execute_job_thread(
            job_id,
            target=lambda *_args, **_kwargs: None,
            payload={"start_date": "2026-05-01", "client_id": 151},
            schedule_id=schedule["id"],
        )

        job = get_job(self.db_path, job_id)
        updated_schedule = get_schedule(self.db_path, schedule["id"])
        self.assertEqual("succeeded", job["status"])
        self.assertEqual("succeeded", updated_schedule["last_run_status"])
        self.assertFalse(service._run_lock.locked())

    def test_service_finalize_run_from_output_updates_run_and_recommendations(self) -> None:
        service = self._make_service()
        schedule = create_schedule(
            self.db_path,
            {
                "name": "ranking",
                "schedule_type": "ranking",
                "enabled": True,
                "timezone": "America/Denver",
                "day_of_week": "mon-fri",
                "hour": 14,
                "minute": 40,
                "client_id": 151,
                "lookback_days": 7,
                "workflow_base": "examples/workflow_us_lgb_2020_port.yaml",
                "pipeline_start_date": None,
                "pipeline_include_portfolio": True,
            },
            "examples/workflow_us_lgb_2020_port.yaml",
        )
        run_id = service._create_run_record(
            schedule_id=schedule["id"],
            trigger_source="schedule",
            client_id=151,
            lookback_days=7,
            workflow_base="examples/workflow_us_lgb_2020_port.yaml",
        )

        ranking_csv = self.project_root / "tmp_test_finalize_ranking.csv"
        try:
            pd.DataFrame(
                [
                    {
                        "rank": 1,
                        "symbol": "AAPL",
                        "score": 0.12,
                        "percentile": 99.0,
                        "close": 210.0,
                        "signal_date": "2026-05-06",
                    },
                    {
                        "rank": 2,
                        "symbol": "MSFT",
                        "score": 0.11,
                        "percentile": 98.0,
                        "close": 310.0,
                        "signal_date": "2026-05-06",
                    },
                ]
            ).to_csv(ranking_csv, index=False)

            output = "\n".join(
                [
                    f"[ok] ranking exported: {ranking_csv}",
                    "[ok] html report exported: reports/test.html",
                    "[ok] signal_date=2026-05-06 rows=2 missing_close=0",
                    "[ok] experiment_id=exp-test recorder_id=rec-test",
                ]
            )
            service._finalize_run_from_output(run_id, schedule["id"], output)

            run = service.get_run(run_id)
            recs = service.get_run_recommendations(run_id)
            updated_schedule = get_schedule(self.db_path, schedule["id"])
            self.assertEqual("succeeded", run["status"])
            self.assertEqual("2026-05-06", run["signal_date"])
            self.assertEqual(2, run["row_count"])
            self.assertEqual(2, len(recs))
            self.assertEqual("AAPL", recs[0]["symbol"])
            self.assertEqual("succeeded", updated_schedule["last_run_status"])
        finally:
            if ranking_csv.exists():
                ranking_csv.unlink()

    def test_service_execute_run_success_updates_run_and_schedule(self) -> None:
        service = self._make_service()
        schedule = create_schedule(
            self.db_path,
            {
                "name": "ranking",
                "schedule_type": "ranking",
                "enabled": True,
                "timezone": "America/Denver",
                "day_of_week": "mon-fri",
                "hour": 14,
                "minute": 40,
                "client_id": 151,
                "lookback_days": 7,
                "workflow_base": "examples/workflow_us_lgb_2020_port.yaml",
                "pipeline_start_date": None,
                "pipeline_include_portfolio": True,
            },
            "examples/workflow_us_lgb_2020_port.yaml",
        )
        run_id = service._create_run_record(
            schedule_id=schedule["id"],
            trigger_source="schedule",
            client_id=151,
            lookback_days=7,
            workflow_base="examples/workflow_us_lgb_2020_port.yaml",
        )
        ranking_csv = self.project_root / "tmp_test_execute_run.csv"
        try:
            pd.DataFrame(
                [
                    {
                        "rank": 1,
                        "symbol": "AAPL",
                        "score": 0.12,
                        "percentile": 99.0,
                        "close": 210.0,
                        "signal_date": "2026-05-06",
                    }
                ]
            ).to_csv(ranking_csv, index=False)
            output = "\n".join(
                [
                    f"[ok] ranking exported: {ranking_csv}",
                    "[ok] html report exported: reports/test.html",
                    "[ok] signal_date=2026-05-06 rows=1 missing_close=0",
                    "[ok] experiment_id=exp-test recorder_id=rec-test",
                ]
            )
            proc = SimpleNamespace(returncode=0, stdout=output, stderr="")
            service._run_lock.acquire()
            with patch("ib_qlib_pipeline.webapi.service.subprocess.run", return_value=proc):
                service._execute_run(run_id, schedule["id"], ["dummy"])

            run = service.get_run(run_id)
            updated_schedule = get_schedule(self.db_path, schedule["id"])
            recs = service.get_run_recommendations(run_id)
            self.assertEqual("succeeded", run["status"])
            self.assertEqual("succeeded", updated_schedule["last_run_status"])
            self.assertEqual(1, len(recs))
            self.assertFalse(service._run_lock.locked())
        finally:
            if ranking_csv.exists():
                ranking_csv.unlink()

    def test_service_execute_run_failure_updates_run_and_schedule(self) -> None:
        service = self._make_service()
        schedule = create_schedule(
            self.db_path,
            {
                "name": "ranking",
                "schedule_type": "ranking",
                "enabled": True,
                "timezone": "America/Denver",
                "day_of_week": "mon-fri",
                "hour": 14,
                "minute": 40,
                "client_id": 151,
                "lookback_days": 7,
                "workflow_base": "examples/workflow_us_lgb_2020_port.yaml",
                "pipeline_start_date": None,
                "pipeline_include_portfolio": True,
            },
            "examples/workflow_us_lgb_2020_port.yaml",
        )
        run_id = service._create_run_record(
            schedule_id=schedule["id"],
            trigger_source="schedule",
            client_id=151,
            lookback_days=7,
            workflow_base="examples/workflow_us_lgb_2020_port.yaml",
        )
        proc = SimpleNamespace(returncode=1, stdout="", stderr="boom")
        service._run_lock.acquire()
        with patch("ib_qlib_pipeline.webapi.service.subprocess.run", return_value=proc):
            service._execute_run(run_id, schedule["id"], ["dummy"])

        run = service.get_run(run_id)
        updated_schedule = get_schedule(self.db_path, schedule["id"])
        self.assertEqual("failed", run["status"])
        self.assertEqual("boom", run["error_text"])
        self.assertEqual("failed", updated_schedule["last_run_status"])
        self.assertFalse(service._run_lock.locked())

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

    def test_service_command_builders(self) -> None:
        service = self._make_service()

        refresh = service._build_refresh_data_command(client_id=151, start_date="2026-05-01")
        backfill = service._build_backfill_command(
            signal_date="2026-05-06",
            workflow_base="examples/workflow_us_xgb_2020_port.yaml",
            model_id=2,
            client_id=151,
        )
        append = service._build_append_portfolio_command(
            portfolio_run_id=42,
            model_id=3,
            end_date="2026-05-05",
        )

        self.assertEqual("run.py", refresh[1])
        self.assertIn("--dump-bin", refresh)
        self.assertEqual("backfill_rankings.py", backfill[1])
        self.assertIn("--skip-existing-db", backfill)
        self.assertIn("--model-id", backfill)
        self.assertEqual("simulate_portfolio.py", append[1])
        self.assertEqual(["--append-run-id", "42"], append[2:4])
        self.assertTrue(append[-2:] == ["--model-id", "3"])

    def test_service_previous_trading_day_and_missing_days(self) -> None:
        service = self._make_service()
        trading_days = [
            pd.Timestamp("2026-05-01").date(),
            pd.Timestamp("2026-05-04").date(),
            pd.Timestamp("2026-05-05").date(),
            pd.Timestamp("2026-05-06").date(),
        ]

        with patch("ib_qlib_pipeline.webapi.service.read_available_trading_days", return_value=trading_days):
            self.assertEqual(pd.Timestamp("2026-05-05").date(), service._previous_trading_day(pd.Timestamp("2026-05-06").date()))

        self._insert_completed_run(signal_date="2026-05-05", model_id=1)
        missing = service._missing_signal_days_for_model(
            model_id=1,
            trade_date=pd.Timestamp("2026-05-06").date(),
            trading_days=trading_days,
            fallback_start_date=pd.Timestamp("2026-05-01").date(),
        )
        self.assertEqual([pd.Timestamp("2026-05-06").date()], missing)

    def test_service_operations_summary_ready_flags(self) -> None:
        service = self._make_service()
        run_id = self._insert_completed_run(signal_date="2026-05-06", model_id=1)
        portfolio_run_id = create_portfolio_run(
            db_path=self.db_path,
            name="lgb-run",
            strategy="top10-hold20",
            buy_top_n=10,
            hold_top_n=20,
            target_notional=10000.0,
            start_signal_date="2026-05-01",
            end_signal_date="2026-05-05",
            notes="test",
        )
        insert_portfolio_lot(
            db_path=self.db_path,
            portfolio_run_id=portfolio_run_id,
            symbol="AAPL",
            entry_run_id=run_id,
            entry_signal_date="2026-05-05",
            entry_trade_date="2026-05-06",
            entry_rank=1,
            entry_price_open=200.0,
            shares=50,
            target_notional=10000.0,
        )

        with patch(
            "ib_qlib_pipeline.webapi.service.read_available_trading_days",
            return_value=[
                pd.Timestamp("2026-05-05").date(),
                pd.Timestamp("2026-05-06").date(),
            ],
        ):
            summary = service.get_operations_summary("2026-05-06")

        lgb = next(item for item in summary["models"] if item["model_key"] == "lgb")
        self.assertTrue(lgb["ranking_ready_for_trade_date"])
        self.assertTrue(lgb["portfolio_ready_for_trade_date"])
        self.assertEqual("2026-05-05", lgb["expected_portfolio_end_signal_date"])

    def test_service_latest_model_state_helpers(self) -> None:
        service = self._make_service()
        run_id = self._insert_completed_run(signal_date="2026-05-06", model_id=1)
        create_portfolio_run(
            db_path=self.db_path,
            name="lgb-run-latest",
            strategy="top10-hold20",
            buy_top_n=10,
            hold_top_n=20,
            target_notional=10000.0,
            start_signal_date="2026-05-01",
            end_signal_date="2026-05-05",
            notes="test",
        )
        portfolio_run_id = create_portfolio_run(
            db_path=self.db_path,
            name="lgb-run-newest",
            strategy="top10-hold20",
            buy_top_n=10,
            hold_top_n=20,
            target_notional=10000.0,
            start_signal_date="2026-05-02",
            end_signal_date="2026-05-06",
            notes="test",
        )
        insert_portfolio_lot(
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

        latest_signal = service._latest_backfill_signal_date_for_model(1)
        latest_portfolio = service._latest_portfolio_run_for_model(1)

        self.assertEqual(pd.Timestamp("2026-05-06").date(), latest_signal)
        self.assertIsNotNone(latest_portfolio)
        self.assertEqual(portfolio_run_id, latest_portfolio["id"])

    def test_service_list_ranking_dates_filters_and_pagination(self) -> None:
        service = self._make_service()
        self._insert_completed_run(signal_date="2026-05-05", model_id=1)
        self._insert_completed_run(signal_date="2026-05-06", model_id=1)
        self._insert_completed_run(signal_date="2026-05-06", model_id=2)

        all_lgb = service.list_ranking_dates(limit=10, offset=0, model_id=1)
        paged = service.list_ranking_dates(limit=1, offset=0, model_id=1)
        queried = service.list_ranking_dates(limit=10, offset=0, query="2026-05-05", model_id=1)

        self.assertEqual(2, all_lgb["total"])
        self.assertEqual(2, len(all_lgb["items"]))
        self.assertEqual("2026-05-06", all_lgb["items"][0]["signal_date"])
        self.assertTrue(paged["has_more"])
        self.assertEqual(1, paged["next_offset"])
        self.assertEqual(1, queried["total"])
        self.assertEqual("2026-05-05", queried["items"][0]["signal_date"])

    def test_service_trigger_methods_build_expected_jobs(self) -> None:
        service = self._make_service()
        with patch.object(service, "_start_job", return_value={"id": 123}) as start_job:
            result = service.trigger_manual_run(
                {"client_id": 151, "lookback_days": 7, "workflow_base": "examples/workflow_us_lgb_2020_port.yaml"}
            )
            self.assertEqual({"id": 123}, result)
            start_job.assert_called_once()
            kwargs = start_job.call_args.kwargs
            self.assertEqual("manual_ranking_run", kwargs["job_type"])
            self.assertEqual("manual", kwargs["payload"]["trigger_source"])

        with patch.object(service, "_start_job", return_value={"id": 124}) as start_job:
            result = service.trigger_backfill_job({"signal_date": "2026-05-06", "model_id": 1, "client_id": 151})
            self.assertEqual({"id": 124}, result)
            kwargs = start_job.call_args.kwargs
            self.assertEqual("backfill_ranking", kwargs["job_type"])
            self.assertIn("LightGBM_Default", kwargs["title"])

        with patch.object(service, "_start_job", return_value={"id": 125}) as start_job:
            result = service.trigger_append_portfolio_job({"portfolio_run_id": 4, "model_id": 1, "end_date": "2026-05-06"})
            self.assertEqual({"id": 125}, result)
            kwargs = start_job.call_args.kwargs
            self.assertEqual("append_portfolio", kwargs["job_type"])
            self.assertEqual(4, kwargs["payload"]["portfolio_run_id"])

    def test_service_retry_job_routes_to_correct_handler(self) -> None:
        service = self._make_service()

        with patch.object(service, "trigger_data_refresh_job", return_value={"id": 201}) as target:
            job_id = create_job(
                self.db_path,
                job_type="refresh_data",
                title="Refresh",
                payload_json='{"start_date":"2026-05-01","client_id":151}',
            )
            result = service.retry_job(job_id)
            self.assertEqual({"id": 201}, result)
            target.assert_called_once_with({"start_date": "2026-05-01", "client_id": 151})

        with patch.object(service, "trigger_daily_close_pipeline_job", return_value={"id": 202}) as target:
            job_id = create_job(
                self.db_path,
                job_type="daily_close_pipeline",
                title="Daily Close",
                payload_json='{"trade_date":"2026-05-06","client_id":151,"start_date":"2026-05-01","include_portfolio":true}',
            )
            result = service.retry_job(job_id)
            self.assertEqual({"id": 202}, result)
            target.assert_called_once()

        with patch.object(service, "_start_job", return_value={"id": 203}) as target:
            job_id = create_job(
                self.db_path,
                job_type="scheduled_ranking_run",
                title="Scheduled ranking",
                payload_json='{"schedule_id":9,"trigger_source":"schedule","client_id":151,"lookback_days":7,"workflow_base":"examples/workflow_us_lgb_2020_port.yaml"}',
            )
            result = service.retry_job(job_id)
            self.assertEqual({"id": 203}, result)
            kwargs = target.call_args.kwargs
            self.assertEqual("scheduled_ranking_run", kwargs["job_type"])
            self.assertEqual(9, kwargs["schedule_id"])


if __name__ == "__main__":
    unittest.main()
