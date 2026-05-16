from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from ib_qlib_pipeline.dborm.models import Run
from ib_qlib_pipeline.webapi.app import create_app
from ib_qlib_pipeline.dborm.session import create_session_factory_for_path
from ib_qlib_pipeline.webapi.db import init_db
from ib_qlib_pipeline.webapi.job_store import (
    append_job_log,
    append_job_step_output,
    create_job,
    create_job_step,
    get_job,
    list_job_log_lines,
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
from ib_qlib_pipeline.webapi.schemas import StrategyCreate, StrategyUpdate, UniverseCreate, UniverseUpdate
from ib_qlib_pipeline.webapi.service import RankingBackendService
from ib_qlib_pipeline.webapi.settings import Settings
from ib_qlib_pipeline.webapi.strategy_store import (
    create_strategy,
    ensure_default_strategies,
    get_strategy,
    get_strategy_by_key,
    list_strategies,
    update_strategy,
)
from ib_qlib_pipeline.webapi.universe_store import (
    create_universe,
    ensure_default_universes,
    get_universe,
    get_universe_by_key,
    list_universe_symbols,
    list_universes,
    update_universe,
)


class StoreTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmp.name) / "test.db"
        init_db(self.db_path)
        self.project_root = Path(__file__).resolve().parent.parent
        ensure_default_universes(self.db_path, self.project_root)
        ensure_default_strategies(self.db_path)
        ensure_default_models(self.db_path, self.project_root)

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

    def _make_app(self):
        settings = Settings(
            project_root=self.project_root,
            db_path=self.db_path,
            timezone="America/Denver",
            default_workflow_base="examples/workflow_us_lgb_2020_port.yaml",
            run_script_path=self.project_root / "run_daily_ranking.sh",
            api_host="127.0.0.1",
            api_port=8001,
        )
        with patch("ib_qlib_pipeline.webapi.app.Settings.load", return_value=settings):
            app = create_app()
        app.state.settings = settings
        app.state.service = RankingBackendService(settings)
        return app

    def test_universe_store_defaults_and_model_binding(self) -> None:
        universes = list_universes(self.db_path)
        self.assertEqual(4, len(universes))
        self.assertEqual("sp500", universes[0]["key"])
        self.assertEqual(503, universes[0]["symbol_count"])
        self.assertEqual("us_union_sp500_ndx_djia_sox", universes[1]["key"])
        self.assertEqual(524, universes[1]["symbol_count"])
        self.assertEqual("cn_a_share", universes[2]["key"])
        self.assertEqual(0, universes[2]["symbol_count"])
        self.assertEqual("cn_csi800", universes[3]["key"])
        self.assertEqual(800, universes[3]["symbol_count"])

        sp500 = get_universe_by_key(self.db_path, "sp500")
        model = get_model_by_key(self.db_path, "lgb")
        self.assertIsNotNone(sp500)
        self.assertEqual(sp500["id"], model["universe_id"])
        self.assertEqual("S&P 500", model["universe_name"])

        sp500_symbols = list_universe_symbols(self.db_path, int(sp500["id"]))
        self.assertEqual(503, len(sp500_symbols))
        self.assertEqual("MMM", sp500_symbols[0]["symbol"])
        self.assertEqual("MMM", sp500_symbols[0]["ib_symbol"])
        self.assertEqual(1, sp500_symbols[0]["sort_order"])

        union = get_universe_by_key(self.db_path, "us_union_sp500_ndx_djia_sox")
        self.assertIsNotNone(union)
        union_symbols = list_universe_symbols(self.db_path, int(union["id"]))
        self.assertEqual(524, len(union_symbols))
        self.assertEqual("MMM", union_symbols[0]["symbol"])
        self.assertEqual("WOLF", union_symbols[-1]["symbol"])

        cn = get_universe_by_key(self.db_path, "cn_a_share")
        self.assertIsNotNone(cn)
        cn_symbols = list_universe_symbols(self.db_path, int(cn["id"]))
        self.assertEqual([], cn_symbols)

    def test_universe_store_create_and_update(self) -> None:
        created = create_universe(
            self.db_path,
            self.project_root,
            {
                "key": "sp500_top20",
                "name": "SP500 Top20",
                "symbols_file": "symbols/sp500_120.txt",
                "description": "Pilot subset",
                "details": {"purpose": "pilot"},
            },
        )
        self.assertEqual(120, created["symbol_count"])

        updated = update_universe(
            self.db_path,
            self.project_root,
            created["id"],
            {
                "name": "SP500 Top20 Updated",
                "description": "Pilot subset updated",
                "details": {"purpose": "pilot", "version": 2},
            },
        )
        fetched = get_universe(self.db_path, created["id"])
        self.assertEqual("SP500 Top20 Updated", updated["name"])
        self.assertEqual("Pilot subset updated", fetched["description"])
        self.assertEqual(2, fetched["details"]["version"])

    def test_universe_api_list_create_update(self) -> None:
        app = self._make_app()
        route_map = {
            (route.path, tuple(sorted(getattr(route, "methods", [])))): route.endpoint
            for route in app.routes
            if hasattr(route, "endpoint")
        }
        list_endpoint = route_map[("/api/universes", ("GET",))]
        create_endpoint = route_map[("/api/universes", ("POST",))]
        update_endpoint = route_map[("/api/universes/{universe_id}", ("PATCH",))]
        list_symbols_endpoint = route_map[("/api/universes/{universe_id}/symbols", ("GET",))]

        listed = list_endpoint()
        self.assertEqual(4, len(listed))

        created_body = create_endpoint(
            UniverseCreate(
                key="sp500_top20_api",
                name="SP500 Top20 API",
                symbols_file="symbols/sp500_120.txt",
                description="API-created universe",
                details={"source": "test"},
            )
        )
        self.assertEqual(120, created_body["symbol_count"])

        updated_body = update_endpoint(
            int(created_body["id"]),
            UniverseUpdate(name="SP500 Top20 API v2"),
        )
        self.assertEqual("SP500 Top20 API v2", updated_body["name"])

        symbols = list_symbols_endpoint(int(created_body["id"]))
        self.assertEqual(120, len(symbols))
        self.assertEqual("MMM", symbols[0]["symbol"])

    def test_model_store_default_models_and_lookup(self) -> None:
        models = list_models(self.db_path)
        self.assertEqual(19, len(models))
        self.assertEqual("LightGBM_Default", get_model_by_key(self.db_path, "lgb")["name"])
        self.assertEqual("XGBoost_5D", get_model_by_key(self.db_path, "xgb_5d")["name"])
        self.assertEqual(
            "US Union 524",
            get_model_by_key(self.db_path, "lgb_union")["universe_name"],
        )
        self.assertEqual(
            "CatBoost_5D_Union",
            get_model_by_key(self.db_path, "catboost_5d_union")["name"],
        )
        cn_model = get_model_by_key(self.db_path, "cat_cn1")
        cn_universe = get_universe_by_key(self.db_path, "cn_csi800")
        self.assertEqual(cn_universe["id"], cn_model["universe_id"])
        self.assertEqual("CAT1_U16", get_model_by_key(self.db_path, "cat1_u16")["name"])
        self.assertEqual(
            "US Union 524",
            get_model_by_key(self.db_path, "xgb5_u16")["universe_name"],
        )

        model_id = resolve_or_create_model_for_workflow(
            self.db_path,
            self.project_root,
            "examples/workflow_us_catboost_2020_port_next_open_5d.yaml",
        )
        self.assertEqual("CatBoost_5D", get_model(self.db_path, model_id)["name"])

    def test_strategy_store_default_and_crud(self) -> None:
        defaults = list_strategies(self.db_path)
        self.assertEqual(1, len(defaults))
        self.assertEqual("top10_hold20_default", defaults[0]["key"])
        self.assertEqual(10, defaults[0]["buy_top_n"])
        self.assertEqual(20, defaults[0]["hold_top_n"])
        self.assertEqual(10000.0, defaults[0]["target_notional"])
        self.assertTrue(defaults[0]["allow_reentry"])

        created = create_strategy(
            self.db_path,
            {
                "key": "top15_hold30_risk_caps",
                "name": "Top15 Hold30 Risk Caps",
                "strategy_type": "ranking_hold_band",
                "description": "Risk-capped variant",
                "buy_top_n": 15,
                "hold_top_n": 30,
                "target_notional": 8000.0,
                "initial_capital": 250000.0,
                "max_position_notional": 12000.0,
                "max_position_pct": 4.5,
                "fee_bps": 5.0,
                "slippage_bps": 8.0,
                "gap_up_limit_pct": 6.0,
                "gap_down_limit_pct": -4.0,
                "allow_reentry": None,
                "config": {"cash_buffer_pct": 2.0},
                "details": {"note": "optional fields allowed"},
            },
        )
        self.assertIsNone(created["universe_id"])
        self.assertEqual(4.5, created["max_position_pct"])
        self.assertIsNone(created["allow_reentry"])
        self.assertEqual(2.0, created["config"]["cash_buffer_pct"])

        updated = update_strategy(
            self.db_path,
            int(created["id"]),
            {
                "description": "Updated",
                "max_position_pct": 5.0,
                "max_position_notional": None,
                "allow_reentry": False,
                "config": {"cash_buffer_pct": 1.0, "volatility_cap": 0.25},
            },
        )
        fetched = get_strategy(self.db_path, int(created["id"]))
        self.assertEqual("Updated", updated["description"])
        self.assertIsNone(fetched["max_position_notional"])
        self.assertFalse(fetched["allow_reentry"])
        self.assertEqual(0.25, fetched["config"]["volatility_cap"])
        self.assertEqual("Top15 Hold30 Risk Caps", get_strategy_by_key(self.db_path, "top15_hold30_risk_caps")["name"])

    def test_strategy_api_list_create_update(self) -> None:
        app = self._make_app()
        route_map = {
            (route.path, tuple(sorted(getattr(route, "methods", [])))): route.endpoint
            for route in app.routes
            if hasattr(route, "endpoint")
        }
        list_endpoint = route_map[("/api/strategies", ("GET",))]
        get_endpoint = route_map[("/api/strategies/{strategy_id}", ("GET",))]
        create_endpoint = route_map[("/api/strategies", ("POST",))]
        update_endpoint = route_map[("/api/strategies/{strategy_id}", ("PATCH",))]

        listed = list_endpoint()
        self.assertEqual(1, len(listed))

        created = create_endpoint(
            StrategyCreate(
                key="api_strategy",
                name="API Strategy",
                strategy_type="ranking_hold_band",
                buy_top_n=12,
                hold_top_n=24,
                max_position_pct=3.5,
                details={"source": "test"},
            )
        )
        fetched = get_endpoint(int(created["id"]))
        self.assertEqual("api_strategy", fetched["key"])
        self.assertEqual(3.5, fetched["max_position_pct"])

        updated = update_endpoint(
            int(created["id"]),
            StrategyUpdate(
                description="updated via api",
                max_position_notional=15000.0,
                config={"cash_buffer_pct": 1.5},
            ),
        )
        self.assertEqual(15000.0, updated["max_position_notional"])
        self.assertEqual(1.5, updated["config"]["cash_buffer_pct"])

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
        strategy = get_strategy_by_key(self.db_path, "top10_hold20_default")
        portfolio_run_id = create_portfolio_run(
            db_path=self.db_path,
            name="test-run",
            strategy_id=int(strategy["id"]),
            strategy="top10-hold20",
            strategy_config={"strategy_key": strategy["key"], "buy_top_n": 10, "hold_top_n": 20},
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
        self.assertEqual(strategy["id"], portfolio_run["strategy_id"])
        self.assertEqual("top10_hold20_default", portfolio_run["strategy_key"])
        self.assertEqual(10, portfolio_run["strategy_config"]["buy_top_n"])
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
        log_lines = list_job_log_lines(self.db_path, job_id=job_id)
        self.assertEqual(1, len(jobs))
        self.assertEqual("succeeded", job["status"])
        self.assertEqual(1, len(job["steps"]))
        self.assertEqual("succeeded", job["steps"][0]["status"])
        self.assertIn("[refresh_data]", job["log_output"])
        self.assertEqual(5, len(log_lines))
        self.assertEqual("line1", log_lines[0]["content"])
        self.assertEqual("[refresh_data]", log_lines[2]["content"])

    def test_service_command_builders(self) -> None:
        service = self._make_service()

        refresh = service._build_refresh_data_command(client_id=151, start_date="2026-05-01")
        refresh_cn = service._build_refresh_command(
            market="cn",
            client_id=151,
            start_date="2026-05-01",
            config_path="config_cn.yaml",
        )
        backfill = service._build_backfill_command(
            signal_date="2026-05-06",
            workflow_base="examples/workflow_us_xgb_2020_port.yaml",
            model_id=2,
            client_id=151,
        )
        bulk_backfill = service._build_bulk_backfill_command(
            start_date="2026-05-01",
            end_date="2026-05-06",
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
        self.assertIn("config.yaml", refresh)
        self.assertIn("--dump-bin", refresh)
        self.assertEqual("scripts/refresh_cn_daily_baostock.py", refresh_cn[1])
        self.assertIn("config_cn.yaml", refresh_cn)
        self.assertEqual("backfill_rankings_bulk.py", backfill[1])
        self.assertIn("config.yaml", backfill)
        self.assertIn("--skip-existing-db", backfill)
        self.assertIn("--model-id", backfill)
        self.assertNotIn("--skip-existing-files", backfill)
        self.assertEqual("backfill_rankings_bulk.py", bulk_backfill[1])
        self.assertIn("--skip-existing-db", bulk_backfill)
        self.assertNotIn("--skip-existing-files", bulk_backfill)
        self.assertEqual("2026-05-06", backfill[backfill.index("--start-date") + 1])
        self.assertEqual("2026-05-06", backfill[backfill.index("--end-date") + 1])
        self.assertEqual("2026-05-01", bulk_backfill[bulk_backfill.index("--start-date") + 1])
        self.assertEqual("2026-05-06", bulk_backfill[bulk_backfill.index("--end-date") + 1])
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
        strategy = get_strategy_by_key(self.db_path, "top10_hold20_default")
        portfolio_run_id = create_portfolio_run(
            db_path=self.db_path,
            name="lgb-run",
            strategy_id=int(strategy["id"]),
            strategy="top10-hold20",
            strategy_config={"strategy_key": strategy["key"]},
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
        strategy = get_strategy_by_key(self.db_path, "top10_hold20_default")
        create_portfolio_run(
            db_path=self.db_path,
            name="lgb-run-latest",
            strategy_id=int(strategy["id"]),
            strategy="top10-hold20",
            strategy_config={"strategy_key": strategy["key"]},
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
            strategy_id=int(strategy["id"]),
            strategy="top10-hold20",
            strategy_config={"strategy_key": strategy["key"]},
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
        import sqlite3

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

        bulk_run_id = self._insert_completed_run(signal_date="2026-05-07", model_id=1)
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("update runs set trigger_source = ? where id = ?", ("bulk_backfill", bulk_run_id))
            conn.commit()
        finally:
            conn.close()

        with_bulk = service.list_ranking_dates(limit=10, offset=0, model_id=1)
        self.assertEqual(3, with_bulk["total"])
        self.assertEqual("2026-05-07", with_bulk["items"][0]["signal_date"])

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

    def test_service_reload_schedule_jobs_only_registers_enabled(self) -> None:
        service = self._make_service()
        enabled = create_schedule(
            self.db_path,
            {
                "name": "close",
                "schedule_type": "daily_close_pipeline",
                "enabled": True,
                "timezone": "America/Denver",
                "day_of_week": "mon-fri",
                "hour": 14,
                "minute": 40,
                "client_id": 151,
                "lookback_days": 7,
                "workflow_base": "examples/workflow_us_lgb_2020_port.yaml",
                "pipeline_start_date": "2026-05-01",
                "pipeline_include_portfolio": True,
            },
            "examples/workflow_us_lgb_2020_port.yaml",
        )
        create_schedule(
            self.db_path,
            {
                "name": "disabled",
                "schedule_type": "ranking",
                "enabled": False,
                "timezone": "America/Denver",
                "day_of_week": "mon-fri",
                "hour": 15,
                "minute": 10,
                "client_id": 151,
                "lookback_days": 7,
                "workflow_base": "examples/workflow_us_lgb_2020_port.yaml",
                "pipeline_start_date": None,
                "pipeline_include_portfolio": True,
            },
            "examples/workflow_us_lgb_2020_port.yaml",
        )

        fake_scheduler = SimpleNamespace(
            get_jobs=lambda: [SimpleNamespace(id="old-job")],
            remove_job=unittest.mock.Mock(),
            add_job=unittest.mock.Mock(),
        )
        service._scheduler = fake_scheduler

        service.reload_schedule_jobs()

        fake_scheduler.remove_job.assert_called_once_with("old-job")
        fake_scheduler.add_job.assert_called_once()
        kwargs = fake_scheduler.add_job.call_args.kwargs
        self.assertEqual(f"schedule-{enabled['id']}", kwargs["id"])
        self.assertEqual([enabled["id"]], kwargs["args"])

    def test_service_trigger_scheduled_run_daily_close_builds_expected_job(self) -> None:
        service = self._make_service()
        schedule = create_schedule(
            self.db_path,
            {
                "name": "close",
                "schedule_type": "daily_close_pipeline",
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

        fake_now = dt.datetime(2026, 5, 7, 14, 40, tzinfo=dt.timezone.utc)
        with (
            patch("ib_qlib_pipeline.webapi.service.dt.datetime") as mock_datetime,
            patch.object(service, "_start_job", return_value={"id": 301}) as start_job,
        ):
            mock_datetime.now.return_value = fake_now
            service._trigger_scheduled_run(schedule["id"])

        kwargs = start_job.call_args.kwargs
        self.assertEqual("scheduled_daily_close_pipeline", kwargs["job_type"])
        self.assertEqual(schedule["id"], kwargs["schedule_id"])
        self.assertEqual(schedule["id"], kwargs["payload"]["schedule_id"])
        self.assertEqual(True, kwargs["payload"]["include_portfolio"])

    def test_service_trigger_scheduled_run_ranking_builds_expected_job(self) -> None:
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

        with patch.object(service, "_start_job", return_value={"id": 302}) as start_job:
            service._trigger_scheduled_run(schedule["id"])

        kwargs = start_job.call_args.kwargs
        self.assertEqual("scheduled_ranking_run", kwargs["job_type"])
        self.assertEqual(schedule["id"], kwargs["schedule_id"])
        self.assertEqual("schedule", kwargs["payload"]["trigger_source"])
        self.assertEqual(schedule["workflow_base"], kwargs["payload"]["workflow_base"])

    def test_service_execute_daily_close_pipeline_job_orchestrates_models(self) -> None:
        service = self._make_service()
        models = [
            {"id": 1, "key": "lgb", "name": "LightGBM_Default", "workflow_base": "examples/workflow_us_lgb_2020_port.yaml", "universe_id": 1, "universe_key": "sp500"},
            {"id": 2, "key": "xgb_union", "name": "XGBoost_Default_Union", "workflow_base": "examples/workflow_us_xgb_2020_port.yaml", "universe_id": 2, "universe_key": "us_union_sp500_ndx_djia_sox"},
            {"id": 185, "key": "cat_cn1", "name": "CAT_CN1", "workflow_base": "examples/wf_cat_cn1.yaml", "universe_id": 4, "universe_key": "cn_csi800", "details": {"config_path": "config_cn.yaml"}},
        ]
        payload = {
            "trade_date": "2026-05-06",
            "client_id": 151,
            "start_date": "2026-05-01",
            "include_portfolio": True,
        }

        with (
            patch("ib_qlib_pipeline.webapi.service.list_models", return_value=models),
            patch("ib_qlib_pipeline.webapi.service.read_available_trading_days", side_effect=[
                [dt.date(2026, 5, 5), dt.date(2026, 5, 6)],
                [dt.date(2026, 5, 5), dt.date(2026, 5, 6)],
                [dt.date(2026, 5, 5), dt.date(2026, 5, 6)],
                [dt.date(2026, 5, 5), dt.date(2026, 5, 6)],
                [dt.date(2026, 5, 5), dt.date(2026, 5, 6)],
                [dt.date(2026, 5, 5), dt.date(2026, 5, 6)],
            ]),
            patch.object(service, "_missing_signal_days_for_model", side_effect=[[dt.date(2026, 5, 5)], [dt.date(2026, 5, 5), dt.date(2026, 5, 6)], [dt.date(2026, 5, 6)]]),
            patch.object(service, "_latest_portfolio_run_for_model", side_effect=[{"id": 11, "model_id": 1}, None, {"id": 22, "model_id": 185}]),
            patch.object(service, "_run_job_step", return_value="ok") as run_job_step,
            patch.object(service, "_record_job_note") as record_note,
        ):
            service._execute_daily_close_pipeline_job(901, payload)

        called_steps = [call.args[1] for call in run_job_step.call_args_list]
        self.assertEqual(
            [
                "refresh_data_config",
                "refresh_data_config_union",
                "refresh_cn_config_cn",
                "backfill_lgb_2026-05-05_to_2026-05-05",
                "backfill_xgb_union_2026-05-05_to_2026-05-06",
                "backfill_cat_cn1_2026-05-06_to_2026-05-06",
                "append_lgb_portfolio",
                "append_cat_cn1_portfolio",
            ],
            called_steps,
        )
        record_note.assert_called_once()
        self.assertIn("append_xgb_union_portfolio", record_note.call_args.kwargs["step_name"])

    def test_service_execute_daily_close_pipeline_job_skips_portfolio_when_disabled(self) -> None:
        service = self._make_service()
        models = [
            {"id": 1, "key": "lgb", "name": "LightGBM_Default", "workflow_base": "examples/workflow_us_lgb_2020_port.yaml", "universe_id": 1, "universe_key": "sp500"},
        ]
        payload = {
            "trade_date": "2026-05-06",
            "client_id": 151,
            "start_date": "2026-05-01",
            "include_portfolio": False,
        }

        with (
            patch("ib_qlib_pipeline.webapi.service.list_models", return_value=models),
            patch("ib_qlib_pipeline.webapi.service.read_available_trading_days", return_value=[dt.date(2026, 5, 6)]),
            patch.object(service, "_missing_signal_days_for_model", return_value=[dt.date(2026, 5, 6)]),
            patch.object(service, "_run_job_step", return_value="ok") as run_job_step,
            patch.object(service, "_latest_portfolio_run_for_model") as latest_portfolio,
        ):
            service._execute_daily_close_pipeline_job(902, payload)

        called_steps = [call.args[1] for call in run_job_step.call_args_list]
        self.assertEqual(["refresh_data_config", "backfill_lgb_2026-05-06_to_2026-05-06"], called_steps)
        latest_portfolio.assert_not_called()

    def test_service_config_path_for_model_uses_universe_details(self) -> None:
        service = self._make_service()
        union_model = get_model_by_key(self.db_path, "lgb_union")
        self.assertIsNotNone(union_model)
        self.assertEqual("config_union.yaml", service._config_path_for_model(union_model))
        sp500_model = get_model_by_key(self.db_path, "lgb")
        self.assertIsNotNone(sp500_model)
        self.assertEqual("config.yaml", service._config_path_for_model(sp500_model))


if __name__ == "__main__":
    unittest.main()
