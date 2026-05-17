from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from .db import init_db
from .job_store import (
    create_job as create_job_record,
    get_job as get_job_record,
    list_job_log_lines as list_job_log_line_records,
    list_jobs as list_job_records,
    request_cancel_job,
)
from .model_store import ensure_default_models, get_model, list_models, resolve_or_create_model_for_workflow
from .operations_service import build_operations_summary, build_scheduled_job_request
from .portfolio_store import list_portfolio_runs, latest_portfolio_run_for_model
from .run_store import (
    backfill_legacy_run_models as backfill_legacy_run_models_record,
    create_queued_run,
    get_run as get_run_record,
    get_run_recommendations as get_run_recommendation_records,
    latest_backfill_signal_date_for_model,
    list_ranking_dates as list_ranking_date_records,
    list_runs as list_run_records,
    mark_run_failed as mark_run_failed_record,
)
from .schedule_store import (
    create_schedule as create_schedule_record,
    delete_schedule as delete_schedule_record,
    get_schedule as get_schedule_record,
    list_schedules as list_schedule_records,
    update_schedule_run_state,
    update_schedule as update_schedule_record,
)
from .settings import Settings
from .strategy_store import (
    backfill_portfolio_run_strategies,
    create_strategy as create_strategy_record,
    ensure_default_strategies,
    get_strategy as get_strategy_record,
    list_strategies as list_strategy_records,
    update_strategy as update_strategy_record,
)
from .universe_store import (
    backfill_model_universes,
    backfill_portfolio_run_universes,
    backfill_run_universes,
    create_universe as create_universe_record,
    ensure_default_universes,
    get_universe as get_universe_record,
    list_universes as list_universe_records,
    list_universe_symbols as list_universe_symbol_records,
    update_universe as update_universe_record,
)
from ..ranking.ranking_loader import read_available_trading_days


class BusyError(RuntimeError):
    """Raised when a ranking run is already in progress."""


class NotFoundError(RuntimeError):
    """Raised when an entity does not exist."""


class RankingBackendService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._scheduler = BackgroundScheduler(timezone=settings.timezone)
        init_db(settings.db_path)
        ensure_default_universes(settings.db_path, settings.project_root)
        ensure_default_strategies(settings.db_path)
        ensure_default_models(settings.db_path, settings.project_root)
        backfill_model_universes(settings.db_path)
        self._backfill_legacy_run_models()
        backfill_run_universes(settings.db_path)
        backfill_portfolio_run_universes(settings.db_path)
        backfill_portfolio_run_strategies(settings.db_path)

    def start(self) -> None:
        self._scheduler.start()
        self.reload_schedule_jobs()

    def stop(self) -> None:
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)

    def list_schedules(self) -> list[dict[str, Any]]:
        return list_schedule_records(self.settings.db_path)

    def list_universes(self) -> list[dict[str, Any]]:
        return list_universe_records(self.settings.db_path)

    def list_strategies(self) -> list[dict[str, Any]]:
        return list_strategy_records(self.settings.db_path)

    def get_strategy(self, strategy_id: int) -> dict[str, Any]:
        row = get_strategy_record(self.settings.db_path, strategy_id)
        if row is None:
            raise NotFoundError(f"Strategy {strategy_id} not found")
        return row

    def create_strategy(self, payload: dict[str, Any]) -> dict[str, Any]:
        return create_strategy_record(self.settings.db_path, payload)

    def update_strategy(self, strategy_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        row = update_strategy_record(self.settings.db_path, strategy_id, payload)
        if row is None:
            raise NotFoundError(f"Strategy {strategy_id} not found")
        return row

    def get_universe(self, universe_id: int) -> dict[str, Any]:
        row = get_universe_record(self.settings.db_path, universe_id)
        if row is None:
            raise NotFoundError(f"Universe {universe_id} not found")
        return row

    def list_universe_symbols(self, universe_id: int) -> list[dict[str, Any]]:
        row = get_universe_record(self.settings.db_path, universe_id)
        if row is None:
            raise NotFoundError(f"Universe {universe_id} not found")
        return list_universe_symbol_records(self.settings.db_path, universe_id)

    def create_universe(self, payload: dict[str, Any]) -> dict[str, Any]:
        return create_universe_record(self.settings.db_path, self.settings.project_root, payload)

    def update_universe(self, universe_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        row = update_universe_record(self.settings.db_path, self.settings.project_root, universe_id, payload)
        if row is None:
            raise NotFoundError(f"Universe {universe_id} not found")
        return row

    def create_schedule(self, payload: dict[str, Any]) -> dict[str, Any]:
        row = create_schedule_record(
            self.settings.db_path,
            payload,
            self.settings.default_workflow_base,
        )
        self.reload_schedule_jobs()
        return row

    def update_schedule(self, schedule_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        row = update_schedule_record(self.settings.db_path, schedule_id, payload)
        if row is None:
            raise NotFoundError(f"Schedule {schedule_id} not found")
        self.reload_schedule_jobs()
        return row

    def delete_schedule(self, schedule_id: int) -> None:
        deleted = delete_schedule_record(self.settings.db_path, schedule_id)
        if not deleted:
            raise NotFoundError(f"Schedule {schedule_id} not found")
        self.reload_schedule_jobs()

    def get_schedule(self, schedule_id: int) -> dict[str, Any]:
        row = get_schedule_record(self.settings.db_path, schedule_id)
        if row is None:
            raise NotFoundError(f"Schedule {schedule_id} not found")
        return row

    def list_runs(
        self,
        limit: int = 50,
        status: str | None = None,
        signal_date: str | None = None,
    ) -> list[dict[str, Any]]:
        return list_run_records(
            self.settings.db_path,
            limit=limit,
            status=status,
            signal_date=signal_date,
        )

    def list_jobs(self, limit: int = 30) -> list[dict[str, Any]]:
        return list_job_records(self.settings.db_path, limit=limit)

    def get_operations_summary(self, trade_date: str) -> dict[str, Any]:
        trade_day = dt.date.fromisoformat(trade_date)
        expected_portfolio_end = self._previous_trading_day(trade_day).isoformat()
        models = list_models(self.settings.db_path)
        portfolio_runs = list_portfolio_runs(self.settings.db_path)
        runs = self.list_runs(limit=500, status="succeeded")
        return build_operations_summary(
            models=models,
            portfolio_runs=portfolio_runs,
            runs=runs,
            trade_date=trade_date,
            expected_portfolio_end_signal_date=expected_portfolio_end,
        )

    def get_job(self, job_id: int) -> dict[str, Any]:
        item = get_job_record(self.settings.db_path, job_id)
        if item is None:
            raise NotFoundError(f"Job {job_id} not found")
        return item

    def list_job_log_lines(self, job_id: int, *, after_id: int = 0, limit: int = 500) -> dict[str, Any]:
        item = get_job_record(self.settings.db_path, job_id)
        if item is None:
            raise NotFoundError(f"Job {job_id} not found")
        return list_job_log_line_records(self.settings.db_path, job_id=job_id, after_id=after_id, limit=limit)

    def retry_job(self, job_id: int) -> dict[str, Any]:
        job = self.get_job(job_id)
        payload = json.loads(job.get("payload_json") or "{}")
        job_type = str(job["job_type"])
        if job_type == "refresh_data":
            return self.trigger_data_refresh_job(payload)
        if job_type == "backfill_ranking":
            return self.trigger_backfill_job(payload)
        if job_type == "append_portfolio":
            return self.trigger_append_portfolio_job(payload)
        if job_type == "daily_close_pipeline":
            return self.trigger_daily_close_pipeline_job(payload)
        if job_type == "scheduled_daily_close_pipeline":
            return self._enqueue_job(
                job_type=job_type,
                title=str(job["title"]),
                payload=payload,
                schedule_id=int(payload["schedule_id"]) if payload.get("schedule_id") is not None else None,
            )
        if job_type in {"manual_ranking_run", "scheduled_ranking_run"}:
            return self._enqueue_job(
                job_type=job_type,
                title=str(job["title"]),
                payload=payload,
                schedule_id=int(payload["schedule_id"]) if payload.get("schedule_id") is not None else None,
            )
        raise RuntimeError(f"Retry not supported for job_type={job_type}")

    def cancel_job(self, job_id: int, reason: str = "user requested") -> dict[str, Any]:
        item = request_cancel_job(
            self.settings.db_path,
            job_id,
            reason=reason,
            now=dt.datetime.now(dt.timezone.utc).isoformat(),
        )
        if item is None:
            raise NotFoundError(f"Job {job_id} not found")
        return item

    def list_ranking_dates(
        self,
        *,
        limit: int = 20,
        offset: int = 0,
        query: str | None = None,
        model_id: int | None = None,
    ) -> dict[str, Any]:
        return list_ranking_date_records(
            self.settings.db_path,
            limit=limit,
            offset=offset,
            query=query,
            model_id=model_id,
        )

    def get_run(self, run_id: int) -> dict[str, Any]:
        row = get_run_record(self.settings.db_path, run_id)
        if row is None:
            raise NotFoundError(f"Run {run_id} not found")
        return row

    def get_run_recommendations(self, run_id: int) -> list[dict[str, Any]]:
        return get_run_recommendation_records(self.settings.db_path, run_id)

    def trigger_manual_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._enqueue_job(
            job_type="manual_ranking_run",
            title=f"Manual ranking run ({payload['workflow_base']})",
            payload={
                "schedule_id": None,
                "trigger_source": "manual",
                "client_id": int(payload["client_id"]),
                "lookback_days": int(payload["lookback_days"]),
                "workflow_base": str(payload["workflow_base"]),
            },
        )

    def trigger_data_refresh_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._enqueue_job(
            job_type="refresh_data",
            title=f"Refresh market data from {payload['start_date']}",
            payload=payload,
        )

    def trigger_backfill_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        model = get_model(self.settings.db_path, int(payload["model_id"]))
        if model is None:
            raise NotFoundError(f"Model {payload['model_id']} not found")
        return self._enqueue_job(
            job_type="backfill_ranking",
            title=f"Backfill {model['name']} ranking for {payload['signal_date']}",
            payload=payload,
        )

    def trigger_append_portfolio_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._enqueue_job(
            job_type="append_portfolio",
            title=f"Append portfolio run #{payload['portfolio_run_id']} through {payload['end_date']}",
            payload=payload,
        )

    def trigger_daily_close_pipeline_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._enqueue_job(
            job_type="daily_close_pipeline",
            title=f"Daily close pipeline for {payload['trade_date']}",
            payload=payload,
        )

    def reload_schedule_jobs(self) -> None:
        for job in self._scheduler.get_jobs():
            self._scheduler.remove_job(job.id)
        for schedule in self.list_schedules():
            if not schedule["enabled"]:
                continue
            trigger = CronTrigger(
                day_of_week=str(schedule["day_of_week"]),
                hour=int(schedule["hour"]),
                minute=int(schedule["minute"]),
                timezone=str(schedule["timezone"]),
            )
            self._scheduler.add_job(
                self._trigger_scheduled_run,
                trigger=trigger,
                args=[int(schedule["id"])],
                id=f"schedule-{schedule['id']}",
                replace_existing=True,
            )

    def _trigger_scheduled_run(self, schedule_id: int) -> None:
        schedule = self.get_schedule(schedule_id)
        request = build_scheduled_job_request(
            schedule,
            now_local_date=dt.datetime.now(ZoneInfo(str(schedule["timezone"]))).date(),
        )
        self._enqueue_job(
            job_type=str(request["job_type"]),
            title=str(request["title"]),
            payload=dict(request["payload"]),
            schedule_id=int(request["schedule_id"]),
        )

    def _enqueue_job(
        self,
        *,
        job_type: str,
        title: str,
        payload: dict[str, Any],
        schedule_id: int | None = None,
    ) -> dict[str, Any]:
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        job_id = create_job_record(
            self.settings.db_path,
            job_type=job_type,
            title=title,
            payload_json=json.dumps(payload, ensure_ascii=True, sort_keys=True),
            requested_by="web",
            created_at=now,
        )
        if schedule_id is not None:
            update_schedule_run_state(
                self.settings.db_path,
                schedule_id,
                updated_at=now,
                last_triggered_at=now,
                last_run_status="queued",
            )
        return self.get_job(job_id)

    def _create_run_record(
        self,
        *,
        schedule_id: int | None,
        trigger_source: str,
        client_id: int,
        lookback_days: int,
        workflow_base: str,
    ) -> int:
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        model_id = resolve_or_create_model_for_workflow(
            self.settings.db_path,
            self.settings.project_root,
            workflow_base,
        )
        command = [
            str(self.settings.run_script_path),
            "--client-id",
            str(client_id),
            "--lookback-days",
            str(lookback_days),
            "--workflow-base",
            workflow_base,
        ]
        run_id = create_queued_run(
            db_path=self.settings.db_path,
            schedule_id=schedule_id,
            model_id=model_id,
            trigger_source=trigger_source,
            client_id=client_id,
            lookback_days=lookback_days,
            workflow_base=workflow_base,
            command=" ".join(command),
            created_at=now,
        )
        if schedule_id is not None:
            update_schedule_run_state(
                self.settings.db_path,
                schedule_id,
                updated_at=now,
                last_triggered_at=now,
                last_run_id=run_id,
                last_run_status="queued",
            )
        return run_id

    def _backfill_legacy_run_models(self) -> None:
        lgb_model_id = resolve_or_create_model_for_workflow(
            self.settings.db_path,
            self.settings.project_root,
            "examples/workflow_us_lgb_2020_port.yaml",
        )
        backfill_legacy_run_models_record(
            self.settings.db_path,
            default_model_id=lgb_model_id,
        )

    def _build_refresh_command(
        self,
        *,
        market: str,
        client_id: int,
        start_date: str,
        config_path: str = "config.yaml",
    ) -> list[str]:
        if market == "cn":
            return self._build_refresh_cn_command(
                start_date=start_date,
                config_path=config_path,
            )
        return self._build_refresh_data_command(
            client_id=client_id,
            start_date=start_date,
            config_path=config_path,
        )

    def _build_refresh_data_command(
        self,
        *,
        client_id: int,
        start_date: str,
        config_path: str = "config.yaml",
    ) -> list[str]:
        python_bin = self.settings.project_root / ".venv" / "bin" / "python"
        return [
            str(python_bin),
            "run.py",
            "--config",
            config_path,
            "--client-id",
            str(client_id),
            "--start-date",
            start_date,
            "--bar-size",
            "1 day",
            "--no-news",
            "--dump-bin",
        ]

    def _build_refresh_cn_command(
        self,
        *,
        start_date: str,
        config_path: str = "config_cn.yaml",
    ) -> list[str]:
        python_bin = self.settings.project_root / ".venv" / "bin" / "python"
        return [
            str(python_bin),
            "scripts/refresh_cn_daily_baostock.py",
            "--config",
            config_path,
            "--start-date",
            start_date,
        ]

    def _build_backfill_command(
        self,
        *,
        signal_date: str,
        workflow_base: str,
        model_id: int,
        client_id: int,
        config_path: str = "config.yaml",
    ) -> list[str]:
        return self._build_bulk_backfill_command(
            start_date=signal_date,
            end_date=signal_date,
            workflow_base=workflow_base,
            model_id=model_id,
            client_id=client_id,
            config_path=config_path,
        )

    def _build_bulk_backfill_command(
        self,
        *,
        start_date: str,
        end_date: str,
        workflow_base: str,
        model_id: int,
        client_id: int,
        config_path: str = "config.yaml",
    ) -> list[str]:
        python_bin = self.settings.project_root / ".venv" / "bin" / "python"
        return [
            str(python_bin),
            "backfill_rankings_bulk.py",
            "--config",
            config_path,
            "--start-date",
            start_date,
            "--end-date",
            end_date,
            "--workflow-base",
            workflow_base,
            "--model-id",
            str(model_id),
            "--client-id",
            str(client_id),
            "--html-mode",
            "cached",
            "--skip-existing-db",
            "--manifest-dir",
            "reports/manifests",
        ]

    def _config_path_for_model(self, model: dict[str, Any]) -> str:
        details = model.get("details") or {}
        model_config_path = details.get("config_path")
        if isinstance(model_config_path, str) and model_config_path.strip():
            return model_config_path.strip()
        universe_id = model.get("universe_id")
        if universe_id is not None:
            universe = get_universe_record(self.settings.db_path, int(universe_id))
            if universe is not None:
                details = universe.get("details") or {}
                config_path = details.get("config_path")
                if isinstance(config_path, str) and config_path.strip():
                    return config_path.strip()
        if str(model.get("universe_key") or "").strip() == "us_union_sp500_ndx_djia_sox":
            return "config_union.yaml"
        return "config.yaml"

    def _market_for_model(self, model: dict[str, Any]) -> str:
        details = model.get("details") or {}
        market = details.get("market")
        if isinstance(market, str) and market.strip():
            return market.strip().lower()
        universe_id = model.get("universe_id")
        if universe_id is not None:
            universe = get_universe_record(self.settings.db_path, int(universe_id))
            if universe is not None:
                details = universe.get("details") or {}
                market = details.get("market")
                if isinstance(market, str) and market.strip():
                    return market.strip().lower()
        return self._market_for_config_path(self._config_path_for_model(model))

    def _market_for_config_path(self, config_path: str) -> str:
        stem = Path(config_path).stem.lower()
        if stem.endswith("_cn") or stem.startswith("config_cn") or "_cn" in stem:
            return "cn"
        return "us"

    def _build_append_portfolio_command(
        self,
        *,
        portfolio_run_id: int,
        model_id: int | None,
        end_date: str,
    ) -> list[str]:
        python_bin = self.settings.project_root / ".venv" / "bin" / "python"
        command = [
            str(python_bin),
            "simulate_portfolio.py",
            "--append-run-id",
            str(portfolio_run_id),
            "--end-date",
            end_date,
        ]
        if model_id is not None:
            command.extend(["--model-id", str(model_id)])
        return command

    def _previous_trading_day(self, trade_date: dt.date, config_path: str = "config.yaml") -> dt.date:
        trading_days = read_available_trading_days(
            self.settings.project_root,
            config_path=(self.settings.project_root / config_path).resolve(),
        )
        prior_days = [day for day in trading_days if day < trade_date]
        if not prior_days:
            raise RuntimeError(f"No prior trading day found before {trade_date.isoformat()}")
        return prior_days[-1]

    def _latest_backfill_signal_date_for_model(self, model_id: int) -> dt.date | None:
        return latest_backfill_signal_date_for_model(self.settings.db_path, model_id)

    def _missing_signal_days_for_model(
        self,
        *,
        model_id: int,
        trade_date: dt.date,
        trading_days: list[dt.date],
        fallback_start_date: dt.date,
    ) -> list[dt.date]:
        latest_signal = self._latest_backfill_signal_date_for_model(model_id)
        start_day = fallback_start_date if latest_signal is None else latest_signal + dt.timedelta(days=1)
        return [day for day in trading_days if start_day <= day <= trade_date]

    def _latest_portfolio_run_for_model(self, model_id: int) -> dict[str, Any] | None:
        return latest_portfolio_run_for_model(self.settings.db_path, model_id)

    def _mark_run_failed(self, run_id: int, schedule_id: int | None, error_text: str) -> None:
        finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
        mark_run_failed_record(
            self.settings.db_path,
            run_id,
            finished_at=finished_at,
            error_text=error_text,
        )
        if schedule_id is not None:
            update_schedule_run_state(
                self.settings.db_path,
                schedule_id,
                updated_at=finished_at,
                last_run_status="failed",
            )
