from __future__ import annotations

import datetime as dt
import json
import os
import re
import subprocess
import threading
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from .db import init_db
from .job_store import (
    append_job_log,
    append_job_step_output,
    create_job as create_job_record,
    create_job_step,
    get_job as get_job_record,
    list_jobs as list_job_records,
    mark_job_finished,
    mark_job_running,
    next_job_step_order,
    update_job_step,
)
from .model_store import ensure_default_models, get_model, list_models, resolve_or_create_model_for_workflow
from .operations_service import build_operations_summary, build_scheduled_job_request
from .portfolio_store import list_portfolio_runs, latest_portfolio_run_for_model
from .run_store import (
    backfill_legacy_run_models as backfill_legacy_run_models_record,
    create_queued_run,
    finalize_succeeded_run,
    get_run as get_run_record,
    get_run_recommendations as get_run_recommendation_records,
    latest_backfill_signal_date_for_model,
    list_ranking_dates as list_ranking_date_records,
    list_runs as list_run_records,
    mark_run_running,
    mark_run_failed as mark_run_failed_record,
    ranking_df_to_rows,
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
        self._run_lock = threading.Lock()
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
            return self._start_job(
                job_type=job_type,
                title=str(job["title"]),
                payload=payload,
                target=self._execute_daily_close_pipeline_job,
                schedule_id=int(payload["schedule_id"]) if payload.get("schedule_id") is not None else None,
            )
        if job_type in {"manual_ranking_run", "scheduled_ranking_run"}:
            return self._start_job(
                job_type=job_type,
                title=str(job["title"]),
                payload=payload,
                target=self._execute_ranking_run_job,
                schedule_id=int(payload["schedule_id"]) if payload.get("schedule_id") is not None else None,
            )
        raise RuntimeError(f"Retry not supported for job_type={job_type}")

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
        return self._start_job(
            job_type="manual_ranking_run",
            title=f"Manual ranking run ({payload['workflow_base']})",
            payload={
                "schedule_id": None,
                "trigger_source": "manual",
                "client_id": int(payload["client_id"]),
                "lookback_days": int(payload["lookback_days"]),
                "workflow_base": str(payload["workflow_base"]),
            },
            target=self._execute_ranking_run_job,
        )

    def trigger_data_refresh_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._start_job(
            job_type="refresh_data",
            title=f"Refresh market data from {payload['start_date']}",
            payload=payload,
            target=self._execute_refresh_data_job,
        )

    def trigger_backfill_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        model = get_model(self.settings.db_path, int(payload["model_id"]))
        if model is None:
            raise NotFoundError(f"Model {payload['model_id']} not found")
        return self._start_job(
            job_type="backfill_ranking",
            title=f"Backfill {model['name']} ranking for {payload['signal_date']}",
            payload=payload,
            target=self._execute_backfill_job,
        )

    def trigger_append_portfolio_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._start_job(
            job_type="append_portfolio",
            title=f"Append portfolio run #{payload['portfolio_run_id']} through {payload['end_date']}",
            payload=payload,
            target=self._execute_append_portfolio_job,
        )

    def trigger_daily_close_pipeline_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._start_job(
            job_type="daily_close_pipeline",
            title=f"Daily close pipeline for {payload['trade_date']}",
            payload=payload,
            target=self._execute_daily_close_pipeline_job,
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
        target = (
            self._execute_daily_close_pipeline_job
            if request["target_name"] == "daily_close_pipeline"
            else self._execute_ranking_run_job
        )
        self._start_job(
            job_type=str(request["job_type"]),
            title=str(request["title"]),
            payload=dict(request["payload"]),
            target=target,
            schedule_id=int(request["schedule_id"]),
        )

    def _start_job(
        self,
        *,
        job_type: str,
        title: str,
        payload: dict[str, Any],
        target: Any,
        schedule_id: int | None = None,
    ) -> dict[str, Any]:
        if not self._run_lock.acquire(blocking=False):
            raise BusyError("Another backend run or job is already in progress")
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        try:
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
            thread = threading.Thread(
                target=self._execute_job_thread,
                args=(job_id, target, payload, schedule_id),
                daemon=True,
            )
            thread.start()
            return self.get_job(job_id)
        except Exception:
            self._run_lock.release()
            raise

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

    def _execute_job_thread(self, job_id: int, target: Any, payload: dict[str, Any], schedule_id: int | None) -> None:
        started_at = dt.datetime.now(dt.timezone.utc).isoformat()
        try:
            mark_job_running(self.settings.db_path, job_id, started_at)
            if schedule_id is not None:
                update_schedule_run_state(
                    self.settings.db_path,
                    schedule_id,
                    updated_at=started_at,
                    last_run_status="running",
                )
            target(job_id, payload)
            finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
            mark_job_finished(self.settings.db_path, job_id, status="succeeded", finished_at=finished_at, error_text=None)
            if schedule_id is not None:
                update_schedule_run_state(
                    self.settings.db_path,
                    schedule_id,
                    updated_at=finished_at,
                    last_run_status="succeeded",
                )
        except Exception as exc:  # noqa: BLE001
            finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
            mark_job_finished(self.settings.db_path, job_id, status="failed", finished_at=finished_at, error_text=str(exc))
            if schedule_id is not None:
                update_schedule_run_state(
                    self.settings.db_path,
                    schedule_id,
                    updated_at=finished_at,
                    last_run_status="failed",
                )
        finally:
            self._run_lock.release()

    def _execute_ranking_run_job(self, job_id: int, payload: dict[str, Any]) -> None:
        schedule_id = int(payload["schedule_id"]) if payload.get("schedule_id") is not None else None
        trigger_source = str(payload["trigger_source"])
        client_id = int(payload["client_id"])
        lookback_days = int(payload["lookback_days"])
        workflow_base = str(payload["workflow_base"])
        run_id = self._create_run_record(
            schedule_id=schedule_id,
            trigger_source=trigger_source,
            client_id=client_id,
            lookback_days=lookback_days,
            workflow_base=workflow_base,
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
        combined_output = self._run_job_step(job_id, "ranking_run", command)
        self._finalize_run_from_output(run_id, schedule_id, combined_output)

    def _execute_run(self, run_id: int, schedule_id: int | None, command: list[str]) -> None:
        started_at = dt.datetime.now(dt.timezone.utc).isoformat()
        try:
            mark_run_running(
                self.settings.db_path,
                run_id,
                started_at=started_at,
            )
            if schedule_id is not None:
                update_schedule_run_state(
                    self.settings.db_path,
                    schedule_id,
                    updated_at=started_at,
                    last_run_status="running",
                )

            proc = subprocess.run(
                command,
                cwd=str(self.settings.project_root),
                text=True,
                capture_output=True,
                check=False,
            )
            combined_output = "\n".join(
                part for part in [proc.stdout.strip(), proc.stderr.strip()] if part
            ).strip()

            if proc.returncode != 0:
                self._mark_run_failed(run_id, schedule_id, combined_output or "Ranking command failed")
                return

            artifacts = self._parse_run_output(combined_output)
            ranking_df = pd.read_csv(artifacts["ranking_csv_path"])
            finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
            finalize_succeeded_run(
                db_path=self.settings.db_path,
                run_id=run_id,
                ranking_df=ranking_df,
                signal_date=artifacts["signal_date"],
                ranking_csv_path=artifacts["ranking_csv_path"],
                html_report_path=artifacts["html_report_path"],
                experiment_id=artifacts["experiment_id"],
                recorder_id=artifacts["recorder_id"],
                log_output=combined_output,
                finished_at=finished_at,
            )
            if schedule_id is not None:
                update_schedule_run_state(
                    self.settings.db_path,
                    schedule_id,
                    updated_at=finished_at,
                    last_run_status="succeeded",
                )
        except Exception as exc:  # noqa: BLE001
            self._mark_run_failed(run_id, schedule_id, str(exc))
        finally:
            self._run_lock.release()

    def _execute_refresh_data_job(self, job_id: int, payload: dict[str, Any]) -> None:
        command = self._build_refresh_data_command(
            client_id=int(payload["client_id"]),
            start_date=str(payload["start_date"]),
            config_path=str(payload.get("config_path") or "config.yaml"),
        )
        self._run_job_step(job_id, "refresh_data", command)

    def _execute_backfill_job(self, job_id: int, payload: dict[str, Any]) -> None:
        model = get_model(self.settings.db_path, int(payload["model_id"]))
        if model is None:
            raise NotFoundError(f"Model {payload['model_id']} not found")
        signal_date = str(payload["signal_date"])
        command = self._build_backfill_command(
            signal_date=signal_date,
            workflow_base=str(model["workflow_base"]),
            model_id=int(model["id"]),
            client_id=int(payload["client_id"]),
            config_path=self._config_path_for_model(model),
        )
        self._run_job_step(job_id, f"backfill_{model['key']}", command)

    def _execute_append_portfolio_job(self, job_id: int, payload: dict[str, Any]) -> None:
        command = self._build_append_portfolio_command(
            portfolio_run_id=int(payload["portfolio_run_id"]),
            model_id=int(payload["model_id"]) if payload.get("model_id") is not None else None,
            end_date=str(payload["end_date"]),
        )
        self._run_job_step(job_id, f"append_portfolio_{payload['portfolio_run_id']}", command)

    def _execute_daily_close_pipeline_job(self, job_id: int, payload: dict[str, Any]) -> None:
        trade_date = dt.date.fromisoformat(str(payload["trade_date"]))
        client_id = int(payload["client_id"])
        start_date = str(payload["start_date"])
        include_portfolio = bool(payload.get("include_portfolio", True))

        models = [model for model in list_models(self.settings.db_path) if model.get("workflow_base")]
        config_paths: list[str] = []
        seen_config_paths: set[str] = set()
        for model in models:
            config_path = self._config_path_for_model(model)
            if config_path in seen_config_paths:
                continue
            seen_config_paths.add(config_path)
            config_paths.append(config_path)

        for config_path in config_paths:
            step_suffix = Path(config_path).stem
            self._run_job_step(
                job_id,
                f"refresh_data_{step_suffix}",
                self._build_refresh_data_command(
                    client_id=client_id,
                    start_date=start_date,
                    config_path=config_path,
                ),
            )

        trading_days = read_available_trading_days(self.settings.project_root)
        start_bound = dt.date.fromisoformat(start_date)
        for model in models:
            missing_days = self._missing_signal_days_for_model(
                model_id=int(model["id"]),
                trade_date=trade_date,
                trading_days=trading_days,
                fallback_start_date=start_bound,
            )
            if not missing_days:
                continue
            self._run_job_step(
                job_id,
                f"backfill_{model['key']}_{missing_days[0].isoformat()}_to_{missing_days[-1].isoformat()}",
                self._build_bulk_backfill_command(
                    start_date=missing_days[0].isoformat(),
                    end_date=missing_days[-1].isoformat(),
                    workflow_base=str(model["workflow_base"]),
                    model_id=int(model["id"]),
                    client_id=client_id,
                    config_path=self._config_path_for_model(model),
                ),
            )

        if not include_portfolio:
            return

        append_end_date = self._previous_trading_day(trade_date).isoformat()
        for model in models:
            portfolio_run = self._latest_portfolio_run_for_model(int(model["id"]))
            if portfolio_run is None:
                self._record_job_note(
                    job_id,
                    step_name=f"append_{model['key']}_portfolio",
                    message=f"Skipped portfolio append for model_id={model['id']} ({model['name']}): no portfolio run found yet",
                )
                continue
            self._run_job_step(
                job_id,
                f"append_{model['key']}_portfolio",
                self._build_append_portfolio_command(
                    portfolio_run_id=int(portfolio_run["id"]),
                    model_id=int(model["id"]),
                    end_date=append_end_date,
                ),
            )

    def _run_job_step(self, job_id: int, step_name: str, command: list[str]) -> str:
        started_at = dt.datetime.now(dt.timezone.utc).isoformat()
        command_text = " ".join(command)
        step_order = next_job_step_order(self.settings.db_path, job_id)
        step_id = create_job_step(
            self.settings.db_path,
            job_id=job_id,
            step_order=step_order,
            step_name=step_name,
            status="running",
            command=command_text,
            started_at=started_at,
        )

        child_env = os.environ.copy()
        child_env.setdefault("PYTHONUNBUFFERED", "1")
        proc = subprocess.Popen(
            command,
            cwd=str(self.settings.project_root),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=child_env,
            bufsize=1,
        )
        assert proc.stdout is not None
        lines: list[str] = []
        for line in proc.stdout:
            clean_line = line.rstrip("\n")
            lines.append(clean_line)
            append_job_step_output(self.settings.db_path, step_id=step_id, line=clean_line)
        rc = proc.wait()
        combined_output = "\n".join(lines).strip()
        finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
        status = "succeeded" if rc == 0 else "failed"
        error_text = None if rc == 0 else (combined_output or f"Step failed: {step_name}")

        update_job_step(
            self.settings.db_path,
            step_id=step_id,
            status=status,
            finished_at=finished_at,
            log_output=combined_output,
            error_text=error_text,
        )
        append_job_log(self.settings.db_path, job_id=job_id, section_name=step_name, content=combined_output)
        if rc != 0:
            raise RuntimeError(error_text or f"Step failed: {step_name}")
        return combined_output

    def _record_job_note(self, job_id: int, step_name: str, message: str) -> None:
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        step_order = next_job_step_order(self.settings.db_path, job_id)
        create_job_step(
            self.settings.db_path,
            job_id=job_id,
            step_order=step_order,
            step_name=step_name,
            status="succeeded",
            command=message,
            started_at=now,
            finished_at=now,
            log_output=message,
            error_text=None,
        )
        append_job_log(self.settings.db_path, job_id=job_id, section_name=step_name, content=message)

    def _finalize_run_from_output(self, run_id: int, schedule_id: int | None, combined_output: str) -> None:
        artifacts = self._parse_run_output(combined_output)
        ranking_df = pd.read_csv(artifacts["ranking_csv_path"])
        finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
        finalize_succeeded_run(
            db_path=self.settings.db_path,
            run_id=run_id,
            ranking_df=ranking_df,
            signal_date=artifacts["signal_date"],
            ranking_csv_path=artifacts["ranking_csv_path"],
            html_report_path=artifacts["html_report_path"],
            experiment_id=artifacts["experiment_id"],
            recorder_id=artifacts["recorder_id"],
            log_output=combined_output,
            finished_at=finished_at,
        )
        if schedule_id is not None:
            update_schedule_run_state(
                self.settings.db_path,
                schedule_id,
                updated_at=finished_at,
                last_run_status="succeeded",
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

    def _build_backfill_command(
        self,
        *,
        signal_date: str,
        workflow_base: str,
        model_id: int,
        client_id: int,
        config_path: str = "config.yaml",
    ) -> list[str]:
        python_bin = self.settings.project_root / ".venv" / "bin" / "python"
        return [
            str(python_bin),
            "backfill_rankings.py",
            "--config",
            config_path,
            "--start-date",
            signal_date,
            "--end-date",
            signal_date,
            "--workflow-base",
            workflow_base,
            "--model-id",
            str(model_id),
            "--client-id",
            str(client_id),
            "--html-mode",
            "cached",
            "--skip-existing-db",
            "--skip-existing-files",
        ]

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
        ]

    def _config_path_for_model(self, model: dict[str, Any]) -> str:
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

    def _previous_trading_day(self, trade_date: dt.date) -> dt.date:
        trading_days = read_available_trading_days(self.settings.project_root)
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

    def _parse_run_output(self, output: str) -> dict[str, str]:
        patterns = {
            "ranking_csv_path": r"\[ok\] ranking exported: (.+)",
            "html_report_path": r"\[ok\] html report exported: (.+)",
            "signal_date": r"\[ok\] signal_date=(\d{4}-\d{2}-\d{2})",
            "experiment_id": r"\[ok\] experiment_id=([^\s]+)",
            "recorder_id": r"recorder_id=([^\s]+)",
        }
        parsed: dict[str, str] = {}
        for key, pattern in patterns.items():
            match = re.search(pattern, output)
            if match is None:
                raise RuntimeError(f"Could not parse {key} from ranking output")
            parsed[key] = match.group(1).strip()
        return parsed
