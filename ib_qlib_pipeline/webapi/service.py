from __future__ import annotations

import datetime as dt
import json
import os
import re
import sqlite3
import subprocess
import threading
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from .db import connect, init_db, rows_to_dicts
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
from .portfolio_store import list_portfolio_runs, latest_portfolio_run_for_model
from .run_store import (
    get_run as get_run_record,
    get_run_recommendations as get_run_recommendation_records,
    latest_backfill_signal_date_for_model,
    list_ranking_dates as list_ranking_date_records,
    list_runs as list_run_records,
    ranking_df_to_rows,
)
from .schedule_store import (
    create_schedule as create_schedule_record,
    delete_schedule as delete_schedule_record,
    get_schedule as get_schedule_record,
    list_schedules as list_schedule_records,
    update_schedule as update_schedule_record,
)
from .settings import Settings
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
        ensure_default_models(settings.db_path)
        self._backfill_legacy_run_models()

    def start(self) -> None:
        self._scheduler.start()
        self.reload_schedule_jobs()

    def stop(self) -> None:
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)

    def list_schedules(self) -> list[dict[str, Any]]:
        return list_schedule_records(self.settings.db_path)

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
        items: list[dict[str, Any]] = []
        for model in models:
            model_runs = [row for row in runs if row.get("model_id") == model["id"]]
            latest_ranking = next(
                (
                    row
                    for row in model_runs
                    if row.get("trigger_source") == "backfill" and row.get("signal_date") is not None
                ),
                None,
            )
            portfolio_run = next((row for row in portfolio_runs if row.get("model_id") == model["id"]), None)
            ranking_signal_date = str(latest_ranking["signal_date"]) if latest_ranking else None
            portfolio_end = str(portfolio_run["end_signal_date"]) if portfolio_run and portfolio_run.get("end_signal_date") else None
            items.append(
                {
                    "model_id": model["id"],
                    "model_key": model["key"],
                    "model_name": model["name"],
                    "workflow_base": model.get("workflow_base"),
                    "latest_ranking_run_id": latest_ranking.get("id") if latest_ranking else None,
                    "latest_ranking_signal_date": ranking_signal_date,
                    "ranking_ready_for_trade_date": ranking_signal_date == trade_date,
                    "portfolio_run_id": portfolio_run.get("id") if portfolio_run else None,
                    "portfolio_run_name": portfolio_run.get("name") if portfolio_run else None,
                    "portfolio_end_signal_date": portfolio_end,
                    "portfolio_ready_for_trade_date": portfolio_end == expected_portfolio_end,
                    "expected_portfolio_end_signal_date": expected_portfolio_end,
                }
            )
        return {
            "trade_date": trade_date,
            "expected_portfolio_end_signal_date": expected_portfolio_end,
            "models": items,
        }

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
        schedule_type = str(schedule.get("schedule_type") or "ranking")
        if schedule_type == "daily_close_pipeline":
            now_local = dt.datetime.now(ZoneInfo(str(schedule["timezone"])))
            trade_date = now_local.date().isoformat()
            pipeline_start_date = schedule.get("pipeline_start_date")
            if not pipeline_start_date:
                pipeline_start_date = (now_local.date() - dt.timedelta(days=int(schedule["lookback_days"]))).isoformat()
            self._start_job(
                job_type="scheduled_daily_close_pipeline",
                title=f"Scheduled daily close pipeline #{schedule_id} ({trade_date})",
                payload={
                    "schedule_id": schedule_id,
                    "trade_date": trade_date,
                    "client_id": int(schedule["client_id"]),
                    "start_date": str(pipeline_start_date),
                    "include_portfolio": bool(schedule.get("pipeline_include_portfolio")),
                },
                target=self._execute_daily_close_pipeline_job,
                schedule_id=schedule_id,
            )
            return

        self._start_job(
            job_type="scheduled_ranking_run",
            title=f"Scheduled ranking run #{schedule_id} ({schedule['workflow_base']})",
            payload={
                "schedule_id": schedule_id,
                "trigger_source": "schedule",
                "client_id": int(schedule["client_id"]),
                "lookback_days": int(schedule["lookback_days"]),
                "workflow_base": str(schedule["workflow_base"]),
            },
            target=self._execute_ranking_run_job,
            schedule_id=schedule_id,
        )

    def _start_run(
        self,
        schedule_id: int | None,
        trigger_source: str,
        client_id: int,
        lookback_days: int,
        workflow_base: str,
    ) -> dict[str, Any]:
        if not self._run_lock.acquire(blocking=False):
            raise BusyError("A ranking run is already in progress")
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
        try:
            with connect(self.settings.db_path) as conn:
                cursor = conn.execute(
                    """
                    INSERT INTO runs (
                        schedule_id, model_id, trigger_source, status, client_id, lookback_days,
                        workflow_base, command, created_at
                    ) VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, ?)
                    """,
                    (
                        schedule_id,
                        model_id,
                        trigger_source,
                        client_id,
                        lookback_days,
                        workflow_base,
                        " ".join(command),
                        now,
                    ),
                )
                run_id = int(cursor.lastrowid)
                if schedule_id is not None:
                    conn.execute(
                        """
                        UPDATE schedules
                        SET last_triggered_at = ?, last_run_id = ?, last_run_status = 'queued', updated_at = ?
                        WHERE id = ?
                        """,
                        (now, run_id, now, schedule_id),
                    )
            thread = threading.Thread(
                target=self._execute_run,
                args=(run_id, schedule_id, command),
                daemon=True,
            )
            thread.start()
            return self.get_run(run_id)
        except Exception:
            self._run_lock.release()
            raise

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
            with connect(self.settings.db_path) as conn:
                if schedule_id is not None:
                    conn.execute(
                        """
                        UPDATE schedules
                        SET last_triggered_at = ?, last_run_status = 'queued', updated_at = ?
                        WHERE id = ?
                        """,
                        (now, now, schedule_id),
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
        with connect(self.settings.db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO runs (
                    schedule_id, model_id, trigger_source, status, client_id, lookback_days,
                    workflow_base, command, created_at
                ) VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, ?)
                """,
                (
                    schedule_id,
                    model_id,
                    trigger_source,
                    client_id,
                    lookback_days,
                    workflow_base,
                    " ".join(command),
                    now,
                ),
            )
            run_id = int(cursor.lastrowid)
            if schedule_id is not None:
                conn.execute(
                    """
                    UPDATE schedules
                    SET last_triggered_at = ?, last_run_id = ?, last_run_status = 'queued', updated_at = ?
                    WHERE id = ?
                    """,
                    (now, run_id, now, schedule_id),
                )
        return run_id

    def _backfill_legacy_run_models(self) -> None:
        lgb_model_id = resolve_or_create_model_for_workflow(
            self.settings.db_path,
            self.settings.project_root,
            "examples/workflow_us_lgb_2020_port.yaml",
        )
        with connect(self.settings.db_path) as conn:
            conn.execute(
                """
                UPDATE runs
                SET model_id = ?
                WHERE model_id IS NULL
                """,
                (lgb_model_id,),
            )

    def _execute_job_thread(self, job_id: int, target: Any, payload: dict[str, Any], schedule_id: int | None) -> None:
        started_at = dt.datetime.now(dt.timezone.utc).isoformat()
        try:
            mark_job_running(self.settings.db_path, job_id, started_at)
            with connect(self.settings.db_path) as conn:
                if schedule_id is not None:
                    conn.execute(
                        "UPDATE schedules SET last_run_status = 'running', updated_at = ? WHERE id = ?",
                        (started_at, schedule_id),
                    )
            target(job_id, payload)
            finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
            mark_job_finished(self.settings.db_path, job_id, status="succeeded", finished_at=finished_at, error_text=None)
            with connect(self.settings.db_path) as conn:
                if schedule_id is not None:
                    conn.execute(
                        "UPDATE schedules SET last_run_status = 'succeeded', updated_at = ? WHERE id = ?",
                        (finished_at, schedule_id),
                    )
        except Exception as exc:  # noqa: BLE001
            finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
            mark_job_finished(self.settings.db_path, job_id, status="failed", finished_at=finished_at, error_text=str(exc))
            with connect(self.settings.db_path) as conn:
                if schedule_id is not None:
                    conn.execute(
                        "UPDATE schedules SET last_run_status = 'failed', updated_at = ? WHERE id = ?",
                        (finished_at, schedule_id),
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
            with connect(self.settings.db_path) as conn:
                conn.execute(
                    "UPDATE runs SET status = 'running', started_at = ? WHERE id = ?",
                    (started_at, run_id),
                )
                if schedule_id is not None:
                    conn.execute(
                        "UPDATE schedules SET last_run_status = 'running', updated_at = ? WHERE id = ?",
                        (started_at, schedule_id),
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
            recommendations = ranking_df_to_rows(ranking_df)

            with connect(self.settings.db_path) as conn:
                conn.execute(
                    """
                    UPDATE runs
                    SET status = 'succeeded',
                        finished_at = ?,
                        signal_date = ?,
                        ranking_csv_path = ?,
                        html_report_path = ?,
                        experiment_id = ?,
                        recorder_id = ?,
                        row_count = ?,
                        log_output = ?,
                        error_text = NULL
                    WHERE id = ?
                    """,
                    (
                        finished_at,
                        artifacts["signal_date"],
                        artifacts["ranking_csv_path"],
                        artifacts["html_report_path"],
                        artifacts["experiment_id"],
                        artifacts["recorder_id"],
                        len(recommendations),
                        combined_output,
                        run_id,
                    ),
                )
                conn.executemany(
                    """
                    INSERT INTO recommendations (run_id, rank, symbol, score, percentile, entry_price, signal_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            run_id,
                            int(row["rank"]),
                            str(row["symbol"]),
                            float(row["score"]),
                            float(row["percentile"]) if row["percentile"] is not None else None,
                            float(row["entry_price"]) if row["entry_price"] is not None else None,
                            str(row["signal_date"]),
                        )
                        for row in recommendations
                    ],
                )
                if schedule_id is not None:
                    conn.execute(
                        "UPDATE schedules SET last_run_status = 'succeeded', updated_at = ? WHERE id = ?",
                        (finished_at, schedule_id),
                    )
        except Exception as exc:  # noqa: BLE001
            self._mark_run_failed(run_id, schedule_id, str(exc))
        finally:
            self._run_lock.release()

    def _execute_refresh_data_job(self, job_id: int, payload: dict[str, Any]) -> None:
        command = self._build_refresh_data_command(
            client_id=int(payload["client_id"]),
            start_date=str(payload["start_date"]),
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

        self._run_job_step(
            job_id,
            "refresh_data",
            self._build_refresh_data_command(client_id=client_id, start_date=start_date),
        )

        models = [model for model in list_models(self.settings.db_path) if model.get("workflow_base")]
        trading_days = read_available_trading_days(self.settings.project_root)
        start_bound = dt.date.fromisoformat(start_date)
        for model in models:
            missing_days = self._missing_signal_days_for_model(
                model_id=int(model["id"]),
                trade_date=trade_date,
                trading_days=trading_days,
                fallback_start_date=start_bound,
            )
            for signal_day in missing_days:
                self._run_job_step(
                    job_id,
                    f"backfill_{model['key']}_{signal_day.isoformat()}",
                    self._build_backfill_command(
                        signal_date=signal_day.isoformat(),
                        workflow_base=str(model["workflow_base"]),
                        model_id=int(model["id"]),
                        client_id=client_id,
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
        recommendations = ranking_df_to_rows(ranking_df)
        with connect(self.settings.db_path) as conn:
            conn.execute(
                """
                UPDATE runs
                SET status = 'succeeded',
                    finished_at = ?,
                    signal_date = ?,
                    ranking_csv_path = ?,
                    html_report_path = ?,
                    experiment_id = ?,
                    recorder_id = ?,
                    row_count = ?,
                    log_output = ?,
                    error_text = NULL
                WHERE id = ?
                """,
                (
                    finished_at,
                    artifacts["signal_date"],
                    artifacts["ranking_csv_path"],
                    artifacts["html_report_path"],
                    artifacts["experiment_id"],
                    artifacts["recorder_id"],
                    len(recommendations),
                    combined_output,
                    run_id,
                ),
            )
            conn.executemany(
                """
                INSERT INTO recommendations (run_id, rank, symbol, score, percentile, entry_price, signal_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        int(row["rank"]),
                        str(row["symbol"]),
                        float(row["score"]),
                        float(row["percentile"]) if row["percentile"] is not None else None,
                        float(row["entry_price"]) if row["entry_price"] is not None else None,
                        str(row["signal_date"]),
                    )
                    for row in recommendations
                ],
            )
            if schedule_id is not None:
                conn.execute(
                    "UPDATE schedules SET last_run_status = 'succeeded', updated_at = ? WHERE id = ?",
                    (finished_at, schedule_id),
                )

    def _build_refresh_data_command(self, *, client_id: int, start_date: str) -> list[str]:
        python_bin = self.settings.project_root / ".venv" / "bin" / "python"
        return [
            str(python_bin),
            "run.py",
            "--config",
            "config.yaml",
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
    ) -> list[str]:
        python_bin = self.settings.project_root / ".venv" / "bin" / "python"
        return [
            str(python_bin),
            "backfill_rankings.py",
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
        with connect(self.settings.db_path) as conn:
            conn.execute(
                """
                UPDATE runs
                SET status = 'failed',
                    finished_at = ?,
                    error_text = ?,
                    log_output = COALESCE(log_output, ?)
                WHERE id = ?
                """,
                (finished_at, error_text, error_text, run_id),
            )
            if schedule_id is not None:
                conn.execute(
                    "UPDATE schedules SET last_run_status = 'failed', updated_at = ? WHERE id = ?",
                    (finished_at, schedule_id),
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
