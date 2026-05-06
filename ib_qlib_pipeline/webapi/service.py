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

import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from .db import connect, init_db, rows_to_dicts
from .model_store import ensure_default_models, get_model, list_models, resolve_or_create_model_for_workflow
from .portfolio_store import list_portfolio_runs
from .run_store import ranking_df_to_rows
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
        with connect(self.settings.db_path) as conn:
            rows = conn.execute("SELECT * FROM schedules ORDER BY hour, minute, id").fetchall()
        return rows_to_dicts(rows)

    def create_schedule(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        with connect(self.settings.db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO schedules (
                    name, enabled, timezone, day_of_week, hour, minute,
                    client_id, lookback_days, workflow_base, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["name"],
                    1 if payload["enabled"] else 0,
                    payload["timezone"],
                    payload["day_of_week"],
                    payload["hour"],
                    payload["minute"],
                    payload["client_id"],
                    payload["lookback_days"],
                    payload["workflow_base"],
                    now,
                    now,
                ),
            )
            schedule_id = int(cursor.lastrowid)
            row = conn.execute("SELECT * FROM schedules WHERE id = ?", (schedule_id,)).fetchone()
        self.reload_schedule_jobs()
        assert row is not None
        return dict(row)

    def update_schedule(self, schedule_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        if not payload:
            return self.get_schedule(schedule_id)
        allowed = {
            "name",
            "enabled",
            "timezone",
            "day_of_week",
            "hour",
            "minute",
            "client_id",
            "lookback_days",
            "workflow_base",
        }
        assignments = []
        params: list[Any] = []
        for key, value in payload.items():
            if key not in allowed or value is None:
                continue
            assignments.append(f"{key} = ?")
            params.append(1 if key == "enabled" and value else 0 if key == "enabled" else value)
        if not assignments:
            return self.get_schedule(schedule_id)
        params.extend([dt.datetime.now(dt.timezone.utc).isoformat(), schedule_id])
        with connect(self.settings.db_path) as conn:
            cursor = conn.execute(
                f"UPDATE schedules SET {', '.join(assignments)}, updated_at = ? WHERE id = ?",
                tuple(params),
            )
            if cursor.rowcount == 0:
                raise NotFoundError(f"Schedule {schedule_id} not found")
            row = conn.execute("SELECT * FROM schedules WHERE id = ?", (schedule_id,)).fetchone()
        self.reload_schedule_jobs()
        assert row is not None
        return dict(row)

    def delete_schedule(self, schedule_id: int) -> None:
        with connect(self.settings.db_path) as conn:
            cursor = conn.execute("DELETE FROM schedules WHERE id = ?", (schedule_id,))
            if cursor.rowcount == 0:
                raise NotFoundError(f"Schedule {schedule_id} not found")
        self.reload_schedule_jobs()

    def get_schedule(self, schedule_id: int) -> dict[str, Any]:
        with connect(self.settings.db_path) as conn:
            row = conn.execute("SELECT * FROM schedules WHERE id = ?", (schedule_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"Schedule {schedule_id} not found")
        return dict(row)

    def list_runs(
        self,
        limit: int = 50,
        status: str | None = None,
        signal_date: str | None = None,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT r.*, s.name AS schedule_name,
                   m.key AS model_key,
                   m.name AS model_name,
                   m.model_class AS model_class,
                   m.module_path AS model_module_path
            FROM runs r
            LEFT JOIN schedules s ON s.id = r.schedule_id
            LEFT JOIN models m ON m.id = r.model_id
            WHERE 1 = 1
        """
        params: list[Any] = []
        if status is not None:
            query += " AND r.status = ?"
            params.append(status)
        if signal_date is not None:
            query += " AND r.signal_date = ?"
            params.append(signal_date)
        query += " ORDER BY COALESCE(r.started_at, r.created_at) DESC LIMIT ?"
        params.append(limit)
        with connect(self.settings.db_path) as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return rows_to_dicts(rows)

    def list_jobs(self, limit: int = 30) -> list[dict[str, Any]]:
        with connect(self.settings.db_path) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM jobs
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return rows_to_dicts(rows)

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
        with connect(self.settings.db_path) as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if row is None:
                raise NotFoundError(f"Job {job_id} not found")
            steps = conn.execute(
                """
                SELECT *
                FROM job_steps
                WHERE job_id = ?
                ORDER BY step_order, id
                """,
                (job_id,),
            ).fetchall()
        item = dict(row)
        item["steps"] = rows_to_dicts(steps)
        return item

    def list_ranking_dates(
        self,
        *,
        limit: int = 20,
        offset: int = 0,
        query: str | None = None,
        model_id: int | None = None,
    ) -> dict[str, Any]:
        where = "WHERE r.trigger_source = 'backfill' AND r.signal_date IS NOT NULL"
        params: list[Any] = []
        if query:
            where += " AND r.signal_date LIKE ?"
            params.append(f"%{query}%")
        if model_id is not None:
            where += " AND r.model_id = ?"
            params.append(model_id)

        count_query = f"""
            SELECT COUNT(*)
            FROM runs r
            {where}
        """
        list_query = f"""
            SELECT r.id AS run_id, r.signal_date, r.row_count,
                   m.id AS model_id, m.key AS model_key, m.name AS model_name
            FROM runs r
            LEFT JOIN models m ON m.id = r.model_id
            {where}
            ORDER BY r.signal_date DESC
            LIMIT ? OFFSET ?
        """
        with connect(self.settings.db_path) as conn:
            total = int(conn.execute(count_query, tuple(params)).fetchone()[0])
            rows = conn.execute(list_query, tuple([*params, limit, offset])).fetchall()
        items = [dict(row) for row in rows]
        next_offset = offset + len(items)
        return {
            "items": items,
            "total": total,
            "has_more": next_offset < total,
            "next_offset": next_offset,
        }

    def get_run(self, run_id: int) -> dict[str, Any]:
        with connect(self.settings.db_path) as conn:
            row = conn.execute(
                """
                SELECT r.*, s.name AS schedule_name,
                       m.key AS model_key,
                       m.name AS model_name,
                       m.model_class AS model_class,
                       m.module_path AS model_module_path
                FROM runs r
                LEFT JOIN schedules s ON s.id = r.schedule_id
                LEFT JOIN models m ON m.id = r.model_id
                WHERE r.id = ?
                """,
                (run_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"Run {run_id} not found")
        return dict(row)

    def get_run_recommendations(self, run_id: int) -> list[dict[str, Any]]:
        with connect(self.settings.db_path) as conn:
            rows = conn.execute(
                """
                SELECT rank, symbol, score, percentile, entry_price, signal_date
                FROM recommendations
                WHERE run_id = ?
                ORDER BY rank
                """,
                (run_id,),
            ).fetchall()
        return rows_to_dicts(rows)

    def trigger_manual_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._start_run(
            schedule_id=None,
            trigger_source="manual",
            client_id=int(payload["client_id"]),
            lookback_days=int(payload["lookback_days"]),
            workflow_base=str(payload["workflow_base"]),
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
        self._start_run(
            schedule_id=schedule_id,
            trigger_source="schedule",
            client_id=int(schedule["client_id"]),
            lookback_days=int(schedule["lookback_days"]),
            workflow_base=str(schedule["workflow_base"]),
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
    ) -> dict[str, Any]:
        if not self._run_lock.acquire(blocking=False):
            raise BusyError("Another backend run or job is already in progress")
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        try:
            with connect(self.settings.db_path) as conn:
                cursor = conn.execute(
                    """
                    INSERT INTO jobs (
                        job_type, title, status, payload_json, created_at, requested_by
                    ) VALUES (?, ?, 'queued', ?, ?, ?)
                    """,
                    (
                        job_type,
                        title,
                        json.dumps(payload, ensure_ascii=True, sort_keys=True),
                        now,
                        "web",
                    ),
                )
                job_id = int(cursor.lastrowid)
            thread = threading.Thread(
                target=self._execute_job_thread,
                args=(job_id, target, payload),
                daemon=True,
            )
            thread.start()
            return self.get_job(job_id)
        except Exception:
            self._run_lock.release()
            raise

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

    def _execute_job_thread(self, job_id: int, target: Any, payload: dict[str, Any]) -> None:
        started_at = dt.datetime.now(dt.timezone.utc).isoformat()
        try:
            with connect(self.settings.db_path) as conn:
                conn.execute(
                    "UPDATE jobs SET status = 'running', started_at = ? WHERE id = ?",
                    (started_at, job_id),
                )
            target(job_id, payload)
            finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
            with connect(self.settings.db_path) as conn:
                conn.execute(
                    "UPDATE jobs SET status = 'succeeded', finished_at = ?, error_text = NULL WHERE id = ?",
                    (finished_at, job_id),
                )
        except Exception as exc:  # noqa: BLE001
            finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
            with connect(self.settings.db_path) as conn:
                conn.execute(
                    "UPDATE jobs SET status = 'failed', finished_at = ?, error_text = ? WHERE id = ?",
                    (finished_at, str(exc), job_id),
                )
        finally:
            self._run_lock.release()

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
        for model in models:
            self._run_job_step(
                job_id,
                f"backfill_{model['key']}",
                self._build_backfill_command(
                    signal_date=trade_date.isoformat(),
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
                raise RuntimeError(f"No portfolio run found for model_id={model['id']}")
            self._run_job_step(
                job_id,
                f"append_{model['key']}_portfolio",
                self._build_append_portfolio_command(
                    portfolio_run_id=int(portfolio_run["id"]),
                    model_id=int(model["id"]),
                    end_date=append_end_date,
                ),
            )

    def _run_job_step(self, job_id: int, step_name: str, command: list[str]) -> None:
        started_at = dt.datetime.now(dt.timezone.utc).isoformat()
        command_text = " ".join(command)
        with connect(self.settings.db_path) as conn:
            step_order = int(
                conn.execute(
                    "SELECT COALESCE(MAX(step_order), 0) + 1 FROM job_steps WHERE job_id = ?",
                    (job_id,),
                ).fetchone()[0]
            )
            cursor = conn.execute(
                """
                INSERT INTO job_steps (
                    job_id, step_order, step_name, status, command, started_at
                ) VALUES (?, ?, ?, 'running', ?, ?)
                """,
                (job_id, step_order, step_name, command_text, started_at),
            )
            step_id = int(cursor.lastrowid)

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
            with connect(self.settings.db_path) as conn:
                conn.execute(
                    """
                    UPDATE job_steps
                    SET log_output = COALESCE(log_output, '') ||
                        CASE
                          WHEN COALESCE(log_output, '') = '' THEN ''
                          ELSE char(10)
                        END ||
                        ?
                    WHERE id = ?
                    """,
                    (clean_line, step_id),
                )
        rc = proc.wait()
        combined_output = "\n".join(lines).strip()
        finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
        status = "succeeded" if rc == 0 else "failed"
        error_text = None if rc == 0 else (combined_output or f"Step failed: {step_name}")

        with connect(self.settings.db_path) as conn:
            conn.execute(
                """
                UPDATE job_steps
                SET status = ?, finished_at = ?, log_output = ?, error_text = ?
                WHERE id = ?
                """,
                (status, finished_at, combined_output, error_text, step_id),
            )
            conn.execute(
                """
                UPDATE jobs
                SET log_output = COALESCE(log_output, '') ||
                    CASE
                      WHEN COALESCE(log_output, '') = '' THEN ''
                      ELSE char(10) || char(10)
                    END ||
                    ?
                WHERE id = ?
                """,
                (f"[{step_name}]\n{combined_output}".strip(), job_id),
            )
        if rc != 0:
            raise RuntimeError(error_text or f"Step failed: {step_name}")

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

    def _latest_portfolio_run_for_model(self, model_id: int) -> dict[str, Any] | None:
        for row in list_portfolio_runs(self.settings.db_path):
            if row.get("model_id") == model_id:
                return row
        return None

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
