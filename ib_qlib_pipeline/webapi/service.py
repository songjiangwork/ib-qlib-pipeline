from __future__ import annotations

import datetime as dt
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
from .settings import Settings


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
            SELECT r.*, s.name AS schedule_name
            FROM runs r
            LEFT JOIN schedules s ON s.id = r.schedule_id
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

    def get_run(self, run_id: int) -> dict[str, Any]:
        with connect(self.settings.db_path) as conn:
            row = conn.execute(
                """
                SELECT r.*, s.name AS schedule_name
                FROM runs r
                LEFT JOIN schedules s ON s.id = r.schedule_id
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
                        schedule_id, trigger_source, status, client_id, lookback_days,
                        workflow_base, command, created_at
                    ) VALUES (?, ?, 'queued', ?, ?, ?, ?, ?)
                    """,
                    (schedule_id, trigger_source, client_id, lookback_days, workflow_base, " ".join(command), now),
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
            recommendations = self._ranking_df_to_rows(ranking_df)
            finished_at = dt.datetime.now(dt.timezone.utc).isoformat()

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

    def _ranking_df_to_rows(self, ranking_df: pd.DataFrame) -> list[dict[str, Any]]:
        if "close" not in ranking_df.columns:
            raise RuntimeError("Ranking CSV missing close column")
        signal_date = pd.to_datetime(ranking_df["signal_date"]).dt.date.iloc[0].isoformat()
        rows: list[dict[str, Any]] = []
        for record in ranking_df.to_dict(orient="records"):
            rows.append(
                {
                    "rank": int(record["rank"]),
                    "symbol": str(record["symbol"]),
                    "score": float(record["score"]),
                    "percentile": float(record["percentile"]) if pd.notna(record["percentile"]) else None,
                    "entry_price": float(record["close"]) if pd.notna(record["close"]) else None,
                    "signal_date": signal_date,
                }
            )
        return rows
