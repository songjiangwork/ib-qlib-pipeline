from __future__ import annotations

import datetime as dt
import json
import os
import re
import select
import signal
import subprocess
from pathlib import Path
from typing import Any

import pandas as pd

from ..ranking.ranking_loader import read_available_trading_days
from .job_store import (
    append_job_log,
    append_job_log_line,
    append_job_step_output,
    create_job_step,
    is_cancel_requested,
    next_job_step_order,
    update_job_heartbeat,
    update_job_step,
)
from .model_store import get_model, resolve_or_create_model_for_workflow
from .portfolio_store import latest_portfolio_run_for_model
from .run_store import (
    create_queued_run,
    finalize_succeeded_run,
    latest_backfill_signal_date_for_model,
    mark_run_failed as mark_run_failed_record,
)
from .schedule_store import update_schedule_run_state
from .settings import Settings
from .universe_store import get_universe as get_universe_record


class JobCanceledError(RuntimeError):
    """Raised when a worker cancels an in-flight job."""


class JobExecutor:
    def __init__(self, settings: Settings, worker_id: str) -> None:
        self.settings = settings
        self.worker_id = worker_id

    def execute_job(self, job: dict[str, Any]) -> None:
        payload = json.loads(job.get("payload_json") or "{}")
        job_type = str(job["job_type"])

        if job_type == "refresh_data":
            self.execute_refresh_data_job(int(job["id"]), payload)
            return
        if job_type == "backfill_ranking":
            self.execute_backfill_job(int(job["id"]), payload)
            return
        if job_type == "append_portfolio":
            self.execute_append_portfolio_job(int(job["id"]), payload)
            return
        if job_type in {"daily_close_pipeline", "scheduled_daily_close_pipeline"}:
            self.execute_daily_close_pipeline_job(int(job["id"]), payload)
            return
        if job_type in {"manual_ranking_run", "scheduled_ranking_run"}:
            self.execute_ranking_run_job(int(job["id"]), payload)
            return
        raise RuntimeError(f"Unsupported job_type={job_type}")

    def execute_ranking_run_job(self, job_id: int, payload: dict[str, Any]) -> None:
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
        combined_output = self.execute_step(job_id, "ranking_run", command)
        self._finalize_run_from_output(run_id, schedule_id, combined_output)

    def execute_refresh_data_job(self, job_id: int, payload: dict[str, Any]) -> None:
        config_path = str(payload.get("config_path") or "config.yaml")
        market = str(payload.get("market") or self._market_for_config_path(config_path))
        command = self._build_refresh_command(
            market=market,
            client_id=int(payload["client_id"]),
            start_date=str(payload["start_date"]),
            config_path=config_path,
        )
        self.execute_step(job_id, "refresh_data", command)

    def execute_backfill_job(self, job_id: int, payload: dict[str, Any]) -> None:
        model = get_model(self.settings.db_path, int(payload["model_id"]))
        if model is None:
            raise RuntimeError(f"Model {payload['model_id']} not found")
        signal_date = str(payload["signal_date"])
        command = self._build_backfill_command(
            signal_date=signal_date,
            workflow_base=str(model["workflow_base"]),
            model_id=int(model["id"]),
            client_id=int(payload["client_id"]),
            config_path=self._config_path_for_model(model),
        )
        self.execute_step(job_id, f"backfill_{model['key']}", command)

    def execute_append_portfolio_job(self, job_id: int, payload: dict[str, Any]) -> None:
        command = self._build_append_portfolio_command(
            portfolio_run_id=int(payload["portfolio_run_id"]),
            model_id=int(payload["model_id"]) if payload.get("model_id") is not None else None,
            end_date=str(payload["end_date"]),
        )
        self.execute_step(job_id, f"append_portfolio_{payload['portfolio_run_id']}", command)

    def execute_daily_close_pipeline_job(self, job_id: int, payload: dict[str, Any]) -> None:
        trade_date = dt.date.fromisoformat(str(payload["trade_date"]))
        client_id = int(payload["client_id"])
        start_date = str(payload["start_date"])
        include_portfolio = bool(payload.get("include_portfolio", True))

        models = [model for model in self._list_models() if model.get("workflow_base")]
        refresh_targets: list[tuple[str, str]] = []
        seen_refresh_targets: set[tuple[str, str]] = set()
        for model in models:
            config_path = self._config_path_for_model(model)
            market = self._market_for_model(model)
            refresh_target = (market, config_path)
            if refresh_target in seen_refresh_targets:
                continue
            seen_refresh_targets.add(refresh_target)
            refresh_targets.append(refresh_target)

        for market, config_path in refresh_targets:
            step_suffix = Path(config_path).stem
            step_prefix = "refresh_cn" if market == "cn" else "refresh_data"
            self.execute_step(
                job_id,
                f"{step_prefix}_{step_suffix}",
                self._build_refresh_command(
                    market=market,
                    client_id=client_id,
                    start_date=start_date,
                    config_path=config_path,
                ),
            )

        start_bound = dt.date.fromisoformat(start_date)
        trading_days_by_config: dict[str, list[dt.date]] = {}
        for model in models:
            config_path = self._config_path_for_model(model)
            if config_path not in trading_days_by_config:
                trading_days_by_config[config_path] = read_available_trading_days(
                    self.settings.project_root,
                    config_path=(self.settings.project_root / config_path).resolve(),
                )
            missing_days = self._missing_signal_days_for_model(
                model_id=int(model["id"]),
                trade_date=trade_date,
                trading_days=trading_days_by_config[config_path],
                fallback_start_date=start_bound,
            )
            if not missing_days:
                continue
            self.execute_step(
                job_id,
                f"backfill_{model['key']}_{missing_days[0].isoformat()}_to_{missing_days[-1].isoformat()}",
                self._build_bulk_backfill_command(
                    start_date=missing_days[0].isoformat(),
                    end_date=missing_days[-1].isoformat(),
                    workflow_base=str(model["workflow_base"]),
                    model_id=int(model["id"]),
                    client_id=client_id,
                    config_path=config_path,
                ),
            )

        if not include_portfolio:
            return

        for model in models:
            append_end_date = self._previous_trading_day(
                trade_date,
                config_path=self._config_path_for_model(model),
            ).isoformat()
            portfolio_run = self._latest_portfolio_run_for_model(int(model["id"]))
            if portfolio_run is None:
                self.record_job_note(
                    job_id,
                    step_name=f"append_{model['key']}_portfolio",
                    message=f"Skipped portfolio append for model_id={model['id']} ({model['name']}): no portfolio run found yet",
                )
                continue
            self.execute_step(
                job_id,
                f"append_{model['key']}_portfolio",
                self._build_append_portfolio_command(
                    portfolio_run_id=int(portfolio_run["id"]),
                    model_id=int(model["id"]),
                    end_date=append_end_date,
                ),
            )

    def execute_step(self, job_id: int, step_name: str, command: list[str]) -> str:
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
        lines: list[str] = []
        try:
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
                start_new_session=True,
            )
            assert proc.stdout is not None
            fd = proc.stdout.fileno()
            last_heartbeat = dt.datetime.now(dt.timezone.utc)
            canceled = False
            while True:
                ready, _, _ = select.select([fd], [], [], 1.0)
                if ready:
                    line = proc.stdout.readline()
                    if line:
                        clean_line = line.rstrip("\n")
                        lines.append(clean_line)
                        append_job_step_output(self.settings.db_path, step_id=step_id, line=clean_line)
                if is_cancel_requested(self.settings.db_path, job_id):
                    canceled = True
                    append_job_log_line(
                        self.settings.db_path,
                        job_id=job_id,
                        step_id=step_id,
                        content="cancel requested",
                        stream="system",
                    )
                    self._terminate_process_group(proc)
                    break
                now = dt.datetime.now(dt.timezone.utc)
                if (now - last_heartbeat).total_seconds() >= 2:
                    update_job_heartbeat(
                        self.settings.db_path,
                        job_id,
                        worker_id=self.worker_id,
                        now=now.isoformat(),
                    )
                    last_heartbeat = now
                if proc.poll() is not None:
                    break
            rc = proc.wait()
            combined_output = "\n".join(lines).strip()
            finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
            if canceled:
                update_job_step(
                    self.settings.db_path,
                    step_id=step_id,
                    status="canceled",
                    finished_at=finished_at,
                    log_output=combined_output,
                    error_text="Job canceled",
                )
                if combined_output:
                    append_job_log(self.settings.db_path, job_id=job_id, section_name=step_name, content=combined_output)
                raise JobCanceledError("Job canceled")
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
        except JobCanceledError:
            raise
        except Exception as exc:  # noqa: BLE001
            combined_output = "\n".join(lines).strip()
            finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
            error_text = str(exc)
            update_job_step(
                self.settings.db_path,
                step_id=step_id,
                status="failed",
                finished_at=finished_at,
                log_output=combined_output or None,
                error_text=error_text,
            )
            if combined_output:
                append_job_log(self.settings.db_path, job_id=job_id, section_name=step_name, content=combined_output)
            raise

    def record_job_note(self, job_id: int, *, step_name: str, message: str) -> None:
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        step_order = next_job_step_order(self.settings.db_path, job_id)
        step_id = create_job_step(
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
        append_job_log_line(self.settings.db_path, job_id=job_id, step_id=step_id, content=message, stream="system", created_at=now)
        append_job_log(self.settings.db_path, job_id=job_id, section_name=step_name, content=message)

    def _terminate_process_group(self, proc: subprocess.Popen[str]) -> None:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                return

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

    def _list_models(self) -> list[dict[str, Any]]:
        from .model_store import list_models

        return list_models(self.settings.db_path)

    def _build_refresh_command(
        self,
        *,
        market: str,
        client_id: int,
        start_date: str,
        config_path: str = "config.yaml",
    ) -> list[str]:
        if market == "cn":
            return self._build_refresh_cn_command(start_date=start_date, config_path=config_path)
        return self._build_refresh_data_command(client_id=client_id, start_date=start_date, config_path=config_path)

    def _build_refresh_data_command(self, *, client_id: int, start_date: str, config_path: str = "config.yaml") -> list[str]:
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

    def _build_refresh_cn_command(self, *, start_date: str, config_path: str = "config_cn.yaml") -> list[str]:
        python_bin = self.settings.project_root / ".venv" / "bin" / "python"
        return [str(python_bin), "scripts/refresh_cn_daily_baostock.py", "--config", config_path, "--start-date", start_date]

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

    def _build_append_portfolio_command(self, *, portfolio_run_id: int, model_id: int | None, end_date: str) -> list[str]:
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
        mark_run_failed_record(self.settings.db_path, run_id, finished_at=finished_at, error_text=error_text)
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
