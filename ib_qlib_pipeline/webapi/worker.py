from __future__ import annotations

import argparse
import datetime as dt
import os
import socket
import time

from .job_executor import JobCanceledError, JobExecutor
from .job_store import (
    claim_next_queued_job,
    mark_job_canceled,
    mark_job_failed_by_worker,
    mark_job_succeeded_by_worker,
    recover_stale_jobs,
)
from .schedule_store import update_schedule_run_state
from .settings import Settings


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


def _schedule_id_for_job(job: dict[str, object]) -> int | None:
    try:
        payload = job.get("payload_json")
        if not isinstance(payload, str) or not payload.strip():
            return None
        import json

        data = json.loads(payload)
        value = data.get("schedule_id")
        return int(value) if value is not None else None
    except Exception:  # noqa: BLE001
        return None


def _update_schedule_status(settings: Settings, job: dict[str, object], *, status: str) -> None:
    schedule_id = _schedule_id_for_job(job)
    if schedule_id is None:
        return
    update_schedule_run_state(
        settings.db_path,
        schedule_id,
        updated_at=utc_now(),
        last_run_status=status,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the standalone ranking job worker.")
    parser.add_argument("--once", action="store_true", help="Claim at most one queued job and exit.")
    parser.add_argument("--poll-interval", type=float, default=2.0, help="Polling interval in seconds.")
    parser.add_argument("--worker-id", default=None, help="Worker identifier.")
    parser.add_argument("--stale-timeout-seconds", type=int, default=900, help="Stale heartbeat timeout.")
    return parser.parse_args()


def run_worker(*, once: bool, poll_interval: float, worker_id: str, stale_timeout_seconds: int) -> None:
    settings = Settings.load()
    executor = JobExecutor(settings, worker_id)
    pid = os.getpid()

    while True:
        now_dt = dt.datetime.now(dt.timezone.utc)
        stale_before = (now_dt - dt.timedelta(seconds=stale_timeout_seconds)).isoformat()
        recover_stale_jobs(settings.db_path, stale_before=stale_before, now=now_dt.isoformat())

        job = claim_next_queued_job(
            settings.db_path,
            worker_id=worker_id,
            pid=pid,
            now=now_dt.isoformat(),
        )
        if job is None:
            if once:
                return
            time.sleep(poll_interval)
            continue

        _update_schedule_status(settings, job, status="running")

        try:
            executor.execute_job(job)
            mark_job_succeeded_by_worker(
                settings.db_path,
                int(job["id"]),
                worker_id=worker_id,
                finished_at=utc_now(),
            )
            _update_schedule_status(settings, job, status="succeeded")
        except JobCanceledError as exc:
            mark_job_canceled(
                settings.db_path,
                int(job["id"]),
                worker_id=worker_id,
                finished_at=utc_now(),
                reason=str(exc),
            )
            _update_schedule_status(settings, job, status="canceled")
        except Exception as exc:  # noqa: BLE001
            mark_job_failed_by_worker(
                settings.db_path,
                int(job["id"]),
                worker_id=worker_id,
                finished_at=utc_now(),
                error_text=str(exc),
            )
            _update_schedule_status(settings, job, status="failed")

        if once:
            return


def main() -> None:
    args = parse_args()
    run_worker(
        once=bool(args.once),
        poll_interval=float(args.poll_interval),
        worker_id=str(args.worker_id or default_worker_id()),
        stale_timeout_seconds=int(args.stale_timeout_seconds),
    )


if __name__ == "__main__":
    main()
