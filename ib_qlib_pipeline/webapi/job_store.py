from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

from sqlalchemy import func, select, update

from ..dborm.models import Job, JobLogLine, JobStep
from ..dborm.session import create_session_factory_for_path


def _session_for_db(db_path: Path):
    return create_session_factory_for_path(db_path)()


def _job_to_dict(job: Job) -> dict[str, Any]:
    return {
        "id": job.id,
        "job_type": job.job_type,
        "title": job.title,
        "status": job.status,
        "payload_json": job.payload_json,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "requested_by": job.requested_by,
        "worker_id": job.worker_id,
        "pid": job.pid,
        "heartbeat_at": job.heartbeat_at,
        "locked_at": job.locked_at,
        "cancel_requested_at": job.cancel_requested_at,
        "cancel_reason": job.cancel_reason,
        "log_output": job.log_output,
        "error_text": job.error_text,
    }


def _job_step_to_dict(step: JobStep) -> dict[str, Any]:
    return {
        "id": step.id,
        "job_id": step.job_id,
        "step_order": step.step_order,
        "step_name": step.step_name,
        "status": step.status,
        "command": step.command,
        "started_at": step.started_at,
        "finished_at": step.finished_at,
        "log_output": step.log_output,
        "error_text": step.error_text,
    }


def _job_log_line_to_dict(line: JobLogLine) -> dict[str, Any]:
    return {
        "id": line.id,
        "job_id": line.job_id,
        "step_id": line.step_id,
        "line_no": line.line_no,
        "stream": line.stream,
        "content": line.content,
        "created_at": line.created_at,
    }


def list_jobs(db_path: Path, *, limit: int = 30) -> list[dict[str, Any]]:
    with _session_for_db(db_path) as session:
        rows = session.execute(
            select(Job).order_by(Job.created_at.desc(), Job.id.desc()).limit(limit)
        ).scalars().all()
    return [_job_to_dict(row) for row in rows]


def get_job(db_path: Path, job_id: int) -> dict[str, Any] | None:
    with _session_for_db(db_path) as session:
        job = session.get(Job, job_id)
        if job is None:
            return None
        steps = session.execute(
            select(JobStep)
            .where(JobStep.job_id == job_id)
            .order_by(JobStep.step_order, JobStep.id)
        ).scalars().all()
    item = _job_to_dict(job)
    item["steps"] = [_job_step_to_dict(step) for step in steps]
    item["log_lines"] = list_job_log_lines(db_path, job_id=job_id)["lines"]
    return item


def create_job(
    db_path: Path,
    *,
    job_type: str,
    title: str,
    payload_json: str,
    requested_by: str = "web",
    created_at: str | None = None,
) -> int:
    created_at = created_at or dt.datetime.now(dt.timezone.utc).isoformat()
    with _session_for_db(db_path) as session:
        job = Job(
            job_type=job_type,
            title=title,
            status="queued",
            payload_json=payload_json,
            created_at=created_at,
            requested_by=requested_by,
        )
        session.add(job)
        session.commit()
        session.refresh(job)
        return int(job.id)


def mark_job_running(db_path: Path, job_id: int, started_at: str | None = None) -> None:
    started_at = started_at or dt.datetime.now(dt.timezone.utc).isoformat()
    with _session_for_db(db_path) as session:
        job = session.get(Job, job_id)
        if job is None:
            return
        job.status = "running"
        job.started_at = started_at
        session.commit()


def claim_next_queued_job(
    db_path: Path,
    *,
    worker_id: str,
    pid: int,
    now: str | None = None,
) -> dict[str, Any] | None:
    now = now or dt.datetime.now(dt.timezone.utc).isoformat()
    with _session_for_db(db_path) as session:
        queued = session.execute(
            select(Job)
            .where(Job.status == "queued")
            .order_by(Job.created_at.asc(), Job.id.asc())
            .limit(1)
        ).scalar_one_or_none()
        if queued is None:
            return None
        result = session.execute(
            update(Job)
            .where(Job.id == queued.id, Job.status == "queued")
            .values(
                status="running",
                worker_id=worker_id,
                pid=pid,
                locked_at=now,
                heartbeat_at=now,
                started_at=now,
            )
        )
        if (result.rowcount or 0) != 1:
            session.rollback()
            return None
        session.commit()
        job = session.get(Job, queued.id)
        assert job is not None
        return _job_to_dict(job)


def update_job_heartbeat(db_path: Path, job_id: int, *, worker_id: str, now: str | None = None) -> None:
    now = now or dt.datetime.now(dt.timezone.utc).isoformat()
    with _session_for_db(db_path) as session:
        session.execute(
            update(Job)
            .where(Job.id == job_id, Job.worker_id == worker_id, Job.status.in_(["running", "cancel_requested"]))
            .values(heartbeat_at=now)
        )
        session.commit()


def request_cancel_job(
    db_path: Path,
    job_id: int,
    *,
    reason: str,
    now: str | None = None,
) -> dict[str, Any] | None:
    now = now or dt.datetime.now(dt.timezone.utc).isoformat()
    with _session_for_db(db_path) as session:
        job = session.get(Job, job_id)
        if job is None:
            return None
        if job.status == "queued":
            job.status = "canceled"
            job.finished_at = now
            job.cancel_reason = reason
        elif job.status == "running":
            job.status = "cancel_requested"
            job.cancel_requested_at = now
            job.cancel_reason = reason
        elif job.status == "cancel_requested":
            if not job.cancel_requested_at:
                job.cancel_requested_at = now
            if not job.cancel_reason:
                job.cancel_reason = reason
        session.commit()
        return _job_to_dict(job)


def is_cancel_requested(db_path: Path, job_id: int) -> bool:
    with _session_for_db(db_path) as session:
        job = session.get(Job, job_id)
        return bool(job and job.status == "cancel_requested")


def mark_job_canceled(
    db_path: Path,
    job_id: int,
    *,
    worker_id: str,
    finished_at: str | None = None,
    reason: str | None = None,
) -> None:
    finished_at = finished_at or dt.datetime.now(dt.timezone.utc).isoformat()
    with _session_for_db(db_path) as session:
        session.execute(
            update(Job)
            .where(Job.id == job_id, Job.worker_id == worker_id, Job.status.in_(["running", "cancel_requested"]))
            .values(status="canceled", finished_at=finished_at, cancel_reason=reason)
        )
        session.commit()


def mark_job_failed_by_worker(
    db_path: Path,
    job_id: int,
    *,
    worker_id: str,
    finished_at: str | None = None,
    error_text: str | None = None,
) -> None:
    finished_at = finished_at or dt.datetime.now(dt.timezone.utc).isoformat()
    with _session_for_db(db_path) as session:
        session.execute(
            update(Job)
            .where(Job.id == job_id, Job.worker_id == worker_id, Job.status.in_(["running", "cancel_requested"]))
            .values(status="failed", finished_at=finished_at, error_text=error_text)
        )
        session.commit()


def mark_job_succeeded_by_worker(
    db_path: Path,
    job_id: int,
    *,
    worker_id: str,
    finished_at: str | None = None,
) -> None:
    finished_at = finished_at or dt.datetime.now(dt.timezone.utc).isoformat()
    with _session_for_db(db_path) as session:
        session.execute(
            update(Job)
            .where(Job.id == job_id, Job.worker_id == worker_id, Job.status.in_(["running", "cancel_requested"]))
            .values(status="succeeded", finished_at=finished_at, error_text=None)
        )
        session.commit()


def recover_stale_jobs(
    db_path: Path,
    *,
    stale_before: str,
    now: str | None = None,
) -> int:
    now = now or dt.datetime.now(dt.timezone.utc).isoformat()
    recovered = 0
    with _session_for_db(db_path) as session:
        running_jobs = session.execute(
            select(Job).where(
                Job.status.in_(["running", "cancel_requested"]),
                Job.heartbeat_at.is_not(None),
                Job.heartbeat_at < stale_before,
            )
        ).scalars().all()
        for job in running_jobs:
            if job.status == "running":
                job.status = "failed"
                job.finished_at = now
                job.error_text = f"Recovered stale job: heartbeat older than {stale_before}"
            else:
                job.status = "canceled"
                job.finished_at = now
            recovered += 1
        session.commit()
    return recovered


def mark_job_finished(
    db_path: Path,
    job_id: int,
    *,
    status: str,
    finished_at: str | None = None,
    error_text: str | None = None,
) -> None:
    finished_at = finished_at or dt.datetime.now(dt.timezone.utc).isoformat()
    with _session_for_db(db_path) as session:
        job = session.get(Job, job_id)
        if job is None:
            return
        job.status = status
        job.finished_at = finished_at
        job.error_text = error_text
        session.commit()


def next_job_step_order(db_path: Path, job_id: int) -> int:
    with _session_for_db(db_path) as session:
        max_order = session.execute(
            select(func.coalesce(func.max(JobStep.step_order), 0)).where(JobStep.job_id == job_id)
        ).scalar_one()
    return int(max_order) + 1


def create_job_step(
    db_path: Path,
    *,
    job_id: int,
    step_order: int,
    step_name: str,
    status: str,
    command: str | None,
    started_at: str | None = None,
    finished_at: str | None = None,
    log_output: str | None = None,
    error_text: str | None = None,
) -> int:
    with _session_for_db(db_path) as session:
        step = JobStep(
            job_id=job_id,
            step_order=step_order,
            step_name=step_name,
            status=status,
            command=command,
            started_at=started_at,
            finished_at=finished_at,
            log_output=log_output,
            error_text=error_text,
        )
        session.add(step)
        session.commit()
        session.refresh(step)
        return int(step.id)


def update_job_step(
    db_path: Path,
    *,
    step_id: int,
    status: str | None = None,
    finished_at: str | None = None,
    log_output: str | None = None,
    error_text: str | None = None,
) -> None:
    with _session_for_db(db_path) as session:
        step = session.get(JobStep, step_id)
        if step is None:
            return
        if status is not None:
            step.status = status
        if finished_at is not None:
            step.finished_at = finished_at
        if log_output is not None:
            step.log_output = log_output
        if error_text is not None or status == "succeeded":
            step.error_text = error_text
        session.commit()


def append_job_step_output(db_path: Path, *, step_id: int, line: str) -> None:
    with _session_for_db(db_path) as session:
        step = session.get(JobStep, step_id)
        if step is None:
            return
        existing = step.log_output or ""
        step.log_output = f"{existing}\n{line}".strip() if existing else line
        line_no = _next_job_log_line_no(session, job_id=step.job_id)
        session.add(
            JobLogLine(
                job_id=step.job_id,
                step_id=step.id,
                line_no=line_no,
                stream=None,
                content=line,
                created_at=dt.datetime.now(dt.timezone.utc).isoformat(),
            )
        )
        session.commit()


def append_job_log(db_path: Path, *, job_id: int, section_name: str, content: str) -> None:
    chunk = f"[{section_name}]\n{content}".strip()
    with _session_for_db(db_path) as session:
        job = session.get(Job, job_id)
        if job is None:
            return
        existing = job.log_output or ""
        job.log_output = f"{existing}\n\n{chunk}".strip() if existing else chunk
        created_at = dt.datetime.now(dt.timezone.utc).isoformat()
        for raw_line in chunk.splitlines():
            line_no = _next_job_log_line_no(session, job_id=job_id)
            session.add(
                JobLogLine(
                    job_id=job_id,
                    step_id=None,
                    line_no=line_no,
                    stream=None,
                    content=raw_line,
                    created_at=created_at,
                )
            )
        session.commit()


def append_job_log_line(
    db_path: Path,
    *,
    job_id: int,
    step_id: int | None,
    content: str,
    stream: str | None = "stdout",
    created_at: str | None = None,
) -> int:
    created_at = created_at or dt.datetime.now(dt.timezone.utc).isoformat()
    with _session_for_db(db_path) as session:
        line_no = _next_job_log_line_no(session, job_id=job_id)
        row = JobLogLine(
            job_id=job_id,
            step_id=step_id,
            line_no=line_no,
            stream=stream,
            content=content,
            created_at=created_at,
        )
        session.add(row)
        session.commit()
        session.refresh(row)
        return int(row.id)


def list_job_log_lines(
    db_path: Path,
    *,
    job_id: int,
    after_id: int = 0,
    limit: int | None = None,
) -> dict[str, Any]:
    with _session_for_db(db_path) as session:
        stmt = (
            select(JobLogLine)
            .where(JobLogLine.job_id == job_id, JobLogLine.id > after_id)
            .order_by(JobLogLine.line_no, JobLogLine.id)
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        rows = session.execute(stmt).scalars().all()
    payload = [_job_log_line_to_dict(row) for row in rows]
    next_after_id = after_id if not payload else int(payload[-1]["id"])
    return {"job_id": job_id, "lines": payload, "next_after_id": next_after_id}


def _next_job_log_line_no(session, *, job_id: int) -> int:
    max_line = session.execute(
        select(func.coalesce(func.max(JobLogLine.line_no), 0)).where(JobLogLine.job_id == job_id)
    ).scalar_one()
    return int(max_line) + 1
