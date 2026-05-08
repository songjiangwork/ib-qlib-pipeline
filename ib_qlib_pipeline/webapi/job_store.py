from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

from sqlalchemy import func, select

from ..dborm.models import Job, JobStep
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
        session.commit()


def append_job_log(db_path: Path, *, job_id: int, section_name: str, content: str) -> None:
    chunk = f"[{section_name}]\n{content}".strip()
    with _session_for_db(db_path) as session:
        job = session.get(Job, job_id)
        if job is None:
            return
        existing = job.log_output or ""
        job.log_output = f"{existing}\n\n{chunk}".strip() if existing else chunk
        session.commit()
