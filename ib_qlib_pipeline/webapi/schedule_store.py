from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

from sqlalchemy import select

from ..dborm.models import Schedule
from ..dborm.session import create_session_factory_for_path


def _session_for_db(db_path: Path):
    return create_session_factory_for_path(db_path)()


def _schedule_to_dict(schedule: Schedule) -> dict[str, Any]:
    return {
        "id": schedule.id,
        "name": schedule.name,
        "schedule_type": schedule.schedule_type,
        "enabled": bool(schedule.enabled),
        "timezone": schedule.timezone,
        "day_of_week": schedule.day_of_week,
        "hour": schedule.hour,
        "minute": schedule.minute,
        "client_id": schedule.client_id,
        "lookback_days": schedule.lookback_days,
        "workflow_base": schedule.workflow_base,
        "pipeline_start_date": schedule.pipeline_start_date,
        "pipeline_include_portfolio": bool(schedule.pipeline_include_portfolio),
        "created_at": schedule.created_at,
        "updated_at": schedule.updated_at,
        "last_triggered_at": schedule.last_triggered_at,
        "last_run_id": schedule.last_run_id,
        "last_run_status": schedule.last_run_status,
    }


def list_schedules(db_path: Path) -> list[dict[str, Any]]:
    with _session_for_db(db_path) as session:
        rows = session.execute(
            select(Schedule).order_by(Schedule.hour, Schedule.minute, Schedule.id)
        ).scalars().all()
    return [_schedule_to_dict(row) for row in rows]


def get_schedule(db_path: Path, schedule_id: int) -> dict[str, Any] | None:
    with _session_for_db(db_path) as session:
        schedule = session.get(Schedule, schedule_id)
    if schedule is None:
        return None
    return _schedule_to_dict(schedule)


def create_schedule(db_path: Path, payload: dict[str, Any], default_workflow_base: str) -> dict[str, Any]:
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    with _session_for_db(db_path) as session:
        schedule = Schedule(
            name=payload["name"],
            schedule_type=str(payload.get("schedule_type") or "ranking"),
            enabled=bool(payload["enabled"]),
            timezone=payload["timezone"],
            day_of_week=payload["day_of_week"],
            hour=payload["hour"],
            minute=payload["minute"],
            client_id=payload["client_id"],
            lookback_days=payload["lookback_days"],
            workflow_base=payload.get("workflow_base") or default_workflow_base,
            pipeline_start_date=payload.get("pipeline_start_date"),
            pipeline_include_portfolio=bool(payload.get("pipeline_include_portfolio", True)),
            created_at=now,
            updated_at=now,
        )
        session.add(schedule)
        session.commit()
        session.refresh(schedule)
        return _schedule_to_dict(schedule)


def update_schedule(db_path: Path, schedule_id: int, payload: dict[str, Any]) -> dict[str, Any] | None:
    if not payload:
        return get_schedule(db_path, schedule_id)
    allowed = {
        "name",
        "schedule_type",
        "enabled",
        "timezone",
        "day_of_week",
        "hour",
        "minute",
        "client_id",
        "lookback_days",
        "workflow_base",
        "pipeline_start_date",
        "pipeline_include_portfolio",
    }
    with _session_for_db(db_path) as session:
        schedule = session.get(Schedule, schedule_id)
        if schedule is None:
            return None
        changed = False
        for key, value in payload.items():
            if key not in allowed or value is None:
                continue
            if key in {"enabled", "pipeline_include_portfolio"}:
                value = bool(value)
            setattr(schedule, key, value)
            changed = True
        if not changed:
            return _schedule_to_dict(schedule)
        schedule.updated_at = dt.datetime.now(dt.timezone.utc).isoformat()
        session.commit()
        session.refresh(schedule)
        return _schedule_to_dict(schedule)


def delete_schedule(db_path: Path, schedule_id: int) -> bool:
    with _session_for_db(db_path) as session:
        schedule = session.get(Schedule, schedule_id)
        if schedule is None:
            return False
        session.delete(schedule)
        session.commit()
        return True
