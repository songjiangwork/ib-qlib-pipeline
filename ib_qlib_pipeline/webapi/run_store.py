from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import select

from ..dborm.models import ModelRef, Recommendation, Run, Schedule
from ..dborm.session import create_session_factory_for_path


def ranking_df_to_rows(ranking_df: pd.DataFrame) -> list[dict[str, Any]]:
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


def insert_completed_run(
    *,
    db_path: Path,
    model_id: int,
    trigger_source: str,
    client_id: int,
    lookback_days: int,
    workflow_base: str,
    command: str,
    ranking_df: pd.DataFrame,
    ranking_csv_path: Path,
    html_report_path: Path | None,
    experiment_id: str,
    recorder_id: str,
    log_output: str,
    schedule_id: int | None = None,
    created_at: str | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
) -> int:
    recommendations = ranking_df_to_rows(ranking_df)
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    created_at = created_at or now
    started_at = started_at or created_at
    finished_at = finished_at or created_at
    signal_date = pd.to_datetime(ranking_df["signal_date"]).dt.date.iloc[0].isoformat()

    session_factory = create_session_factory_for_path(db_path)
    session = session_factory()
    try:
        run = Run(
            schedule_id=schedule_id,
            model_id=model_id,
            trigger_source=trigger_source,
            status="succeeded",
            client_id=client_id,
            lookback_days=lookback_days,
            workflow_base=workflow_base,
            command=command,
            created_at=created_at,
            started_at=started_at,
            finished_at=finished_at,
            signal_date=signal_date,
            ranking_csv_path=str(ranking_csv_path),
            html_report_path=str(html_report_path) if html_report_path is not None else None,
            experiment_id=experiment_id,
            recorder_id=recorder_id,
            row_count=len(recommendations),
            log_output=log_output,
            error_text=None,
        )
        session.add(run)
        session.flush()

        session.add_all(
            [
                Recommendation(
                    run_id=int(run.id),
                    rank=int(row["rank"]),
                    symbol=str(row["symbol"]),
                    score=float(row["score"]),
                    percentile=float(row["percentile"]) if row["percentile"] is not None else None,
                    entry_price=float(row["entry_price"]) if row["entry_price"] is not None else None,
                    signal_date=str(row["signal_date"]),
                )
                for row in recommendations
            ]
        )
        session.commit()
        return int(run.id)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def list_runs(
    db_path: Path,
    *,
    limit: int = 50,
    status: str | None = None,
    signal_date: str | None = None,
) -> list[dict[str, Any]]:
    session_factory = create_session_factory_for_path(db_path)
    session = session_factory()
    try:
        query = (
            select(
                Run,
                Schedule.name.label("schedule_name"),
                ModelRef.key.label("model_key"),
                ModelRef.name.label("model_name"),
                ModelRef.model_class.label("model_class"),
                ModelRef.module_path.label("model_module_path"),
            )
            .select_from(Run)
            .outerjoin(Schedule, Schedule.id == Run.schedule_id)
            .outerjoin(ModelRef, ModelRef.id == Run.model_id)
        )
        if status is not None:
            query = query.where(Run.status == status)
        if signal_date is not None:
            query = query.where(Run.signal_date == signal_date)
        query = query.order_by(Run.started_at.desc().nullslast(), Run.created_at.desc()).limit(limit)
        rows = session.execute(query).all()
        items: list[dict[str, Any]] = []
        for run, schedule_name, model_key, model_name, model_class, model_module_path in rows:
            item = {
                "id": run.id,
                "schedule_id": run.schedule_id,
                "model_id": run.model_id,
                "trigger_source": run.trigger_source,
                "status": run.status,
                "client_id": run.client_id,
                "lookback_days": run.lookback_days,
                "workflow_base": run.workflow_base,
                "command": run.command,
                "created_at": run.created_at,
                "started_at": run.started_at,
                "finished_at": run.finished_at,
                "signal_date": run.signal_date,
                "ranking_csv_path": run.ranking_csv_path,
                "html_report_path": run.html_report_path,
                "experiment_id": run.experiment_id,
                "recorder_id": run.recorder_id,
                "row_count": run.row_count,
                "log_output": run.log_output,
                "error_text": run.error_text,
                "schedule_name": schedule_name,
                "model_key": model_key,
                "model_name": model_name,
                "model_class": model_class,
                "model_module_path": model_module_path,
            }
            items.append(item)
        return items
    finally:
        session.close()


def get_run(db_path: Path, run_id: int) -> dict[str, Any] | None:
    session_factory = create_session_factory_for_path(db_path)
    session = session_factory()
    try:
        row = session.execute(
            select(
                Run,
                Schedule.name.label("schedule_name"),
                ModelRef.key.label("model_key"),
                ModelRef.name.label("model_name"),
                ModelRef.model_class.label("model_class"),
                ModelRef.module_path.label("model_module_path"),
            )
            .select_from(Run)
            .outerjoin(Schedule, Schedule.id == Run.schedule_id)
            .outerjoin(ModelRef, ModelRef.id == Run.model_id)
            .where(Run.id == run_id)
        ).first()
        if row is None:
            return None
        run, schedule_name, model_key, model_name, model_class, model_module_path = row
        return {
            "id": run.id,
            "schedule_id": run.schedule_id,
            "model_id": run.model_id,
            "trigger_source": run.trigger_source,
            "status": run.status,
            "client_id": run.client_id,
            "lookback_days": run.lookback_days,
            "workflow_base": run.workflow_base,
            "command": run.command,
            "created_at": run.created_at,
            "started_at": run.started_at,
            "finished_at": run.finished_at,
            "signal_date": run.signal_date,
            "ranking_csv_path": run.ranking_csv_path,
            "html_report_path": run.html_report_path,
            "experiment_id": run.experiment_id,
            "recorder_id": run.recorder_id,
            "row_count": run.row_count,
            "log_output": run.log_output,
            "error_text": run.error_text,
            "schedule_name": schedule_name,
            "model_key": model_key,
            "model_name": model_name,
            "model_class": model_class,
            "model_module_path": model_module_path,
        }
    finally:
        session.close()


def get_run_recommendations(db_path: Path, run_id: int) -> list[dict[str, Any]]:
    session_factory = create_session_factory_for_path(db_path)
    session = session_factory()
    try:
        rows = session.execute(
            select(Recommendation)
            .where(Recommendation.run_id == run_id)
            .order_by(Recommendation.rank)
        ).scalars().all()
        return [
            {
                "rank": row.rank,
                "symbol": row.symbol,
                "score": row.score,
                "percentile": row.percentile,
                "entry_price": row.entry_price,
                "signal_date": row.signal_date,
            }
            for row in rows
        ]
    finally:
        session.close()


def list_ranking_dates(
    db_path: Path,
    *,
    limit: int = 20,
    offset: int = 0,
    query: str | None = None,
    model_id: int | None = None,
) -> dict[str, Any]:
    session_factory = create_session_factory_for_path(db_path)
    session = session_factory()
    try:
        base_query = (
            select(
                Run.id.label("run_id"),
                Run.signal_date,
                Run.row_count,
                ModelRef.id.label("model_id"),
                ModelRef.key.label("model_key"),
                ModelRef.name.label("model_name"),
            )
            .select_from(Run)
            .outerjoin(ModelRef, ModelRef.id == Run.model_id)
            .where(Run.trigger_source == "backfill", Run.signal_date.is_not(None))
        )
        count_query = select(Run.id).where(Run.trigger_source == "backfill", Run.signal_date.is_not(None))
        if query:
            like_value = f"%{query}%"
            base_query = base_query.where(Run.signal_date.like(like_value))
            count_query = count_query.where(Run.signal_date.like(like_value))
        if model_id is not None:
            base_query = base_query.where(Run.model_id == model_id)
            count_query = count_query.where(Run.model_id == model_id)

        total = len(session.execute(count_query).all())
        rows = session.execute(
            base_query.order_by(Run.signal_date.desc()).limit(limit).offset(offset)
        ).all()
        items = [dict(row._mapping) for row in rows]
        next_offset = offset + len(items)
        return {
            "items": items,
            "total": total,
            "has_more": next_offset < total,
            "next_offset": next_offset,
        }
    finally:
        session.close()
