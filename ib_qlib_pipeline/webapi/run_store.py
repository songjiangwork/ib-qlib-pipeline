from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import func, select

from ..dborm.models import Instrument, ModelRef, Recommendation, Run, Schedule, Universe
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
        model = session.get(ModelRef, model_id)
        run = Run(
            schedule_id=schedule_id,
            model_id=model_id,
            universe_id=int(model.universe_id) if model is not None and model.universe_id is not None else None,
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


def create_queued_run(
    *,
    db_path: Path,
    schedule_id: int | None,
    model_id: int,
    trigger_source: str,
    client_id: int,
    lookback_days: int,
    workflow_base: str,
    command: str,
    created_at: str | None = None,
) -> int:
    created_at = created_at or dt.datetime.now(dt.timezone.utc).isoformat()
    session_factory = create_session_factory_for_path(db_path)
    session = session_factory()
    try:
        model = session.get(ModelRef, model_id)
        run = Run(
            schedule_id=schedule_id,
            model_id=model_id,
            universe_id=int(model.universe_id) if model is not None and model.universe_id is not None else None,
            trigger_source=trigger_source,
            status="queued",
            client_id=client_id,
            lookback_days=lookback_days,
            workflow_base=workflow_base,
            command=command,
            created_at=created_at,
        )
        session.add(run)
        session.commit()
        session.refresh(run)
        return int(run.id)
    finally:
        session.close()


def mark_run_running(
    db_path: Path,
    run_id: int,
    *,
    started_at: str,
) -> None:
    session_factory = create_session_factory_for_path(db_path)
    session = session_factory()
    try:
        run = session.get(Run, run_id)
        if run is None:
            return
        run.status = "running"
        run.started_at = started_at
        session.commit()
    finally:
        session.close()


def finalize_succeeded_run(
    *,
    db_path: Path,
    run_id: int,
    ranking_df: pd.DataFrame,
    signal_date: str,
    ranking_csv_path: str,
    html_report_path: str | None,
    experiment_id: str,
    recorder_id: str,
    log_output: str,
    finished_at: str | None = None,
) -> None:
    finished_at = finished_at or dt.datetime.now(dt.timezone.utc).isoformat()
    recommendations = ranking_df_to_rows(ranking_df)
    session_factory = create_session_factory_for_path(db_path)
    session = session_factory()
    try:
        run = session.get(Run, run_id)
        if run is None:
            return
        run.status = "succeeded"
        run.finished_at = finished_at
        run.signal_date = signal_date
        run.ranking_csv_path = ranking_csv_path
        run.html_report_path = html_report_path
        run.experiment_id = experiment_id
        run.recorder_id = recorder_id
        run.row_count = len(recommendations)
        run.log_output = log_output
        run.error_text = None

        session.query(Recommendation).filter(Recommendation.run_id == run_id).delete()
        session.add_all(
            [
                Recommendation(
                    run_id=run_id,
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
                Universe.key.label("universe_key"),
                Universe.name.label("universe_name"),
            )
            .select_from(Run)
            .outerjoin(Schedule, Schedule.id == Run.schedule_id)
            .outerjoin(ModelRef, ModelRef.id == Run.model_id)
            .outerjoin(Universe, Universe.id == Run.universe_id)
        )
        if status is not None:
            query = query.where(Run.status == status)
        if signal_date is not None:
            query = query.where(Run.signal_date == signal_date)
        query = query.order_by(Run.started_at.desc().nullslast(), Run.created_at.desc()).limit(limit)
        rows = session.execute(query).all()
        items: list[dict[str, Any]] = []
        for run, schedule_name, model_key, model_name, model_class, model_module_path, universe_key, universe_name in rows:
            item = {
                "id": run.id,
                "schedule_id": run.schedule_id,
                "model_id": run.model_id,
                "universe_id": run.universe_id,
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
                "universe_key": universe_key,
                "universe_name": universe_name,
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
                Universe.key.label("universe_key"),
                Universe.name.label("universe_name"),
            )
            .select_from(Run)
            .outerjoin(Schedule, Schedule.id == Run.schedule_id)
            .outerjoin(ModelRef, ModelRef.id == Run.model_id)
            .outerjoin(Universe, Universe.id == Run.universe_id)
            .where(Run.id == run_id)
        ).first()
        if row is None:
            return None
        run, schedule_name, model_key, model_name, model_class, model_module_path, universe_key, universe_name = row
        return {
            "id": run.id,
            "schedule_id": run.schedule_id,
            "model_id": run.model_id,
            "universe_id": run.universe_id,
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
            "universe_key": universe_key,
            "universe_name": universe_name,
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
        symbols = sorted({str(row.symbol).strip().upper() for row in rows if row.symbol})
        instrument_map: dict[str, Instrument] = {}
        if symbols:
            instrument_rows = session.execute(
                select(Instrument).where(Instrument.canonical_symbol.in_(symbols))
            ).scalars().all()
            instrument_map = {str(item.canonical_symbol): item for item in instrument_rows}
        return [
            {
                "rank": row.rank,
                "symbol": row.symbol,
                "display_name": (
                    (
                        instrument_map[str(row.symbol).strip().upper()].name_zh
                        or instrument_map[str(row.symbol).strip().upper()].name_en
                        or instrument_map[str(row.symbol).strip().upper()].display_symbol
                    )
                    if str(row.symbol).strip().upper() in instrument_map
                    else None
                ),
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
        ranking_sources = ("backfill", "bulk_backfill")
        base_query = (
            select(
                Run.id.label("run_id"),
                Run.signal_date,
                Run.row_count,
                ModelRef.id.label("model_id"),
                ModelRef.key.label("model_key"),
                ModelRef.name.label("model_name"),
                Universe.id.label("universe_id"),
                Universe.key.label("universe_key"),
                Universe.name.label("universe_name"),
            )
            .select_from(Run)
            .outerjoin(ModelRef, ModelRef.id == Run.model_id)
            .outerjoin(Universe, Universe.id == Run.universe_id)
            .where(Run.trigger_source.in_(ranking_sources), Run.signal_date.is_not(None))
        )
        count_query = select(Run.id).where(Run.trigger_source.in_(ranking_sources), Run.signal_date.is_not(None))
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


def latest_backfill_signal_date_for_model(db_path: Path, model_id: int) -> dt.date | None:
    session_factory = create_session_factory_for_path(db_path)
    session = session_factory()
    try:
        ranking_sources = ("backfill", "bulk_backfill")
        latest_signal = session.execute(
            select(Run.signal_date)
            .where(
                Run.status == "succeeded",
                Run.trigger_source.in_(ranking_sources),
                Run.model_id == model_id,
                Run.signal_date.is_not(None),
            )
            .order_by(Run.signal_date.desc())
            .limit(1)
        ).scalar_one_or_none()
        if not latest_signal:
            return None
        return dt.date.fromisoformat(str(latest_signal))
    finally:
        session.close()


def load_signal_map(
    db_path: Path,
    *,
    start_date: str,
    end_date: str | None,
    model_id: int | None,
) -> dict[str, dict[str, Any]]:
    session_factory = create_session_factory_for_path(db_path)
    session = session_factory()
    try:
        end_date_sql = end_date or "9999-12-31"
        latest_ids = select(
            Run.signal_date.label("signal_date"),
            func.max(Run.id).label("run_id"),
        ).where(
            Run.status == "succeeded",
            Run.signal_date.is_not(None),
            Run.signal_date.between(start_date, end_date_sql),
        )
        if model_id is not None:
            latest_ids = latest_ids.where(Run.model_id == model_id)
        latest_ids = latest_ids.group_by(Run.signal_date).subquery()

        rows = session.execute(
            select(
                Run.id.label("run_id"),
                Run.model_id.label("model_id"),
                Run.signal_date.label("signal_date"),
                Recommendation.rank.label("rank"),
                Recommendation.symbol.label("symbol"),
            )
            .join(latest_ids, latest_ids.c.run_id == Run.id)
            .join(Recommendation, Recommendation.run_id == Run.id)
            .where(Run.signal_date.between(start_date, end_date_sql))
            .order_by(Run.signal_date, Recommendation.rank)
        ).all()
        signal_map: dict[str, dict[str, Any]] = {}
        for run_id, run_model_id, signal_date, rank, symbol in rows:
            bucket = signal_map.setdefault(
                str(signal_date),
                {
                    "run_id": int(run_id),
                    "model_id": int(run_model_id) if run_model_id is not None else None,
                    "top20": [],
                    "rank_map": {},
                },
            )
            if int(rank) <= 20:
                bucket["top20"].append(str(symbol))
            bucket["rank_map"][str(symbol)] = int(rank)
        return signal_map
    finally:
        session.close()


def mark_run_failed(
    db_path: Path,
    run_id: int,
    *,
    finished_at: str,
    error_text: str,
) -> None:
    session_factory = create_session_factory_for_path(db_path)
    session = session_factory()
    try:
        run = session.get(Run, run_id)
        if run is None:
            return
        run.status = "failed"
        run.finished_at = finished_at
        run.error_text = error_text
        run.log_output = run.log_output or error_text
        session.commit()
    finally:
        session.close()


def backfill_legacy_run_models(
    db_path: Path,
    *,
    default_model_id: int,
) -> int:
    session_factory = create_session_factory_for_path(db_path)
    session = session_factory()
    try:
        count = (
            session.query(Run)
            .filter(Run.model_id.is_(None))
            .update({Run.model_id: default_model_id}, synchronize_session=False)
        )
        session.commit()
        return int(count or 0)
    finally:
        session.close()
