from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pandas as pd

from ..dborm.models import Recommendation, Run
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
