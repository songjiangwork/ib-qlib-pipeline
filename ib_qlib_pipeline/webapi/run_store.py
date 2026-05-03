from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pandas as pd

from .db import connect


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

    with connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO runs (
                schedule_id, model_id, trigger_source, status, client_id, lookback_days,
                workflow_base, command, created_at, started_at, finished_at,
                signal_date, ranking_csv_path, html_report_path, experiment_id,
                recorder_id, row_count, log_output, error_text
            ) VALUES (?, ?, ?, 'succeeded', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                schedule_id,
                model_id,
                trigger_source,
                client_id,
                lookback_days,
                workflow_base,
                command,
                created_at,
                started_at,
                finished_at,
                signal_date,
                str(ranking_csv_path),
                str(html_report_path) if html_report_path is not None else None,
                experiment_id,
                recorder_id,
                len(recommendations),
                log_output,
            ),
        )
        run_id = int(cursor.lastrowid)
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
    return run_id
