from __future__ import annotations

import datetime as dt
from typing import Any


def build_operations_summary(
    *,
    models: list[dict[str, Any]],
    portfolio_runs: list[dict[str, Any]],
    runs: list[dict[str, Any]],
    trade_date: str,
    expected_portfolio_end_signal_date: str,
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for model in models:
        model_runs = [row for row in runs if row.get("model_id") == model["id"]]
        latest_ranking = next(
            (
                row
                for row in model_runs
                if row.get("trigger_source") == "backfill" and row.get("signal_date") is not None
            ),
            None,
        )
        portfolio_run = next((row for row in portfolio_runs if row.get("model_id") == model["id"]), None)
        ranking_signal_date = str(latest_ranking["signal_date"]) if latest_ranking else None
        portfolio_end = (
            str(portfolio_run["end_signal_date"])
            if portfolio_run and portfolio_run.get("end_signal_date")
            else None
        )
        items.append(
            {
                "model_id": model["id"],
                "model_key": model["key"],
                "model_name": model["name"],
                "workflow_base": model.get("workflow_base"),
                "latest_ranking_run_id": latest_ranking.get("id") if latest_ranking else None,
                "latest_ranking_signal_date": ranking_signal_date,
                "ranking_ready_for_trade_date": ranking_signal_date == trade_date,
                "portfolio_run_id": portfolio_run.get("id") if portfolio_run else None,
                "portfolio_run_name": portfolio_run.get("name") if portfolio_run else None,
                "portfolio_end_signal_date": portfolio_end,
                "portfolio_ready_for_trade_date": portfolio_end == expected_portfolio_end_signal_date,
                "expected_portfolio_end_signal_date": expected_portfolio_end_signal_date,
            }
        )
    return {
        "trade_date": trade_date,
        "expected_portfolio_end_signal_date": expected_portfolio_end_signal_date,
        "models": items,
    }


def build_scheduled_job_request(schedule: dict[str, Any], now_local_date: dt.date) -> dict[str, Any]:
    schedule_id = int(schedule["id"])
    schedule_type = str(schedule.get("schedule_type") or "ranking")
    if schedule_type == "daily_close_pipeline":
        pipeline_start_date = schedule.get("pipeline_start_date")
        if not pipeline_start_date:
            pipeline_start_date = (now_local_date - dt.timedelta(days=int(schedule["lookback_days"]))).isoformat()
        return {
            "job_type": "scheduled_daily_close_pipeline",
            "title": f"Scheduled daily close pipeline #{schedule_id} ({now_local_date.isoformat()})",
            "payload": {
                "schedule_id": schedule_id,
                "trade_date": now_local_date.isoformat(),
                "client_id": int(schedule["client_id"]),
                "start_date": str(pipeline_start_date),
                "include_portfolio": bool(schedule.get("pipeline_include_portfolio")),
            },
            "schedule_id": schedule_id,
            "target_name": "daily_close_pipeline",
        }

    return {
        "job_type": "scheduled_ranking_run",
        "title": f"Scheduled ranking run #{schedule_id} ({schedule['workflow_base']})",
        "payload": {
            "schedule_id": schedule_id,
            "trigger_source": "schedule",
            "client_id": int(schedule["client_id"]),
            "lookback_days": int(schedule["lookback_days"]),
            "workflow_base": str(schedule["workflow_base"]),
        },
        "schedule_id": schedule_id,
        "target_name": "ranking_run",
    }
