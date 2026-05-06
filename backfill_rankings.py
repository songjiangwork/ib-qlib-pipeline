#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

import pandas as pd
import yaml

from ib_qlib_pipeline.qlib_runtime import load_qlib_runtime_config
from ib_qlib_pipeline.ranking import TOP_N
from ib_qlib_pipeline.ranking.ranking_loader import (
    load_ranking_dataframe,
    read_available_trading_days,
)
from ib_qlib_pipeline.reporting.company_enricher import (
    cached_topn_details,
    fetch_topn_company_data,
)
from ib_qlib_pipeline.reporting.html_report import build_html_report
from ib_qlib_pipeline.runner.common import log
from ib_qlib_pipeline.runner.qlib_runner import run_qlib_workflow
from ib_qlib_pipeline.runner.runtime_workflow import build_backfill_runtime_workflow, load_base_workflow
from ib_qlib_pipeline.webapi.db import init_db
from ib_qlib_pipeline.webapi.model_store import (
    ensure_default_models,
    get_model,
    resolve_or_create_model_for_workflow,
)
from ib_qlib_pipeline.webapi.run_store import insert_completed_run
from ib_qlib_pipeline.webapi.settings import Settings


def status(message: str) -> None:
    print(message, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill daily ranking CSV/HTML outputs and insert historical runs into SQLite.",
    )
    parser.add_argument("--start-date", default="2025-01-01", help="Backfill start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", default=None, help="Backfill end date in YYYY-MM-DD format; defaults to latest trading date")
    parser.add_argument("--client-id", type=int, default=151, help="IB client id used for HTML enrichment fallback")
    parser.add_argument("--workflow-base", default="examples/workflow_us_lgb_2020_port.yaml", help="Base workflow yaml path")
    parser.add_argument("--model-id", type=int, default=None, help="Model reference id in SQLite; defaults to workflow-derived model")
    parser.add_argument("--skip-existing-db", action="store_true", help="Skip dates that already exist in the runs table")
    parser.add_argument("--skip-existing-files", action="store_true", help="Skip dates whose CSV already exists under reports/rankings")
    parser.add_argument("--max-days", type=int, default=None, help="Optional limit on number of trading days to process")
    parser.add_argument(
        "--html-mode",
        choices=("cached", "ib"),
        default="cached",
        help="HTML enrichment mode: cached avoids live IB calls; ib reuses the live enrichment path",
    )
    return parser.parse_args()


def existing_signal_dates(db_path: Path, model_id: int) -> set[str]:
    init_db(db_path)
    import sqlite3

    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT signal_date
            FROM runs
            WHERE signal_date IS NOT NULL
              AND model_id = ?
            """,
            (model_id,),
        ).fetchall()
        return {str(row[0]) for row in rows}
    finally:
        conn.close()


def backfill_csv_path(project_root: Path, model_key: str, signal_date: dt.date) -> Path:
    out_dir = project_root / "reports" / "rankings"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"sp500_ranking_{model_key}_{signal_date.isoformat()}.csv"


def backfill_html_path(project_root: Path, model_key: str, signal_date: dt.date) -> Path:
    out_dir = project_root / "reports" / "rankings"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"sp500_ranking_{model_key}_{signal_date.isoformat()}.html"


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parent
    settings = Settings.load()
    init_db(settings.db_path)
    ensure_default_models(settings.db_path)
    runtime_cfg = load_qlib_runtime_config(project_root)
    qlib_qrun = runtime_cfg.qlib_qrun_bin
    if not qlib_qrun.exists():
        raise SystemExit(f"Missing qrun: {qlib_qrun}")

    start_date = dt.date.fromisoformat(args.start_date)
    trading_days = read_available_trading_days(project_root)
    end_date = dt.date.fromisoformat(args.end_date) if args.end_date else trading_days[-1]
    selected_days = [day for day in trading_days if start_date <= day <= end_date]
    if args.max_days is not None:
        selected_days = selected_days[: args.max_days]
    if not selected_days:
        raise SystemExit("No trading days found in the selected date range")

    base_workflow = load_base_workflow(project_root, args.workflow_base)
    model_id = (
        args.model_id
        if args.model_id is not None
        else resolve_or_create_model_for_workflow(settings.db_path, project_root, args.workflow_base)
    )
    model = get_model(settings.db_path, model_id)
    if model is None:
        raise SystemExit(f"Model id not found: {model_id}")
    known_dates = existing_signal_dates(settings.db_path, model_id) if args.skip_existing_db else set()

    status(
        f"[info] backfill range: {selected_days[0]} -> {selected_days[-1]} ({len(selected_days)} trading days)"
    )
    status(
        f"[info] model_id={model_id} model={model['name']} class={model['model_class']} workflow={args.workflow_base}"
    )
    for index, trade_date in enumerate(selected_days, start=1):
        trade_date_str = trade_date.isoformat()
        csv_path = backfill_csv_path(project_root, str(model["key"]), trade_date)
        html_path = backfill_html_path(project_root, str(model["key"]), trade_date)

        if args.skip_existing_db and trade_date_str in known_dates:
            status(f"[skip] {trade_date_str} already exists in DB")
            continue
        if args.skip_existing_files and csv_path.exists():
            status(f"[skip] {trade_date_str} CSV already exists")
            continue

        console_lines: list[str] = []
        log(f"[info] [{index}/{len(selected_days)}] backfilling {trade_date_str}", console_lines)

        backtest_end = trade_date
        prior_days = [day for day in trading_days if day < trade_date]
        if prior_days:
            backtest_end = prior_days[-1]
        runtime_wf, experiment_name = build_backfill_runtime_workflow(
            runtime_cfg=runtime_cfg,
            workflow_base=args.workflow_base,
            model_key=str(model["key"]),
            trade_date=trade_date,
            backtest_end=backtest_end,
            base_workflow=base_workflow,
        )
        log(f"[ok] runtime workflow: {runtime_wf}", console_lines)
        log(f"[ok] target_trade_date={trade_date_str} backtest_end={backtest_end.isoformat()}", console_lines)
        log("[ok] backfill mode disabled PortAnaRecord for faster, benchmark-free replay", console_lines)
        log(f"[ok] experiment_name={experiment_name}", console_lines)

        pred_path, exp_id, rec_id = run_qlib_workflow(
            runtime_cfg=runtime_cfg,
            runtime_workflow_path=runtime_wf,
            experiment_name=experiment_name,
            cwd=project_root,
            console_lines=console_lines,
        )
        log(f"[ok] pred={pred_path}", console_lines)
        log(f"[ok] experiment_id={exp_id} recorder_id={rec_id}", console_lines)

        ranking_df = load_ranking_dataframe(project_root, pred_path, exp_id, rec_id)
        ranking_df["run_date"] = pd.to_datetime(trade_date)
        ranking_df["signal_date"] = pd.to_datetime(trade_date)
        ranking_df.to_csv(csv_path, index=False)
        log(f"[ok] ranking exported: {csv_path}", console_lines)
        log(
            f"[ok] signal_date={trade_date_str} rows={len(ranking_df)} missing_close={int(ranking_df['close'].isna().sum())}",
            console_lines,
        )

        topn_details = (
            fetch_topn_company_data(project_root, ranking_df.head(TOP_N), args.client_id, console_lines)
            if args.html_mode == "ib"
            else cached_topn_details(project_root, ranking_df, args.client_id, console_lines)
        )
        build_html_report(ranking_df, topn_details, html_path, console_lines)
        log(f"[ok] html report exported: {html_path}", console_lines)

        timestamp = dt.datetime.combine(trade_date, dt.time(16, 0, tzinfo=dt.timezone.utc)).isoformat()
        run_id = insert_completed_run(
            db_path=settings.db_path,
            model_id=model_id,
            trigger_source="backfill",
            client_id=args.client_id,
            lookback_days=0,
            workflow_base=args.workflow_base,
            command=(
                f"python backfill_rankings.py --start-date {trade_date_str} --end-date {trade_date_str} "
                f"--workflow-base {args.workflow_base} --model-id {model_id}"
            ),
            ranking_df=ranking_df,
            ranking_csv_path=csv_path,
            html_report_path=html_path,
            experiment_id=exp_id,
            recorder_id=rec_id,
            log_output="\n".join(console_lines),
            created_at=timestamp,
            started_at=timestamp,
            finished_at=timestamp,
        )
        status(f"[ok] inserted run_id={run_id} signal_date={trade_date_str}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
