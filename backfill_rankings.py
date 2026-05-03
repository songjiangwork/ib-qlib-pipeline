#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from pathlib import Path

import pandas as pd
import yaml

from ib_qlib_pipeline.webapi.db import init_db
from ib_qlib_pipeline.webapi.run_store import insert_completed_run
from ib_qlib_pipeline.webapi.settings import Settings
from oneclick_daily_ranking import (
    TOP_N,
    _build_price_stats,
    _load_company_meta_cache,
    build_html_report,
    fetch_topn_company_data,
    find_latest_pred,
    load_ranking_dataframe,
    log,
    read_available_trading_days,
    run_cmd,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill daily ranking CSV/HTML outputs and insert historical runs into SQLite.",
    )
    parser.add_argument("--start-date", default="2025-01-01", help="Backfill start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", default=None, help="Backfill end date in YYYY-MM-DD format; defaults to latest trading date")
    parser.add_argument("--client-id", type=int, default=151, help="IB client id used for HTML enrichment fallback")
    parser.add_argument("--workflow-base", default="examples/workflow_us_lgb_2020_port.yaml", help="Base workflow yaml path")
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


def existing_signal_dates(db_path: Path) -> set[str]:
    init_db(db_path)
    import sqlite3

    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute("SELECT DISTINCT signal_date FROM runs WHERE signal_date IS NOT NULL").fetchall()
        return {str(row[0]) for row in rows}
    finally:
        conn.close()


def find_latest_pred_after(project_root: Path, min_mtime: float) -> tuple[Path, str, str]:
    preds = []
    for path in (project_root / "mlruns").glob("*/*/artifacts/pred.pkl"):
        mtime = path.stat().st_mtime
        if mtime >= min_mtime:
            preds.append((mtime, path))
    if not preds:
        return find_latest_pred(project_root)
    _, pred_path = max(preds, key=lambda item: item[0])
    parts = pred_path.parts
    exp_id = parts[-4]
    rec_id = parts[-3]
    return pred_path, exp_id, rec_id


def backfill_csv_path(project_root: Path, signal_date: dt.date) -> Path:
    out_dir = project_root / "reports" / "rankings"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"sp500_ranking_{signal_date.isoformat()}.csv"


def backfill_html_path(project_root: Path, signal_date: dt.date) -> Path:
    out_dir = project_root / "reports" / "rankings"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"sp500_ranking_{signal_date.isoformat()}.html"


def cached_topn_details(project_root: Path, ranking_df: pd.DataFrame, client_id: int, console_lines: list[str]) -> list[dict]:
    rows: list[dict] = []
    log(f"[info] building cached html data for top{TOP_N} without live IB calls", console_lines)
    for row in ranking_df.head(TOP_N).itertuples(index=False):
        symbol = str(row.symbol)
        cached = _load_company_meta_cache(project_root, symbol)
        rows.append(
            {
                "rank": int(row.rank),
                "symbol": symbol,
                "score": float(row.score),
                "percentile": float(row.percentile),
                "close": None if pd.isna(row.close) else float(row.close),
                "metadata": {
                    "company_name": cached.get("longName", "") or symbol,
                    "industry": cached.get("industry", "") or "N/A",
                    "sector": cached.get("sector", "") or "N/A",
                    "category": cached.get("category", "") or "N/A",
                    "market_name": cached.get("marketName", "") or "N/A",
                    "exchange": cached.get("exchange", "") or "N/A",
                    "currency": cached.get("currency", "") or "N/A",
                    "description": cached.get("description", "") or "N/A",
                    "pe": cached.get("pe", "") or "N/A",
                },
                "price_stats": _build_price_stats(project_root, symbol),
            }
        )
    return rows


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parent
    settings = Settings.load()
    init_db(settings.db_path)

    qlib_qrun = Path("/home/song/projects/qlib/.venv/bin/qrun")
    if not qlib_qrun.exists():
        raise SystemExit("Missing qrun: /home/song/projects/qlib/.venv/bin/qrun")

    start_date = dt.date.fromisoformat(args.start_date)
    trading_days = read_available_trading_days(project_root)
    end_date = dt.date.fromisoformat(args.end_date) if args.end_date else trading_days[-1]
    selected_days = [day for day in trading_days if start_date <= day <= end_date]
    if args.max_days is not None:
        selected_days = selected_days[: args.max_days]
    if not selected_days:
        raise SystemExit("No trading days found in the selected date range")

    known_dates = existing_signal_dates(settings.db_path) if args.skip_existing_db else set()
    base_path = project_root / args.workflow_base
    if not base_path.exists():
        raise SystemExit(f"Base workflow not found: {base_path}")
    base_workflow = yaml.safe_load(base_path.read_text(encoding="utf-8"))

    print(f"[info] backfill range: {selected_days[0]} -> {selected_days[-1]} ({len(selected_days)} trading days)")
    for index, trade_date in enumerate(selected_days, start=1):
        trade_date_str = trade_date.isoformat()
        csv_path = backfill_csv_path(project_root, trade_date)
        html_path = backfill_html_path(project_root, trade_date)

        if args.skip_existing_db and trade_date_str in known_dates:
            print(f"[skip] {trade_date_str} already exists in DB")
            continue
        if args.skip_existing_files and csv_path.exists():
            print(f"[skip] {trade_date_str} CSV already exists")
            continue

        console_lines: list[str] = []
        log(f"[info] [{index}/{len(selected_days)}] backfilling {trade_date_str}", console_lines)

        wf = yaml.safe_load(yaml.safe_dump(base_workflow, sort_keys=False, allow_unicode=False))
        wf["data_handler_config"]["end_time"] = trade_date_str
        wf["task"]["dataset"]["kwargs"]["segments"]["test"][1] = trade_date_str
        wf["task"]["record"] = [
            record
            for record in wf["task"]["record"]
            if str(record.get("class")) != "PortAnaRecord"
        ]

        backtest_end = trade_date
        prior_days = [day for day in trading_days if day < trade_date]
        if prior_days:
            backtest_end = prior_days[-1]
        wf["port_analysis_config"]["backtest"]["end_time"] = backtest_end.isoformat()

        tmp_dir = project_root / "reports" / "tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        runtime_wf = tmp_dir / f"workflow_backfill_{trade_date_str}.yaml"
        runtime_wf.write_text(yaml.safe_dump(wf, sort_keys=False, allow_unicode=False), encoding="utf-8")
        log(f"[ok] runtime workflow: {runtime_wf}", console_lines)
        log(f"[ok] target_trade_date={trade_date_str} backtest_end={backtest_end.isoformat()}", console_lines)
        log("[ok] backfill mode disabled PortAnaRecord for faster, benchmark-free replay", console_lines)

        qrun_env = os.environ.copy()
        qrun_env["GIT_DIR"] = str(qlib_qrun.parent.parent.parent / ".git")
        qrun_env["GIT_WORK_TREE"] = str(qlib_qrun.parent.parent.parent)
        before_mtime = dt.datetime.now().timestamp()
        run_cmd([str(qlib_qrun), str(runtime_wf)], cwd=project_root, console_lines=console_lines, env=qrun_env)

        pred_path, exp_id, rec_id = find_latest_pred_after(project_root, before_mtime)
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
            trigger_source="backfill",
            client_id=args.client_id,
            lookback_days=0,
            workflow_base=args.workflow_base,
            command=f"python backfill_rankings.py --start-date {trade_date_str} --end-date {trade_date_str}",
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
        print(f"[ok] inserted run_id={run_id} signal_date={trade_date_str}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
