#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

import pandas as pd

from ib_qlib_pipeline.qlib_runtime import load_qlib_runtime_config
from ib_qlib_pipeline.ranking import TOP_N
from ib_qlib_pipeline.ranking.ranking_loader import (
    build_ranking_for_signal_date,
    read_available_trading_days,
    read_prediction_dataframe,
)
from ib_qlib_pipeline.reporting.company_enricher import cached_topn_details, fetch_topn_company_data
from ib_qlib_pipeline.reporting.html_report import build_html_report
from ib_qlib_pipeline.runner.common import log
from ib_qlib_pipeline.runner.manifest import build_manifest_path, build_run_artifact_manifest
from ib_qlib_pipeline.runner.qlib_runner import run_qlib_workflow
from ib_qlib_pipeline.runner.runtime_workflow import build_bulk_backfill_runtime_workflow, load_base_workflow
from ib_qlib_pipeline.webapi.db import init_db
from ib_qlib_pipeline.webapi.model_store import ensure_default_models, get_model, resolve_or_create_model_for_workflow
from ib_qlib_pipeline.webapi.price_store import load_close_lookup
from ib_qlib_pipeline.webapi.run_store import insert_completed_run
from ib_qlib_pipeline.webapi.settings import Settings


def status(message: str) -> None:
    print(message, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bulk backfill historical rankings using one qrun per date range and per-date exports.",
    )
    parser.add_argument("--start-date", default="2025-01-01", help="Backfill start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", default=None, help="Backfill end date in YYYY-MM-DD format; defaults to latest trading date")
    parser.add_argument("--client-id", type=int, default=151, help="IB client id used for HTML enrichment fallback")
    parser.add_argument("--config", default="config.yaml", help="Config path used to resolve qlib data and IB settings")
    parser.add_argument("--workflow-base", default="examples/workflow_us_lgb_2020_port.yaml", help="Base workflow yaml path")
    parser.add_argument("--model-id", type=int, default=None, help="Model reference id in SQLite; defaults to workflow-derived model")
    parser.add_argument("--skip-existing-db", action="store_true", help="Skip dates that already exist in the runs table")
    parser.add_argument("--skip-existing-files", action="store_true", help="Skip dates whose CSV already exists under reports/rankings")
    parser.add_argument("--manifest-dir", default="reports/manifests", help="Directory for per-signal manifest JSON files")
    parser.add_argument("--max-days", type=int, default=None, help="Optional limit on number of trading days to process")
    parser.add_argument(
        "--html-mode",
        choices=("cached", "ib"),
        default="cached",
        help="HTML enrichment mode: cached avoids live IB calls; ib reuses the live enrichment path",
    )
    return parser.parse_args()


def existing_signal_dates(db_path: Path, model_id: int) -> set[str]:
    import sqlite3

    init_db(db_path)
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
    config_path = (project_root / args.config).resolve()
    settings = Settings.load()
    init_db(settings.db_path)
    ensure_default_models(settings.db_path)
    runtime_cfg = load_qlib_runtime_config(project_root, config_path=config_path)
    qlib_qrun = runtime_cfg.qlib_qrun_bin
    if not qlib_qrun.exists():
        raise SystemExit(f"Missing qrun: {qlib_qrun}")

    start_date = dt.date.fromisoformat(args.start_date)
    trading_days = read_available_trading_days(project_root, config_path=config_path)
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

    pending_days: list[dt.date] = []
    for trade_date in selected_days:
        trade_date_str = trade_date.isoformat()
        csv_path = backfill_csv_path(project_root, str(model["key"]), trade_date)
        if args.skip_existing_db and trade_date_str in known_dates:
            status(f"[skip] {trade_date_str} already exists in DB")
            continue
        if args.skip_existing_files and csv_path.exists():
            status(f"[skip] {trade_date_str} CSV already exists")
            continue
        pending_days.append(trade_date)

    if not pending_days:
        status("[ok] nothing to backfill after skip filters")
        return

    bulk_start = min(pending_days)
    bulk_end = max(pending_days)
    prior_days = [day for day in trading_days if day < bulk_end]
    backtest_end = prior_days[-1] if prior_days else bulk_end

    console_lines: list[str] = []
    log(
        f"[info] bulk backfill range: {bulk_start.isoformat()} -> {bulk_end.isoformat()} "
        f"({len(pending_days)} selected trading days, {len(selected_days)} requested)",
        console_lines,
    )
    log(
        f"[info] model_id={model_id} model={model['name']} class={model['model_class']} workflow={args.workflow_base}",
        console_lines,
    )

    runtime_wf, experiment_name = build_bulk_backfill_runtime_workflow(
        runtime_cfg=runtime_cfg,
        workflow_base=args.workflow_base,
        model_key=str(model["key"]),
        start_date=bulk_start,
        end_date=bulk_end,
        backtest_end=backtest_end,
        base_workflow=base_workflow,
    )
    log(f"[ok] bulk runtime workflow: {runtime_wf}", console_lines)
    log(f"[ok] bulk window={bulk_start.isoformat()}..{bulk_end.isoformat()} backtest_end={backtest_end.isoformat()}", console_lines)
    log("[ok] bulk backfill mode disabled PortAnaRecord for faster, benchmark-free replay", console_lines)
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

    pred_df = read_prediction_dataframe(pred_path)
    log(f"[ok] prediction rows loaded={len(pred_df)} dates={pred_df['signal_datetime'].dt.date.nunique()}", console_lines)
    symbols = pred_df["symbol"].dropna().astype(str).unique().tolist()
    close_cache = load_close_lookup(project_root, symbols=symbols, signal_dates=pending_days)
    close_cache = load_close_lookup(
        project_root,
        symbols=symbols,
        signal_dates=pending_days,
        qlib_csv_dir=runtime_cfg.qlib_csv_dir,
    )
    log(f"[ok] close cache loaded entries={len(close_cache)} symbols={len(symbols)} dates={len(pending_days)}", console_lines)
    manifest_root = (project_root / args.manifest_dir).resolve()
    manifest_stamp = dt.datetime.now(dt.timezone.utc)

    for index, trade_date in enumerate(pending_days, start=1):
        trade_date_str = trade_date.isoformat()
        csv_path = backfill_csv_path(project_root, str(model["key"]), trade_date)
        html_path = backfill_html_path(project_root, str(model["key"]), trade_date)

        day_console_lines = list(console_lines)
        log(f"[info] [{index}/{len(pending_days)}] exporting {trade_date_str}", day_console_lines)
        ranking_df = build_ranking_for_signal_date(
            pred_df=pred_df,
            signal_date=trade_date,
            exp_id=exp_id,
            rec_id=rec_id,
            close_lookup=close_cache,
        )
        ranking_df["run_date"] = pd.to_datetime(trade_date)
        ranking_df["signal_date"] = pd.to_datetime(trade_date)
        ranking_df.to_csv(csv_path, index=False)
        log(f"[ok] ranking exported: {csv_path}", day_console_lines)
        log(
            f"[ok] signal_date={trade_date_str} rows={len(ranking_df)} missing_close={int(ranking_df['close'].isna().sum())}",
            day_console_lines,
        )

        topn_details = (
            fetch_topn_company_data(project_root, ranking_df.head(TOP_N), args.client_id, day_console_lines, config_path=config_path)
            if args.html_mode == "ib"
            else cached_topn_details(project_root, ranking_df, args.client_id, day_console_lines, config_path=config_path)
        )
        build_html_report(ranking_df, topn_details, html_path, day_console_lines)
        log(f"[ok] html report exported: {html_path}", day_console_lines)
        manifest_obj, manifest_path = build_run_artifact_manifest(
            project_root=project_root,
            manifest_path=build_manifest_path(
                project_root,
                job_type="backfill",
                model_key=str(model["key"]),
                date_token=trade_date_str,
                created_at=manifest_stamp,
            ).resolve()
            if args.manifest_dir == "reports/manifests"
            else (
                manifest_root
                / build_manifest_path(
                    project_root,
                    job_type="backfill",
                    model_key=str(model["key"]),
                    date_token=trade_date_str,
                    created_at=manifest_stamp,
                ).name
            ),
            job_type="backfill_ranking",
            status="succeeded",
            model_id=int(model["id"]),
            model_key=str(model["key"]),
            model_name=str(model["name"]),
            universe_id=int(model["universe_id"]) if model.get("universe_id") is not None else None,
            universe_key=str(model["universe_key"]) if model.get("universe_key") else None,
            signal_date=trade_date_str,
            start_date=trade_date_str,
            end_date=trade_date_str,
            ranking_csv_path=csv_path,
            html_report_path=html_path,
            experiment_name=experiment_name,
            experiment_id=exp_id,
            recorder_id=rec_id,
            pred_path=pred_path,
            workflow_base=args.workflow_base,
            runtime_workflow_path=runtime_wf,
            config_path=config_path,
            qlib_csv_dir=runtime_cfg.qlib_csv_dir,
            qlib_bin_dir=runtime_cfg.qlib_bin_dir,
            created_at=dt.datetime.combine(trade_date, dt.time(16, 0, tzinfo=dt.timezone.utc)).isoformat(),
            finished_at=dt.datetime.now(dt.timezone.utc).isoformat(),
            extra={
                "backfill_mode": "bulk",
                "bulk_start_date": bulk_start.isoformat(),
                "bulk_end_date": bulk_end.isoformat(),
                "backtest_end": backtest_end.isoformat(),
                "html_mode": args.html_mode,
                "row_count": len(ranking_df),
                "missing_close": int(ranking_df["close"].isna().sum()),
            },
        )
        manifest_obj.write_json(manifest_path)
        log(f"[ok] manifest exported: {manifest_path}", day_console_lines)
        try:
            manifest_path_for_db = manifest_path.resolve().relative_to(project_root.resolve())
        except ValueError:
            manifest_path_for_db = manifest_path.resolve()

        timestamp = dt.datetime.combine(trade_date, dt.time(16, 0, tzinfo=dt.timezone.utc)).isoformat()
        run_id = insert_completed_run(
            db_path=settings.db_path,
            model_id=model_id,
            trigger_source="bulk_backfill",
            client_id=args.client_id,
            lookback_days=0,
            workflow_base=args.workflow_base,
            command=(
                f"python backfill_rankings_bulk.py --config {args.config} "
                f"--start-date {bulk_start.isoformat()} --end-date {bulk_end.isoformat()} "
                f"--workflow-base {args.workflow_base} --model-id {model_id}"
            ),
            ranking_df=ranking_df,
            ranking_csv_path=csv_path,
            html_report_path=html_path,
            manifest_path=manifest_path_for_db,
            experiment_id=exp_id,
            recorder_id=rec_id,
            log_output="\n".join(day_console_lines),
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
