from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd

from ib_qlib_pipeline.qlib_runtime import load_qlib_runtime_config
from ib_qlib_pipeline.runner.common import log


def read_available_trading_days(project_root: Path) -> list[dt.date]:
    runtime_cfg = load_qlib_runtime_config(project_root)
    cal_path = runtime_cfg.data_dir / "qlib" / "us_data_custom" / "calendars" / "day.txt"
    if not cal_path.exists():
        raise SystemExit(f"Missing qlib calendar: {cal_path}")
    days = [
        dt.date.fromisoformat(line.strip())
        for line in cal_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if len(days) < 2:
        raise SystemExit(f"Qlib calendar has insufficient dates: {cal_path}")
    return days


def load_ranking_dataframe(project_root: Path, pred_path: Path, exp_id: str, rec_id: str) -> pd.DataFrame:
    df = pd.read_pickle(pred_path)
    d = df.reset_index() if isinstance(df.index, pd.MultiIndex) else df.copy()

    date_col = "datetime" if "datetime" in d.columns else "date"
    inst_col = "instrument" if "instrument" in d.columns else "symbol"
    score_col = "score" if "score" in d.columns else d.select_dtypes("number").columns[-1]

    d[date_col] = pd.to_datetime(d[date_col])
    signal_date = d[date_col].max().date()
    cur = d[d[date_col].dt.date == signal_date][[inst_col, score_col]].copy()
    cur.columns = ["symbol", "score"]
    cur = cur.sort_values("score", ascending=False).reset_index(drop=True)
    cur["rank"] = cur.index + 1
    cur["percentile"] = cur["score"].rank(pct=True, ascending=True) * 100

    price_dir = load_qlib_runtime_config(project_root).data_dir / "processed" / "qlib_csv"
    close_vals = []
    for sym in cur["symbol"]:
        fp = price_dir / f"{sym}.csv"
        p = pd.read_csv(fp, usecols=["date", "close"])
        p["date"] = pd.to_datetime(p["date"]).dt.date
        hit = p.loc[p["date"] == signal_date, "close"]
        close_vals.append(float(hit.iloc[-1]) if len(hit) else float("nan"))
    cur["close"] = close_vals

    run_date = dt.date.today()
    cur["run_date"] = pd.to_datetime(run_date)
    cur["signal_date"] = pd.to_datetime(signal_date)
    cur["experiment_id"] = exp_id
    cur["recorder_id"] = rec_id
    return cur[
        ["run_date", "signal_date", "rank", "symbol", "score", "percentile", "close", "experiment_id", "recorder_id"]
    ]


def next_rank_file(out_dir: Path, run_date: dt.date) -> Path:
    base = out_dir / f"sp500_ranking_{run_date.isoformat()}.csv"
    if not base.exists():
        return base
    i = 1
    while True:
        candidate = out_dir / f"sp500_ranking_{run_date.isoformat()}-{i:02d}.csv"
        if not candidate.exists():
            return candidate
        i += 1


def export_ranking_csv(project_root: Path, ranking_df: pd.DataFrame, console_lines: list[str]) -> Path:
    out_dir = project_root / "reports" / "rankings"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_date = pd.to_datetime(ranking_df["run_date"].iloc[0]).date()
    out_file = next_rank_file(out_dir, run_date)
    ranking_df.to_csv(out_file, index=False)

    signal_date = pd.to_datetime(ranking_df["signal_date"].iloc[0]).date()
    log(f"[ok] ranking exported: {out_file}", console_lines)
    log(
        f"[ok] signal_date={signal_date} rows={len(ranking_df)} missing_close={int(ranking_df['close'].isna().sum())}",
        console_lines,
    )
    return out_file
