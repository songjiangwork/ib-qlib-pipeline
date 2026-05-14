from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Mapping

import pandas as pd

from ib_qlib_pipeline.webapi.price_store import load_close_lookup
from ib_qlib_pipeline.qlib_runtime import load_qlib_runtime_config
from ib_qlib_pipeline.runner.common import log


def read_available_trading_days(project_root: Path, config_path: Path | None = None) -> list[dt.date]:
    runtime_cfg = load_qlib_runtime_config(project_root, config_path=config_path)
    cal_path = runtime_cfg.qlib_bin_dir / "calendars" / "day.txt"
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


def load_ranking_dataframe(
    project_root: Path,
    pred_path: Path,
    exp_id: str,
    rec_id: str,
    config_path: Path | None = None,
) -> pd.DataFrame:
    pred_df = read_prediction_dataframe(pred_path)
    signal_date = pred_df["signal_datetime"].max().date()
    return build_ranking_for_signal_date(
        pred_df=pred_df,
        signal_date=signal_date,
        exp_id=exp_id,
        rec_id=rec_id,
        project_root=project_root,
    )


def read_prediction_dataframe(pred_path: Path) -> pd.DataFrame:
    df = pd.read_pickle(pred_path)
    d = df.reset_index() if isinstance(df.index, pd.MultiIndex) else df.copy()
    date_col = "datetime" if "datetime" in d.columns else "date"
    inst_col = "instrument" if "instrument" in d.columns else "symbol"
    score_col = "score" if "score" in d.columns else d.select_dtypes("number").columns[-1]

    standardized = d[[date_col, inst_col, score_col]].copy()
    standardized.columns = ["signal_datetime", "symbol", "score"]
    standardized["signal_datetime"] = pd.to_datetime(standardized["signal_datetime"])
    standardized["symbol"] = standardized["symbol"].astype(str)
    standardized["score"] = pd.to_numeric(standardized["score"], errors="coerce")
    return standardized[["signal_datetime", "symbol", "score"]]


def build_ranking_for_signal_date(
    *,
    pred_df: pd.DataFrame,
    signal_date: dt.date,
    exp_id: str,
    rec_id: str,
    close_lookup: Mapping[tuple[str, dt.date], float] | None = None,
    project_root: Path | None = None,
) -> pd.DataFrame:
    cur = pred_df.loc[pred_df["signal_datetime"].dt.date == signal_date, ["symbol", "score"]].copy()
    if cur.empty:
        raise ValueError(f"No prediction rows found for signal_date={signal_date.isoformat()}")
    cur = cur.sort_values("score", ascending=False).reset_index(drop=True)
    cur["rank"] = cur.index + 1
    cur["percentile"] = cur["score"].rank(pct=True, ascending=True) * 100

    if close_lookup is None:
        if project_root is None:
            raise ValueError("project_root is required when close_lookup is not provided")
        close_lookup = _load_close_lookup(project_root=project_root, symbols=cur["symbol"].tolist(), signal_date=signal_date)
    cur["close"] = [float(close_lookup.get((str(sym), signal_date), float("nan"))) for sym in cur["symbol"]]

    run_date = dt.date.today()
    cur["run_date"] = pd.to_datetime(run_date)
    cur["signal_date"] = pd.to_datetime(signal_date)
    cur["experiment_id"] = exp_id
    cur["recorder_id"] = rec_id
    return cur[
        ["run_date", "signal_date", "rank", "symbol", "score", "percentile", "close", "experiment_id", "recorder_id"]
    ]


def _load_close_lookup(*, project_root: Path, symbols: list[str], signal_date: dt.date) -> dict[tuple[str, dt.date], float]:
    try:
        runtime_cfg = load_qlib_runtime_config(project_root)
    except FileNotFoundError:
        runtime_cfg = None
    return load_close_lookup(
        project_root,
        symbols=symbols,
        signal_dates=[signal_date],
        qlib_csv_dir=runtime_cfg.qlib_csv_dir if runtime_cfg is not None else None,
    )


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
