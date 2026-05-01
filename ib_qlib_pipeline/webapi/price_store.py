from __future__ import annotations

import datetime as dt
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd


@lru_cache(maxsize=2048)
def _load_price_frame(project_root_str: str, symbol: str) -> pd.DataFrame:
    price_path = Path(project_root_str) / "data" / "processed" / "qlib_csv" / f"{symbol}.csv"
    if not price_path.exists():
        raise FileNotFoundError(f"Missing price file for {symbol}: {price_path}")
    frame = pd.read_csv(price_path, usecols=["date", "close"])
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    frame = frame.sort_values("date").reset_index(drop=True)
    return frame


def compute_forward_metrics(
    project_root: Path,
    symbol: str,
    entry_date: dt.date,
    entry_price: float | None,
    horizons: list[int],
) -> dict[str, dict[str, Any] | None]:
    try:
        frame = _load_price_frame(str(project_root), symbol)
    except FileNotFoundError:
        return {"latest": None, **{f"{h}d": None for h in horizons}}

    hit = frame.index[frame["date"] == entry_date]
    if len(hit) == 0:
        return {"latest": None, **{f"{h}d": None for h in horizons}}

    entry_idx = int(hit[0])
    base_price = float(entry_price) if entry_price is not None else float(frame.iloc[entry_idx]["close"])

    def build_metric(target_idx: int) -> dict[str, Any] | None:
        if target_idx >= len(frame):
            return None
        target = frame.iloc[target_idx]
        future_price = float(target["close"])
        return {
            "date": target["date"].isoformat(),
            "price": future_price,
            "change": future_price - base_price,
            "return_pct": ((future_price / base_price) - 1.0) * 100.0,
            "direction": "up" if future_price >= base_price else "down",
        }

    metrics: dict[str, dict[str, Any] | None] = {}
    for horizon in horizons:
        metrics[f"{horizon}d"] = build_metric(entry_idx + horizon)
    metrics["latest"] = build_metric(len(frame) - 1)
    return metrics


def summarize_performance(
    project_root: Path,
    recommendations: list[dict[str, Any]],
    horizons: list[int],
) -> dict[str, dict[str, Any]]:
    labels = [f"{h}d" for h in horizons] + ["latest"]
    buckets: dict[str, list[float]] = {label: [] for label in labels}

    for row in recommendations:
        metrics = compute_forward_metrics(
            project_root=project_root,
            symbol=str(row["symbol"]),
            entry_date=dt.date.fromisoformat(str(row["signal_date"])),
            entry_price=float(row["entry_price"]) if row["entry_price"] is not None else None,
            horizons=horizons,
        )
        row["performance"] = metrics
        for label in labels:
            metric = metrics.get(label)
            if metric is not None:
                buckets[label].append(float(metric["return_pct"]))

    summary: dict[str, dict[str, Any]] = {}
    for label, returns in buckets.items():
        if not returns:
            summary[label] = {"count": 0, "avg_return_pct": None, "win_rate_pct": None}
            continue
        wins = sum(1 for value in returns if value >= 0.0)
        summary[label] = {
            "count": len(returns),
            "avg_return_pct": sum(returns) / len(returns),
            "win_rate_pct": wins * 100.0 / len(returns),
        }
    return summary
