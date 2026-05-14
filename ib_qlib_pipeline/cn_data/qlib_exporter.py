from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pandas as pd

from .baostock_downloader import BAOSTOCK_FIELDS, load_metadata, write_failed_symbols, write_metadata


def export_baostock_raw_to_qlib_csv(
    *,
    raw_dir: Path,
    out_dir: Path,
    metadata_path: Path,
    failed_file: Path,
    start_date: str = "2016-01-01",
    drop_suspended: bool = True,
    drop_zero_volume: bool = True,
    max_symbols: int | None = None,
    progress: callable | None = None,
) -> list[dict[str, Any]]:
    raw_files = sorted(raw_dir.glob("*.csv"))
    if max_symbols is not None:
        raw_files = raw_files[: max(0, int(max_symbols))]
    out_dir.mkdir(parents=True, exist_ok=True)

    existing_metadata = load_metadata(metadata_path)
    updated_rows: list[dict[str, Any]] = []
    failed_rows: list[dict[str, Any]] = []
    start_day = pd.Timestamp(start_date).date()

    for index, raw_path in enumerate(raw_files, start=1):
        symbol = raw_path.stem.upper()
        if progress is not None:
            progress(f"[info] [{index}/{len(raw_files)}] exporting {symbol}")
        row = dict(existing_metadata.get(symbol, {}))
        row["symbol"] = symbol
        try:
            exported, stats = _export_single_raw_file(
                raw_path=raw_path,
                out_dir=out_dir,
                start_day=start_day,
                drop_suspended=drop_suspended,
                drop_zero_volume=drop_zero_volume,
            )
        except Exception as exc:  # noqa: BLE001
            row["status"] = "failed"
            row["error_msg"] = str(exc)
            failed_rows.append(row)
            updated_rows.append(row)
            continue

        row.update(stats)
        row["status"] = "ok" if exported else "empty"
        if not exported:
            failed_rows.append(row)
        updated_rows.append(row)

    merged_metadata = _merge_metadata(existing_metadata, updated_rows)
    rows = sorted(merged_metadata.values(), key=lambda item: item["symbol"])
    write_metadata(metadata_path, rows)
    write_failed_symbols(failed_file, rows)
    return rows


def _merge_metadata(
    existing: dict[str, dict[str, Any]],
    updates: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    merged = {key: dict(value) for key, value in existing.items()}
    for row in updates:
        merged[str(row["symbol"]).upper()] = row
    return merged


def _export_single_raw_file(
    *,
    raw_path: Path,
    out_dir: Path,
    start_day: dt.date,
    drop_suspended: bool,
    drop_zero_volume: bool,
) -> tuple[bool, dict[str, Any]]:
    frame = pd.read_csv(raw_path, dtype=str)
    for field in BAOSTOCK_FIELDS:
        if field not in frame.columns:
            frame[field] = ""
    frame = frame[BAOSTOCK_FIELDS].copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.dropna(subset=["date"])
    frame = frame.loc[frame["date"].dt.date >= start_day].copy()

    suspended_dropped = 0
    zero_volume_dropped = 0
    if drop_suspended:
        suspended_mask = frame["tradestatus"].astype(str).ne("1")
        suspended_dropped = int(suspended_mask.sum())
        frame = frame.loc[~suspended_mask].copy()
    if drop_zero_volume:
        volume_series = pd.to_numeric(frame["volume"], errors="coerce").fillna(0.0)
        zero_mask = volume_series.le(0.0)
        zero_volume_dropped = int(zero_mask.sum())
        frame = frame.loc[~zero_mask].copy()

    for column in ["open", "high", "low", "close", "volume"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["open", "high", "low", "close", "volume"])
    frame = frame.drop_duplicates(subset=["date"], keep="last")
    frame = frame.sort_values("date").reset_index(drop=True)

    qlib = pd.DataFrame(
        {
            "date": frame["date"].dt.strftime("%Y-%m-%d"),
            "symbol": raw_path.stem.upper(),
            "open": frame["open"].astype(float),
            "high": frame["high"].astype(float),
            "low": frame["low"].astype(float),
            "close": frame["close"].astype(float),
            "volume": frame["volume"].astype(float),
            "factor": 1.0,
        }
    )

    out_path = out_dir / raw_path.name
    if qlib.empty:
        if out_path.exists():
            out_path.unlink()
        return False, {
            "first_qlib_date": "",
            "last_qlib_date": "",
            "qlib_rows": 0,
            "dropped_suspended_rows": suspended_dropped,
            "dropped_zero_volume_rows": zero_volume_dropped,
        }

    qlib.to_csv(out_path, index=False)
    return True, {
        "first_qlib_date": str(qlib.iloc[0]["date"]),
        "last_qlib_date": str(qlib.iloc[-1]["date"]),
        "qlib_rows": int(len(qlib)),
        "dropped_suspended_rows": suspended_dropped,
        "dropped_zero_volume_rows": zero_volume_dropped,
    }
