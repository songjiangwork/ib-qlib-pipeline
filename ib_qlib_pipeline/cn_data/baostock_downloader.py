from __future__ import annotations

import csv
import datetime as dt
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


BAOSTOCK_FIELDS = [
    "date",
    "code",
    "open",
    "high",
    "low",
    "close",
    "preclose",
    "volume",
    "amount",
    "adjustflag",
    "turn",
    "tradestatus",
    "pctChg",
    "isST",
]


@dataclass
class DownloadResult:
    symbol: str
    baostock_code: str
    status: str
    error_code: str = ""
    error_msg: str = ""
    first_raw_date: str = ""
    last_raw_date: str = ""
    raw_rows: int = 0
    downloaded_at: str = ""


def parse_symbol_map(path: Path) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = [part.strip() for part in line.split(",")]
        if len(parts) < 2:
            raise ValueError(f"Invalid symbol map line: {line}")
        rows.append((parts[0].upper(), parts[1].lower()))
    return rows


def load_metadata(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return {str(row.get("symbol", "")).strip().upper(): row for row in reader if row.get("symbol")}


def write_metadata(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "symbol",
        "baostock_code",
        "first_raw_date",
        "last_raw_date",
        "raw_rows",
        "status",
        "error_code",
        "error_msg",
        "downloaded_at",
        "first_qlib_date",
        "last_qlib_date",
        "qlib_rows",
        "dropped_suspended_rows",
        "dropped_zero_volume_rows",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def write_failed_symbols(path: Path, rows: list[dict[str, Any]]) -> None:
    failed = [row["symbol"] for row in rows if row.get("status") in {"failed", "empty"}]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(failed) + ("\n" if failed else ""), encoding="utf-8")


def _today_str() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _frame_summary(df: pd.DataFrame) -> tuple[str, str, int]:
    if df.empty:
        return "", "", 0
    ordered = df.sort_values("date")
    return str(ordered.iloc[0]["date"]), str(ordered.iloc[-1]["date"]), int(len(ordered))


def _read_existing_raw(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame(columns=BAOSTOCK_FIELDS)
    frame = pd.read_csv(path, dtype=str)
    for field in BAOSTOCK_FIELDS:
        if field not in frame.columns:
            frame[field] = ""
    return frame[BAOSTOCK_FIELDS].copy()


def _merge_raw_frames(existing: pd.DataFrame, fetched: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        out = fetched.copy()
    elif fetched.empty:
        out = existing.copy()
    else:
        out = pd.concat([existing, fetched], ignore_index=True)
    if out.empty:
        return out
    out["date"] = out["date"].astype(str)
    out = out.drop_duplicates(subset=["date"], keep="last")
    out = out.sort_values("date").reset_index(drop=True)
    return out


def _start_date_for_incremental(existing: pd.DataFrame, requested_start_date: str) -> str:
    if existing.empty:
        return requested_start_date
    last_date = pd.to_datetime(existing["date"]).max().date()
    return (last_date + dt.timedelta(days=1)).isoformat()


def _fetch_history_frame(
    *,
    bs_module: Any,
    baostock_code: str,
    start_date: str,
    end_date: str,
    adjustflag: str,
) -> tuple[pd.DataFrame, str, str]:
    rs = bs_module.query_history_k_data_plus(
        baostock_code,
        ",".join(BAOSTOCK_FIELDS),
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag=str(adjustflag),
    )
    error_code = str(getattr(rs, "error_code", ""))
    error_msg = str(getattr(rs, "error_msg", ""))
    if error_code != "0":
        return pd.DataFrame(columns=BAOSTOCK_FIELDS), error_code, error_msg
    rows: list[list[str]] = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    frame = pd.DataFrame(rows, columns=rs.fields if rows else BAOSTOCK_FIELDS)
    for field in BAOSTOCK_FIELDS:
        if field not in frame.columns:
            frame[field] = ""
    frame = frame[BAOSTOCK_FIELDS].copy()
    return frame, error_code, error_msg


def download_baostock_daily(
    *,
    symbol_map_path: Path,
    raw_dir: Path,
    metadata_path: Path,
    failed_file: Path,
    start_date: str,
    end_date: str,
    adjustflag: str = "2",
    sleep_seconds: float = 0.2,
    force: bool = False,
    max_symbols: int | None = None,
    bs_module: Any | None = None,
    progress: callable | None = None,
) -> list[dict[str, Any]]:
    if bs_module is None:
        import baostock as bs_module  # type: ignore[import-not-found]

    rows = parse_symbol_map(symbol_map_path)
    if max_symbols is not None:
        rows = rows[: max(0, int(max_symbols))]

    raw_dir.mkdir(parents=True, exist_ok=True)
    existing_metadata = load_metadata(metadata_path)
    results: list[dict[str, Any]] = []

    lg = bs_module.login()
    if str(getattr(lg, "error_code", "")) != "0":
        raise RuntimeError(f"BaoStock login failed: {getattr(lg, 'error_msg', '')}")

    try:
        for index, (symbol, baostock_code) in enumerate(rows, start=1):
            if progress is not None:
                progress(f"[info] [{index}/{len(rows)}] downloading {symbol} ({baostock_code})")
            raw_path = raw_dir / f"{symbol}.csv"
            existing = pd.DataFrame(columns=BAOSTOCK_FIELDS) if force else _read_existing_raw(raw_path)
            fetch_start_date = start_date if force else _start_date_for_incremental(existing, start_date)
            fetched, error_code, error_msg = _fetch_history_frame(
                bs_module=bs_module,
                baostock_code=baostock_code,
                start_date=fetch_start_date,
                end_date=end_date,
                adjustflag=adjustflag,
            )

            row = dict(existing_metadata.get(symbol, {}))
            row.update(
                {
                    "symbol": symbol,
                    "baostock_code": baostock_code,
                    "error_code": error_code,
                    "error_msg": error_msg,
                    "downloaded_at": _today_str(),
                }
            )

            if error_code != "0":
                row["status"] = "failed"
                results.append(row)
                continue

            if existing.empty and fetched.empty:
                row["status"] = "empty"
                row["first_raw_date"] = ""
                row["last_raw_date"] = ""
                row["raw_rows"] = 0
                results.append(row)
                time.sleep(sleep_seconds)
                continue

            if not existing.empty and fetched.empty:
                first_date, last_date, total_rows = _frame_summary(existing)
                row["status"] = "no_new_data"
                row["first_raw_date"] = first_date
                row["last_raw_date"] = last_date
                row["raw_rows"] = total_rows
                results.append(row)
                time.sleep(sleep_seconds)
                continue

            merged = _merge_raw_frames(existing, fetched)
            merged.to_csv(raw_path, index=False)
            first_date, last_date, total_rows = _frame_summary(merged)
            row["status"] = "ok"
            row["first_raw_date"] = first_date
            row["last_raw_date"] = last_date
            row["raw_rows"] = total_rows
            results.append(row)
            time.sleep(sleep_seconds)
    finally:
        bs_module.logout()

    results = sorted(results, key=lambda item: item["symbol"])
    write_metadata(metadata_path, results)
    write_failed_symbols(failed_file, results)
    return results
