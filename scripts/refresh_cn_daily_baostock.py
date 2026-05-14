#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import subprocess
import sys
from pathlib import Path

import yaml

from ib_qlib_pipeline.cn_data.qlib_exporter import export_baostock_raw_to_qlib_csv
from ib_qlib_pipeline.cn_data.symbols import qlib_symbol_to_baostock
from ib_qlib_pipeline.cn_data.baostock_downloader import download_baostock_daily


def status(message: str) -> None:
    print(message, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh CN A-share daily data via BaoStock and rebuild qlib bin.")
    parser.add_argument("--config", default="config_cn.yaml", help="Path to CN config YAML")
    parser.add_argument("--start-date", default=None, help="Refresh raw data from YYYY-MM-DD; defaults to config data.start_date")
    parser.add_argument("--end-date", default=dt.date.today().isoformat(), help="Refresh through YYYY-MM-DD")
    parser.add_argument("--adjustflag", default="2", help="BaoStock adjustflag, default 2")
    parser.add_argument("--sleep", type=float, default=0.2, help="Sleep seconds between symbol requests")
    parser.add_argument("--max-symbols", type=int, default=None, help="Only refresh first N symbols")
    parser.add_argument("--keep-suspended", action="store_true", help="Keep suspended rows during qlib CSV export")
    parser.add_argument("--keep-zero-volume", action="store_true", help="Keep zero-volume rows during qlib CSV export")
    parser.add_argument("--force", action="store_true", help="Force redownload full history for each symbol")
    return parser.parse_args()


def _resolve_path(project_root: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (project_root / path).resolve()


def _load_config(config_path: Path) -> dict:
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if "data" not in raw or "output" not in raw or "qlib" not in raw:
        raise RuntimeError(f"Invalid config file: {config_path}")
    return raw


def _symbol_map_lines(symbols_file: Path) -> list[str]:
    symbols: list[str] = []
    for raw in symbols_file.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        symbols.append(line.split(",")[0].strip().upper())
    return [f"{symbol},{qlib_symbol_to_baostock(symbol)}" for symbol in symbols]


def _materialize_symbol_map(project_root: Path, symbols_file: Path) -> Path:
    downloads_dir = project_root / "symbols" / "cn" / "downloads"
    downloads_dir.mkdir(parents=True, exist_ok=True)
    map_path = downloads_dir / f"{symbols_file.stem}_baostock_map.txt"
    map_path.write_text("\n".join(_symbol_map_lines(symbols_file)) + "\n", encoding="utf-8")
    return map_path


def _run_dump_bin(*, qlib_python_bin: Path, qlib_repo_path: Path, qlib_csv_dir: Path, qlib_bin_dir: Path) -> None:
    command = [
        str(qlib_python_bin),
        str((qlib_repo_path / "scripts" / "dump_bin.py").resolve()),
        "dump_all",
        "--data_path",
        str(qlib_csv_dir),
        "--qlib_dir",
        str(qlib_bin_dir),
        "--freq",
        "day",
        "--date_field_name",
        "date",
        "--symbol_field_name",
        "symbol",
        "--include_fields",
        "open,close,high,low,volume,factor",
        "--file_suffix",
        ".csv",
    ]
    status("[run] " + " ".join(command))
    subprocess.run(command, check=True)


def main() -> None:
    args = parse_args()
    project_root = Path(".").resolve()
    config_path = _resolve_path(project_root, args.config)
    raw = _load_config(config_path)

    data_cfg = raw["data"]
    output_cfg = raw["output"]
    qlib_cfg = raw["qlib"]

    symbols_file = _resolve_path(project_root, data_cfg["symbols_file"])
    raw_dir = _resolve_path(project_root, output_cfg["raw_prices_dir"])
    qlib_csv_dir = _resolve_path(project_root, output_cfg["qlib_csv_dir"])
    qlib_bin_dir = _resolve_path(project_root, output_cfg["qlib_bin_dir"])
    qlib_repo_path = _resolve_path(project_root, qlib_cfg["qlib_repo_path"])
    qlib_python_bin = _resolve_path(project_root, qlib_cfg["python_bin"])

    metadata_path = project_root / "symbols" / "cn" / "csi800_metadata.csv"
    failed_file = project_root / "symbols" / "cn" / "failed_symbols.txt"
    symbol_map_path = _materialize_symbol_map(project_root, symbols_file)
    start_date = args.start_date or str(data_cfg.get("start_date") or "2016-01-01")

    status(f"[info] config={config_path}")
    status(f"[info] symbols={symbols_file}")
    status(f"[info] raw_dir={raw_dir}")
    status(f"[info] qlib_csv_dir={qlib_csv_dir}")
    status(f"[info] qlib_bin_dir={qlib_bin_dir}")

    results = download_baostock_daily(
        symbol_map_path=symbol_map_path,
        raw_dir=raw_dir,
        metadata_path=metadata_path,
        failed_file=failed_file,
        start_date=start_date,
        end_date=args.end_date,
        adjustflag=args.adjustflag,
        sleep_seconds=args.sleep,
        force=args.force,
        max_symbols=args.max_symbols,
        progress=status,
    )
    summary: dict[str, int] = {}
    for row in results:
        summary[str(row.get("status"))] = summary.get(str(row.get("status")), 0) + 1
    status(f"[ok] download summary={summary}")

    export_rows = export_baostock_raw_to_qlib_csv(
        raw_dir=raw_dir,
        out_dir=qlib_csv_dir,
        metadata_path=metadata_path,
        failed_file=failed_file,
        start_date=start_date,
        drop_suspended=not args.keep_suspended,
        drop_zero_volume=not args.keep_zero_volume,
        max_symbols=args.max_symbols,
        progress=status,
    )
    ok = sum(1 for row in export_rows if row.get("status") == "ok")
    empty = sum(1 for row in export_rows if row.get("status") == "empty")
    failed = sum(1 for row in export_rows if row.get("status") == "failed")
    status(f"[ok] export summary ok={ok} empty={empty} failed={failed}")

    _run_dump_bin(
        qlib_python_bin=qlib_python_bin,
        qlib_repo_path=qlib_repo_path,
        qlib_csv_dir=qlib_csv_dir,
        qlib_bin_dir=qlib_bin_dir,
    )
    status("[ok] CN refresh completed")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        print(f"[error] {exc}", file=sys.stderr, flush=True)
        raise
