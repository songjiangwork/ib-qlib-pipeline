#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from ib_qlib_pipeline.cn_data.qlib_exporter import export_baostock_raw_to_qlib_csv


def status(message: str) -> None:
    print(message, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export BaoStock raw A-share daily CSV files to Qlib CSV format.")
    parser.add_argument("--raw-dir", default="data/cn/raw/baostock/daily", help="Input raw directory")
    parser.add_argument("--out-dir", default="data/cn/processed/qlib_csv", help="Output qlib CSV directory")
    parser.add_argument("--metadata-file", default="symbols/cn/csi800_metadata.csv", help="Metadata CSV path")
    parser.add_argument("--failed-file", default="symbols/cn/failed_symbols.txt", help="Failed symbol output path")
    parser.add_argument("--start-date", default="2016-01-01", help="Minimum export date")
    parser.add_argument("--drop-suspended", dest="drop_suspended", action="store_true", default=True)
    parser.add_argument("--keep-suspended", dest="drop_suspended", action="store_false")
    parser.add_argument("--drop-zero-volume", dest="drop_zero_volume", action="store_true", default=True)
    parser.add_argument("--keep-zero-volume", dest="drop_zero_volume", action="store_false")
    parser.add_argument("--max-symbols", type=int, default=None, help="Only process first N raw files")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(".").resolve()
    rows = export_baostock_raw_to_qlib_csv(
        raw_dir=(project_root / args.raw_dir).resolve(),
        out_dir=(project_root / args.out_dir).resolve(),
        metadata_path=(project_root / args.metadata_file).resolve(),
        failed_file=(project_root / args.failed_file).resolve(),
        start_date=args.start_date,
        drop_suspended=bool(args.drop_suspended),
        drop_zero_volume=bool(args.drop_zero_volume),
        max_symbols=args.max_symbols,
        progress=status,
    )
    ok = sum(1 for row in rows if row.get("status") == "ok")
    empty = sum(1 for row in rows if row.get("status") == "empty")
    failed = sum(1 for row in rows if row.get("status") == "failed")
    status(f"[ok] export finished ok={ok} empty={empty} failed={failed}")
    status("[warn] Universe is current CSI800, not point-in-time historical CSI800")


if __name__ == "__main__":
    main()
