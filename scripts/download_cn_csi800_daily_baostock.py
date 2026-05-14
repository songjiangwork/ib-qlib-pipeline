#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

from ib_qlib_pipeline.cn_data.baostock_downloader import download_baostock_daily


def status(message: str) -> None:
    print(message, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download CSI800 A-share daily OHLCV from BaoStock.")
    parser.add_argument("--symbols", default="symbols/cn/csi800_baostock_map.txt", help="Path to Qlib->BaoStock map file")
    parser.add_argument("--raw-dir", default="data/cn/raw/baostock/daily", help="Output raw CSV directory")
    parser.add_argument("--metadata-file", default="symbols/cn/csi800_metadata.csv", help="Metadata CSV path")
    parser.add_argument("--failed-file", default="symbols/cn/failed_symbols.txt", help="Failed symbol output path")
    parser.add_argument("--start-date", default="2016-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", default=dt.date.today().isoformat(), help="End date YYYY-MM-DD")
    parser.add_argument("--adjustflag", default="2", help="BaoStock adjustflag, default 2")
    parser.add_argument("--sleep", type=float, default=0.2, help="Sleep seconds between symbols")
    parser.add_argument("--force", action="store_true", help="Redownload and overwrite full history")
    parser.add_argument("--max-symbols", type=int, default=None, help="Only process first N symbols")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(".").resolve()
    results = download_baostock_daily(
        symbol_map_path=(project_root / args.symbols).resolve(),
        raw_dir=(project_root / args.raw_dir).resolve(),
        metadata_path=(project_root / args.metadata_file).resolve(),
        failed_file=(project_root / args.failed_file).resolve(),
        start_date=args.start_date,
        end_date=args.end_date,
        adjustflag=args.adjustflag,
        sleep_seconds=args.sleep,
        force=args.force,
        max_symbols=args.max_symbols,
        progress=status,
    )
    summary: dict[str, int] = {}
    for row in results:
        summary[row["status"]] = summary.get(row["status"], 0) + 1
    status(f"[ok] finished download total={len(results)} summary={summary}")
    status("[warn] Universe is current CSI800, not point-in-time historical CSI800")


if __name__ == "__main__":
    main()
