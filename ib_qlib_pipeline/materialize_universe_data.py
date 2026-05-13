#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from .pipeline import _load_symbols, _run_dump_bin, load_config


def status(message: str) -> None:
    print(message, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Materialize universe-specific qlib_csv/qlib_bin data from shared raw daily price files.",
    )
    parser.add_argument("--config", required=True, help="Path to YAML config, e.g. config_union.yaml")
    parser.add_argument("--dump-bin", action="store_true", help="Also build qlib bin after qlib_csv materialization")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[1]
    cfg_path = (project_root / args.config).resolve()
    cfg = load_config(cfg_path)

    symbols_path = (project_root / cfg.data.symbols_file).resolve()
    symbols = _load_symbols(symbols_path)
    raw_prices_dir = (project_root / cfg.output.raw_prices_dir).resolve()
    qlib_csv_dir = (project_root / cfg.output.qlib_csv_dir).resolve()
    qlib_bin_dir = (project_root / cfg.output.qlib_bin_dir).resolve()

    qlib_csv_dir.mkdir(parents=True, exist_ok=True)
    qlib_bin_dir.mkdir(parents=True, exist_ok=True)

    copied = 0
    missing = 0
    status(f"[info] config={cfg_path}")
    status(f"[info] symbols_file={symbols_path}")
    status(f"[info] raw_prices_dir={raw_prices_dir}")
    status(f"[info] qlib_csv_dir={qlib_csv_dir}")

    for qlib_symbol, _ib_symbol in symbols:
        raw_path = raw_prices_dir / f"{qlib_symbol}.csv"
        qlib_path = qlib_csv_dir / f"{qlib_symbol}.csv"
        if not raw_path.exists():
            missing += 1
            status(f"[warn] missing raw price file: {raw_path}")
            continue
        shutil.copyfile(raw_path, qlib_path)
        copied += 1
        status(f"[info] materialized qlib csv: {qlib_path}")

    status(f"[ok] materialized {copied} symbols into {qlib_csv_dir} (missing={missing})")

    if args.dump_bin:
        status("[info] running qlib dump_bin...")
        _run_dump_bin(cfg, qlib_csv_dir, qlib_bin_dir)
        status(f"[ok] qlib bin generated: {qlib_bin_dir}")


if __name__ == "__main__":
    main()
