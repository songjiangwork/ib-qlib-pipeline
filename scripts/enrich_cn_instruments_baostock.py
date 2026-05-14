#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from ib_qlib_pipeline.cn_data.instrument_enricher import enrich_cn_instruments_from_baostock
from ib_qlib_pipeline.webapi.db import init_db
from ib_qlib_pipeline.webapi.settings import Settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enrich A-share instrument static metadata from BaoStock")
    parser.add_argument("--symbols", default=None, help="Optional Qlib symbol list or BaoStock map file to limit enrichment")
    parser.add_argument("--db", default=None, help="Optional path to ranking_service.db")
    parser.add_argument("--dry-run", action="store_true", help="Parse and summarize without writing DB")
    parser.add_argument("--max-symbols", type=int, default=None, help="Optional limit for testing")
    parser.add_argument("--source", default="baostock", choices=("baostock",), help="Reserved for future extensibility")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = Settings.load()
    db_path = Path(args.db).resolve() if args.db else settings.db_path
    init_db(db_path)
    symbols_path = Path(args.symbols).resolve() if args.symbols else None

    import baostock as bs  # type: ignore[import-not-found]

    lg = bs.login()
    if str(getattr(lg, "error_code", "")) != "0":
        raise SystemExit(f"BaoStock login failed: {getattr(lg, 'error_msg', '')}")
    try:
        summary = enrich_cn_instruments_from_baostock(
            db_path=db_path,
            bs_module=bs,
            symbols_path=symbols_path,
            dry_run=args.dry_run,
            max_symbols=args.max_symbols,
        )
    finally:
        bs.logout()

    print(summary, flush=True)


if __name__ == "__main__":
    main()
