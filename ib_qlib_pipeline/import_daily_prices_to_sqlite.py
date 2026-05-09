from __future__ import annotations

import argparse
from pathlib import Path

from ib_qlib_pipeline.marketdata.daily_db import DailyPriceStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import daily qlib_csv price files into market_daily.sqlite3")
    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Project root containing data/ and symbols/",
    )
    parser.add_argument(
        "--service-db-path",
        default=None,
        help="Path to ranking service DB used for universe lookup (defaults to data/app/ranking_service.db under project root)",
    )
    parser.add_argument(
        "--universe-key",
        default=None,
        help="Only import symbols from the specified universe key",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated symbol list override",
    )
    return parser


def run(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    project_root = Path(args.project_root).resolve()
    service_db_path = (
        Path(args.service_db_path).resolve()
        if args.service_db_path
        else (project_root / "data" / "app" / "ranking_service.db").resolve()
    )

    store = DailyPriceStore(project_root)
    if args.universe_key:
        imported = store.import_universe(service_db_path, universe_key=str(args.universe_key))
        print(f"[ok] imported {imported} rows for universe={args.universe_key} into {store.db_path}", flush=True)
        return 0
    if args.symbols:
        symbols = [part.strip() for part in str(args.symbols).split(",") if part.strip()]
        imported = store.import_qlib_csv_directory(symbols=symbols)
        print(f"[ok] imported {imported} rows for {len(symbols)} symbols into {store.db_path}", flush=True)
        return 0

    imported = store.import_qlib_csv_directory()
    print(f"[ok] imported {imported} rows for all qlib_csv symbols into {store.db_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
