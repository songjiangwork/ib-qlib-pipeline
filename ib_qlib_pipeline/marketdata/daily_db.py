from __future__ import annotations

import csv
import datetime as dt
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .price_provider import DailySqlitePriceProvider
from ..webapi.universe_store import get_universe_by_key, list_universe_symbols


SCHEMA = """
CREATE TABLE IF NOT EXISTS prices_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL,
    factor REAL,
    source TEXT,
    updated_at TEXT NOT NULL,
    UNIQUE(symbol, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_prices_daily_symbol_date ON prices_daily(symbol, trade_date);
CREATE INDEX IF NOT EXISTS idx_prices_daily_trade_date ON prices_daily(trade_date);
"""


def default_daily_db_path(project_root: Path) -> Path:
    return project_root / "data" / "market" / "market_daily.sqlite3"


def init_daily_market_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(SCHEMA)


def import_daily_csv_file(
    db_path: Path,
    csv_path: Path,
    *,
    symbol: str | None = None,
    source: str = "qlib_csv",
) -> int:
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    target_symbol = symbol or csv_path.stem
    inserted = 0
    with csv_path.open(encoding="utf-8", newline="") as f, sqlite3.connect(str(db_path)) as conn:
        reader = csv.DictReader(f)
        for row in reader:
            row_symbol = str(row.get("symbol") or target_symbol).strip()
            if row_symbol != target_symbol:
                continue
            conn.execute(
                """
                INSERT INTO prices_daily (
                    symbol, trade_date, open, high, low, close, volume, factor, source, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, trade_date) DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    factor=excluded.factor,
                    source=excluded.source,
                    updated_at=excluded.updated_at
                """,
                (
                    target_symbol,
                    str(row["date"]).strip(),
                    float(row["open"]),
                    float(row["high"]),
                    float(row["low"]),
                    float(row["close"]),
                    float(row["volume"]) if row.get("volume") not in (None, "") else None,
                    float(row["factor"]) if row.get("factor") not in (None, "") else None,
                    source,
                    now,
                ),
            )
            inserted += 1
        conn.commit()
    return inserted


@dataclass
class DailyPriceStore:
    project_root: Path

    @property
    def db_path(self) -> Path:
        return default_daily_db_path(self.project_root)

    @property
    def provider(self) -> DailySqlitePriceProvider:
        return DailySqlitePriceProvider(self.db_path)

    def import_qlib_csv_directory(self, *, symbols: Iterable[str] | None = None) -> int:
        init_daily_market_db(self.db_path)
        qlib_dir = self.project_root / "data" / "processed" / "qlib_csv"
        total = 0
        selected = set(symbols) if symbols is not None else None
        for csv_path in sorted(qlib_dir.glob("*.csv")):
            symbol = csv_path.stem
            if selected is not None and symbol not in selected:
                continue
            total += import_daily_csv_file(self.db_path, csv_path, symbol=symbol)
        return total

    def import_universe(self, service_db_path: Path, *, universe_key: str) -> int:
        universe = get_universe_by_key(service_db_path, universe_key)
        if universe is None:
            raise RuntimeError(f"Universe not found: {universe_key}")
        rows = list_universe_symbols(service_db_path, int(universe["id"]))
        symbols = [str(row["symbol"]) for row in rows]
        return self.import_qlib_csv_directory(symbols=symbols)
