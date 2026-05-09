from __future__ import annotations

import csv
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from ..dborm.models import PriceDaily
from ..dborm.session import create_engine_for_path, create_session_factory_for_path
from .price_provider import DailySqlitePriceProvider
from ..webapi.universe_store import get_universe_by_key, list_universe_symbols


def default_daily_db_path(project_root: Path) -> Path:
    return project_root / "data" / "market" / "market_daily.sqlite3"


def init_daily_market_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine_for_path(db_path)
    PriceDaily.__table__.create(bind=engine, checkfirst=True)


def _upsert_rows(db_path: Path, rows: list[dict[str, object]]) -> int:
    if not rows:
        return 0
    session_factory = create_session_factory_for_path(db_path)
    session = session_factory()
    try:
        stmt = sqlite_insert(PriceDaily).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["symbol", "trade_date"],
            set_={
                "open": stmt.excluded.open,
                "high": stmt.excluded.high,
                "low": stmt.excluded.low,
                "close": stmt.excluded.close,
                "volume": stmt.excluded.volume,
                "factor": stmt.excluded.factor,
                "source": stmt.excluded.source,
                "updated_at": stmt.excluded.updated_at,
            },
        )
        session.execute(stmt)
        session.commit()
        return len(rows)
    finally:
        session.close()


def import_daily_csv_file(
    db_path: Path,
    csv_path: Path,
    *,
    symbol: str | None = None,
    source: str = "qlib_csv",
) -> int:
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    target_symbol = symbol or csv_path.stem
    rows: list[dict[str, object]] = []
    with csv_path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_symbol = str(row.get("symbol") or target_symbol).strip()
            if row_symbol != target_symbol:
                continue
            rows.append(
                {
                    "symbol": target_symbol,
                    "trade_date": str(row["date"]).strip(),
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row["volume"]) if row.get("volume") not in (None, "") else None,
                    "factor": float(row["factor"]) if row.get("factor") not in (None, "") else None,
                    "source": source,
                    "updated_at": now,
                }
            )
    return _upsert_rows(db_path, rows)


def import_daily_price_frame(
    db_path: Path,
    frame: pd.DataFrame,
    *,
    symbol: str,
    source: str = "pipeline",
) -> int:
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    normalized = frame.copy()
    if "symbol" not in normalized.columns:
        normalized["symbol"] = symbol
    if "high" not in normalized.columns:
        normalized["high"] = normalized["close"]
    if "low" not in normalized.columns:
        normalized["low"] = normalized["close"]
    if "volume" not in normalized.columns:
        normalized["volume"] = None
    if "factor" not in normalized.columns:
        normalized["factor"] = 1.0
    rows: list[dict[str, object]] = []
    for row in normalized.to_dict(orient="records"):
            row_symbol = str(row.get("symbol") or symbol).strip()
            if row_symbol != symbol:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "trade_date": str(row["date"]).strip(),
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row["volume"]) if row.get("volume") not in (None, "") else None,
                    "factor": float(row["factor"]) if row.get("factor") not in (None, "") else None,
                    "source": source,
                    "updated_at": now,
                }
            )
    return _upsert_rows(db_path, rows)


@dataclass
class DailyPriceStore:
    project_root: Path

    @property
    def db_path(self) -> Path:
        return default_daily_db_path(self.project_root)

    @property
    def provider(self) -> DailySqlitePriceProvider:
        return DailySqlitePriceProvider(self.db_path)

    def import_qlib_csv_directory(
        self,
        *,
        symbols: Iterable[str] | None = None,
        progress: Callable[[str, int], None] | None = None,
    ) -> int:
        init_daily_market_db(self.db_path)
        qlib_dir = self.project_root / "data" / "processed" / "qlib_csv"
        total = 0
        selected = set(symbols) if symbols is not None else None
        for csv_path in sorted(qlib_dir.glob("*.csv")):
            symbol = csv_path.stem
            if selected is not None and symbol not in selected:
                continue
            imported = import_daily_csv_file(self.db_path, csv_path, symbol=symbol)
            total += imported
            if progress is not None:
                progress(symbol, imported)
        return total

    def import_universe(
        self,
        service_db_path: Path,
        *,
        universe_key: str,
        progress: Callable[[str, int], None] | None = None,
    ) -> int:
        universe = get_universe_by_key(service_db_path, universe_key)
        if universe is None:
            raise RuntimeError(f"Universe not found: {universe_key}")
        rows = list_universe_symbols(service_db_path, int(universe["id"]))
        symbols = [str(row["symbol"]) for row in rows]
        return self.import_qlib_csv_directory(symbols=symbols, progress=progress)
