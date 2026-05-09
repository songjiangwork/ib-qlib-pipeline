from __future__ import annotations

import datetime as dt
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


class PriceProvider(Protocol):
    def list_price_history(
        self,
        *,
        symbol: str,
        start_date: dt.date | None = None,
        end_date: dt.date | None = None,
    ) -> list[dict[str, object]]: ...

    def list_price_bars(
        self,
        *,
        symbol: str,
        interval: str = "1d",
        start_date: dt.date | None = None,
        end_date: dt.date | None = None,
    ) -> list[dict[str, object]]: ...


@dataclass
class DailySqlitePriceProvider:
    db_path: Path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def list_price_history(
        self,
        *,
        symbol: str,
        start_date: dt.date | None = None,
        end_date: dt.date | None = None,
    ) -> list[dict[str, object]]:
        query = "SELECT trade_date, close FROM prices_daily WHERE symbol = ?"
        params: list[object] = [symbol]
        if start_date is not None:
            query += " AND trade_date >= ?"
            params.append(start_date.isoformat())
        if end_date is not None:
            query += " AND trade_date <= ?"
            params.append(end_date.isoformat())
        query += " ORDER BY trade_date"
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [{"date": str(row["trade_date"]), "close": float(row["close"])} for row in rows]

    def list_price_bars(
        self,
        *,
        symbol: str,
        interval: str = "1d",
        start_date: dt.date | None = None,
        end_date: dt.date | None = None,
    ) -> list[dict[str, object]]:
        if interval != "1d":
            raise ValueError(f"Unsupported interval: {interval}")
        query = """
            SELECT trade_date, open, high, low, close, volume
            FROM prices_daily
            WHERE symbol = ?
        """
        params: list[object] = [symbol]
        if start_date is not None:
            query += " AND trade_date >= ?"
            params.append(start_date.isoformat())
        if end_date is not None:
            query += " AND trade_date <= ?"
            params.append(end_date.isoformat())
        query += " ORDER BY trade_date"
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            {
                "time": str(row["trade_date"]),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]) if row["volume"] is not None else None,
            }
            for row in rows
        ]
