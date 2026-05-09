from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from sqlalchemy import select

from ..dborm.models import PriceDaily
from ..dborm.session import create_session_factory_for_path


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

    def _session(self):
        return create_session_factory_for_path(self.db_path)()

    def list_price_history(
        self,
        *,
        symbol: str,
        start_date: dt.date | None = None,
        end_date: dt.date | None = None,
    ) -> list[dict[str, object]]:
        query = select(PriceDaily.trade_date, PriceDaily.close).where(PriceDaily.symbol == symbol)
        if start_date is not None:
            query = query.where(PriceDaily.trade_date >= start_date.isoformat())
        if end_date is not None:
            query = query.where(PriceDaily.trade_date <= end_date.isoformat())
        query = query.order_by(PriceDaily.trade_date)
        with self._session() as session:
            rows = session.execute(query).all()
        return [{"date": str(trade_date), "close": float(close)} for trade_date, close in rows]

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
        query = (
            select(
                PriceDaily.trade_date,
                PriceDaily.open,
                PriceDaily.high,
                PriceDaily.low,
                PriceDaily.close,
                PriceDaily.volume,
            )
            .where(PriceDaily.symbol == symbol)
        )
        if start_date is not None:
            query = query.where(PriceDaily.trade_date >= start_date.isoformat())
        if end_date is not None:
            query = query.where(PriceDaily.trade_date <= end_date.isoformat())
        query = query.order_by(PriceDaily.trade_date)
        with self._session() as session:
            rows = session.execute(query).all()
        return [
            {
                "time": str(trade_date),
                "open": float(open_),
                "high": float(high),
                "low": float(low),
                "close": float(close),
                "volume": float(volume) if volume is not None else None,
            }
            for trade_date, open_, high, low, close, volume in rows
        ]
