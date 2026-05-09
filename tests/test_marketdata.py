from __future__ import annotations

import datetime as dt
import sqlite3
import tempfile
import unittest
from pathlib import Path

import simulate_portfolio
from ib_qlib_pipeline.marketdata.daily_db import (
    DailyPriceStore,
    default_daily_db_path,
    import_daily_csv_file,
    init_daily_market_db,
)
from ib_qlib_pipeline.marketdata.price_provider import DailySqlitePriceProvider
from ib_qlib_pipeline.webapi.db import init_db
from ib_qlib_pipeline.webapi.universe_store import ensure_default_universes
from ib_qlib_pipeline.webapi.price_store import list_price_bars, list_price_history


class MarketDataTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.project_root = Path(self._tmp.name)
        (self.project_root / "data" / "processed" / "qlib_csv").mkdir(parents=True, exist_ok=True)
        (self.project_root / "symbols").mkdir(parents=True, exist_ok=True)
        self.csv_path = self.project_root / "data" / "processed" / "qlib_csv" / "TEST.csv"
        self.project_root.joinpath("symbols", "sp500_full_ib_map.txt").write_text("TEST,TEST\n", encoding="utf-8")
        self.project_root.joinpath("symbols", "us_union_sp500_ndx_djia_sox.txt").write_text("TEST,TEST\n", encoding="utf-8")
        self.csv_path.write_text(
            "\n".join(
                [
                    "symbol,date,open,high,low,close,volume,factor",
                    "TEST,2026-05-05,10,11,9,10.5,1000,1",
                    "TEST,2026-05-06,10.5,12,10,11.5,1200,1",
                ]
            ),
            encoding="utf-8",
        )
        self.db_path = default_daily_db_path(self.project_root)
        init_daily_market_db(self.db_path)
        self.service_db_path = self.project_root / "data" / "app" / "ranking_service.db"
        init_db(self.service_db_path)
        ensure_default_universes(self.service_db_path, self.project_root)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_import_daily_csv_file(self) -> None:
        inserted = import_daily_csv_file(self.db_path, self.csv_path, symbol="TEST")
        self.assertEqual(2, inserted)
        with sqlite3.connect(self.db_path) as conn:
            total = conn.execute("SELECT COUNT(*) FROM prices_daily WHERE symbol = 'TEST'").fetchone()[0]
        self.assertEqual(2, total)

    def test_daily_sqlite_price_provider(self) -> None:
        import_daily_csv_file(self.db_path, self.csv_path, symbol="TEST")
        provider = DailySqlitePriceProvider(self.db_path)

        history = provider.list_price_history(
            symbol="TEST",
            start_date=dt.date(2026, 5, 5),
            end_date=dt.date(2026, 5, 6),
        )
        bars = provider.list_price_bars(symbol="TEST", interval="1d")

        self.assertEqual(2, len(history))
        self.assertEqual("2026-05-05", history[0]["date"])
        self.assertEqual(10.5, history[0]["close"])
        self.assertEqual(2, len(bars))
        self.assertEqual("2026-05-06", bars[1]["time"])
        self.assertEqual(1200.0, bars[1]["volume"])

    def test_price_store_prefers_provider_over_csv(self) -> None:
        import_daily_csv_file(self.db_path, self.csv_path, symbol="TEST")

        history = list_price_history(
            self.project_root,
            symbol="TEST",
            start_date=dt.date(2026, 5, 5),
            end_date=dt.date(2026, 5, 6),
        )
        bars = list_price_bars(self.project_root, symbol="TEST", interval="1d")

        self.assertEqual(2, len(history))
        self.assertEqual(2, len(bars))
        self.assertEqual(11.5, bars[1]["close"])

    def test_daily_price_store_import_directory(self) -> None:
        store = DailyPriceStore(self.project_root)
        imported = store.import_qlib_csv_directory(symbols=["TEST"])
        self.assertEqual(2, imported)
        history = store.provider.list_price_history(symbol="TEST")
        self.assertEqual(2, len(history))

    def test_daily_price_store_import_universe(self) -> None:
        store = DailyPriceStore(self.project_root)
        imported = store.import_universe(self.service_db_path, universe_key="sp500")
        self.assertEqual(2, imported)
        history = store.provider.list_price_history(symbol="TEST")
        self.assertEqual(2, len(history))

    def test_simulate_portfolio_price_lookup_prefers_provider(self) -> None:
        import_daily_csv_file(self.db_path, self.csv_path, symbol="TEST")
        price = simulate_portfolio.get_price(self.project_root, "TEST", dt.date(2026, 5, 6), "open")
        self.assertEqual(10.5, price)
