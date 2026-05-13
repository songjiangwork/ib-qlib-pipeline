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
    import_daily_price_frame,
    init_daily_market_db,
)
from ib_qlib_pipeline.marketdata.price_provider import DailySqlitePriceProvider
from ib_qlib_pipeline.webapi.db import init_db
from ib_qlib_pipeline.webapi.strategy_store import ensure_default_strategies
from ib_qlib_pipeline.webapi.universe_store import ensure_default_universes
from ib_qlib_pipeline.webapi.price_store import list_price_bars, list_price_history, load_close_lookup


class MarketDataTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.project_root = Path(self._tmp.name)
        (self.project_root / "data" / "processed" / "qlib_csv").mkdir(parents=True, exist_ok=True)
        (self.project_root / "symbols").mkdir(parents=True, exist_ok=True)
        self.csv_path = self.project_root / "data" / "processed" / "qlib_csv" / "TEST.csv"
        self.project_root.joinpath("symbols", "sp500_full_ib_map.txt").write_text("TEST,TEST\n", encoding="utf-8")
        self.project_root.joinpath("symbols", "us_union_sp500_ndx_djia_sox.txt").write_text("TEST,TEST\n", encoding="utf-8")
        self.project_root.joinpath("symbols", "cn_a_share.txt").write_text("", encoding="utf-8")
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
        ensure_default_strategies(self.service_db_path)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_import_daily_csv_file(self) -> None:
        inserted = import_daily_csv_file(self.db_path, self.csv_path, symbol="TEST")
        self.assertEqual(2, inserted)
        with sqlite3.connect(self.db_path) as conn:
            total = conn.execute("SELECT COUNT(*) FROM prices_daily WHERE symbol = 'TEST'").fetchone()[0]
        self.assertEqual(2, total)

    def test_import_daily_price_frame(self) -> None:
        frame = simulate_portfolio.load_price_frame(str(self.project_root), "TEST")
        inserted = import_daily_price_frame(self.db_path, frame, symbol="TEST", source="pipeline")
        self.assertEqual(2, inserted)
        provider = DailySqlitePriceProvider(self.db_path)
        history = provider.list_price_history(symbol="TEST")
        self.assertEqual(2, len(history))
        self.assertEqual(11.5, history[-1]["close"])

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

    def test_load_close_lookup_prefers_provider(self) -> None:
        import_daily_csv_file(self.db_path, self.csv_path, symbol="TEST")
        lookup = load_close_lookup(
            self.project_root,
            symbols=["TEST"],
            signal_dates=[dt.date(2026, 5, 5), dt.date(2026, 5, 6)],
        )
        self.assertEqual(10.5, lookup[("TEST", dt.date(2026, 5, 5))])
        self.assertEqual(11.5, lookup[("TEST", dt.date(2026, 5, 6))])

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

    def test_resolve_strategy_runtime_uses_default_strategy(self) -> None:
        runtime = simulate_portfolio.resolve_strategy_runtime(
            db_path=self.service_db_path,
            run_name="test-run",
            strategy_key=None,
            buy_top_n=10,
            hold_top_n=20,
            target_notional=10000.0,
        )
        self.assertEqual("top10_hold20_default", runtime.strategy_key)
        self.assertEqual(10, runtime.buy_top_n)
        self.assertEqual(20, runtime.hold_top_n)

    def test_capped_target_notional_applies_absolute_and_percent_caps(self) -> None:
        runtime = simulate_portfolio.StrategyRuntimeConfig(
            strategy_id=None,
            strategy_key="cap-test",
            strategy_name="Cap Test",
            strategy_type="ranking_hold_band",
            buy_top_n=10,
            hold_top_n=20,
            target_notional=10000.0,
            signal_timing="t_close",
            execution_timing="t_plus_1_open",
            trade_price_basis="open",
            max_position_notional=7000.0,
            max_position_pct=2.0,
            initial_capital=200000.0,
        )
        self.assertEqual(4000.0, simulate_portfolio.capped_target_notional(runtime))

    def test_adjusted_execution_price_applies_fee_and_slippage(self) -> None:
        runtime = simulate_portfolio.StrategyRuntimeConfig(
            strategy_id=None,
            strategy_key="cost-test",
            strategy_name="Cost Test",
            strategy_type="ranking_hold_band",
            buy_top_n=10,
            hold_top_n=20,
            target_notional=10000.0,
            signal_timing="t_close",
            execution_timing="t_plus_1_open",
            trade_price_basis="open",
            fee_bps=5.0,
            slippage_bps=10.0,
        )
        self.assertAlmostEqual(100.15, simulate_portfolio.adjusted_execution_price(100.0, side="buy", strategy=runtime))
        self.assertAlmostEqual(99.85, simulate_portfolio.adjusted_execution_price(100.0, side="sell", strategy=runtime))

    def test_gap_filter_blocks_large_gap_up(self) -> None:
        runtime = simulate_portfolio.StrategyRuntimeConfig(
            strategy_id=None,
            strategy_key="gap-test",
            strategy_name="Gap Test",
            strategy_type="ranking_hold_band",
            buy_top_n=10,
            hold_top_n=20,
            target_notional=10000.0,
            signal_timing="t_close",
            execution_timing="t_plus_1_open",
            trade_price_basis="open",
            gap_up_limit_pct=3.0,
        )
        blocked = simulate_portfolio.blocked_by_gap_filter(
            self.project_root,
            "TEST",
            dt.date(2026, 5, 6),
            10.5,
            runtime,
        )
        self.assertFalse(blocked)
        blocked = simulate_portfolio.blocked_by_gap_filter(
            self.project_root,
            "TEST",
            dt.date(2026, 5, 6),
            11.0,
            runtime,
        )
        self.assertTrue(blocked)
