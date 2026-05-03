#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from functools import lru_cache
from pathlib import Path

import pandas as pd

from ib_qlib_pipeline.webapi.db import connect, init_db
from ib_qlib_pipeline.webapi.portfolio_store import (
    OpenLot,
    close_portfolio_lot,
    create_portfolio_run,
    insert_portfolio_lot,
    insert_portfolio_mark,
)
from ib_qlib_pipeline.webapi.settings import Settings
from oneclick_daily_ranking import read_available_trading_days


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Simulate per-stock lots using T-1 rankings and T open execution.",
    )
    parser.add_argument("--start-date", default="2025-01-01", help="Start signal date in YYYY-MM-DD format")
    parser.add_argument("--end-date", default=None, help="End signal date in YYYY-MM-DD format")
    parser.add_argument("--name", default=None, help="Optional portfolio run label")
    parser.add_argument("--buy-top-n", type=int, default=10, help="Buy new symbols from top N each signal day")
    parser.add_argument("--hold-top-n", type=int, default=20, help="Continue holding while symbol stays within top N")
    parser.add_argument("--target-notional", type=float, default=10000.0, help="Dollar notional per new lot")
    parser.add_argument("--notes", default=None, help="Optional notes stored with the portfolio run")
    return parser.parse_args()


def load_signal_map(db_path: Path, start_date: str, end_date: str | None) -> dict[str, dict]:
    end_date_sql = end_date or "9999-12-31"
    query = """
        SELECT r.id AS run_id,
               r.signal_date,
               rec.rank,
               rec.symbol
        FROM runs r
        JOIN recommendations rec ON rec.run_id = r.id
        JOIN (
            SELECT signal_date, MAX(id) AS run_id
            FROM runs
            WHERE status = 'succeeded'
              AND signal_date IS NOT NULL
              AND signal_date BETWEEN ? AND ?
            GROUP BY signal_date
        ) latest ON latest.run_id = r.id
        WHERE r.signal_date BETWEEN ? AND ?
        ORDER BY r.signal_date, rec.rank
    """
    signal_map: dict[str, dict] = {}
    with connect(db_path) as conn:
        rows = conn.execute(query, (start_date, end_date_sql, start_date, end_date_sql)).fetchall()
    for row in rows:
        signal_date = str(row["signal_date"])
        bucket = signal_map.setdefault(signal_date, {"run_id": int(row["run_id"]), "top20": [], "rank_map": {}})
        rank = int(row["rank"])
        symbol = str(row["symbol"])
        if rank <= 20:
            bucket["top20"].append(symbol)
        bucket["rank_map"][symbol] = rank
    return signal_map


@lru_cache(maxsize=1024)
def load_price_frame(project_root_str: str, symbol: str) -> pd.DataFrame:
    path = Path(project_root_str) / "data" / "processed" / "qlib_csv" / f"{symbol}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing price file for {symbol}: {path}")
    frame = pd.read_csv(path, usecols=["date", "open", "close"])
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    frame = frame.sort_values("date").reset_index(drop=True)
    return frame


def get_price(project_root: Path, symbol: str, trade_date: dt.date, field: str) -> float | None:
    try:
        frame = load_price_frame(str(project_root), symbol)
    except FileNotFoundError:
        return None
    hit = frame.loc[frame["date"] == trade_date, field]
    if hit.empty:
        return None
    return float(hit.iloc[-1])


def next_trade_date_map(trading_days: list[dt.date]) -> dict[dt.date, dt.date]:
    mapping: dict[dt.date, dt.date] = {}
    for left, right in zip(trading_days, trading_days[1:]):
        mapping[left] = right
    return mapping


def simulate() -> None:
    args = parse_args()
    settings = Settings.load()
    init_db(settings.db_path)

    project_root = settings.project_root
    trading_days = read_available_trading_days(project_root)
    next_day = next_trade_date_map(trading_days)

    start_date = dt.date.fromisoformat(args.start_date)
    end_date = dt.date.fromisoformat(args.end_date) if args.end_date else None
    signal_map = load_signal_map(settings.db_path, args.start_date, args.end_date)
    if not signal_map:
        raise SystemExit("No ranking runs found in DB for the selected signal date range")

    signal_days = sorted(dt.date.fromisoformat(value) for value in signal_map.keys())
    if end_date is None:
        end_date = signal_days[-1]
    run_name = args.name or f"top{args.buy_top_n}-hold{args.hold_top_n}-{args.start_date}-to-{end_date.isoformat()}"
    portfolio_run_id = create_portfolio_run(
        db_path=settings.db_path,
        name=run_name,
        strategy="t_minus_1_rank__t_open_execute",
        buy_top_n=args.buy_top_n,
        hold_top_n=args.hold_top_n,
        target_notional=args.target_notional,
        start_signal_date=args.start_date,
        end_signal_date=end_date.isoformat(),
        notes=args.notes,
    )

    open_lots: dict[str, list[OpenLot]] = {}
    processed_signals = 0

    for signal_date in signal_days:
        if signal_date < start_date or signal_date > end_date:
            continue
        trade_date = next_day.get(signal_date)
        if trade_date is None:
            print(f"[skip] {signal_date.isoformat()} has no next trade date")
            continue

        bucket = signal_map[signal_date.isoformat()]
        run_id = int(bucket["run_id"])
        top20 = list(bucket["top20"])[: args.hold_top_n]
        top10 = top20[: args.buy_top_n]
        top20_set = set(top20)
        top10_set = set(top10)
        rank_map = dict(bucket["rank_map"])

        for symbol, lots in list(open_lots.items()):
            if symbol in top20_set:
                continue
            exit_price = get_price(project_root, symbol, trade_date, "open")
            if exit_price is None:
                print(f"[warn] missing open price for sell {symbol} on {trade_date.isoformat()}, keeping lot open")
                continue
            for lot in lots:
                close_portfolio_lot(
                    db_path=settings.db_path,
                    lot_id=lot.id,
                    exit_run_id=run_id,
                    exit_signal_date=signal_date.isoformat(),
                    exit_trade_date=trade_date.isoformat(),
                    exit_rank=rank_map.get(symbol),
                    exit_price_open=exit_price,
                )
            del open_lots[symbol]

        for symbol in top10:
            if symbol in open_lots:
                continue
            entry_price = get_price(project_root, symbol, trade_date, "open")
            if entry_price is None or entry_price <= 0:
                print(f"[warn] missing open price for buy {symbol} on {trade_date.isoformat()}, skipping")
                continue
            shares = int(args.target_notional // entry_price)
            if shares <= 0:
                print(f"[warn] zero shares for {symbol} on {trade_date.isoformat()} at price {entry_price}")
                continue
            lot_id = insert_portfolio_lot(
                db_path=settings.db_path,
                portfolio_run_id=portfolio_run_id,
                symbol=symbol,
                entry_run_id=run_id,
                entry_signal_date=signal_date.isoformat(),
                entry_trade_date=trade_date.isoformat(),
                entry_rank=int(rank_map.get(symbol, 9999)),
                entry_price_open=entry_price,
                shares=shares,
                target_notional=args.target_notional,
            )
            open_lots[symbol] = [
                OpenLot(
                    id=lot_id,
                    symbol=symbol,
                    entry_run_id=run_id,
                    entry_signal_date=signal_date.isoformat(),
                    entry_trade_date=trade_date.isoformat(),
                    entry_rank=int(rank_map.get(symbol, 9999)),
                    entry_price_open=entry_price,
                    shares=shares,
                    target_notional=args.target_notional,
                )
            ]

        for symbol, lots in open_lots.items():
            close_price = get_price(project_root, symbol, trade_date, "close")
            if close_price is None:
                print(f"[warn] missing close price for mark {symbol} on {trade_date.isoformat()}, skipping mark")
                continue
            for lot in lots:
                market_value = close_price * lot.shares
                cost = lot.entry_price_open * lot.shares
                unrealized_pnl = market_value - cost
                unrealized_return_pct = ((close_price / lot.entry_price_open) - 1.0) * 100.0
                insert_portfolio_mark(
                    db_path=settings.db_path,
                    portfolio_lot_id=lot.id,
                    trade_date=trade_date.isoformat(),
                    close_price=close_price,
                    market_value=market_value,
                    unrealized_pnl=unrealized_pnl,
                    unrealized_return_pct=unrealized_return_pct,
                    is_in_top20=symbol in top20_set,
                    is_in_top10=symbol in top10_set,
                )

        processed_signals += 1
        print(
            f"[ok] signal={signal_date.isoformat()} trade={trade_date.isoformat()} "
            f"held={sum(len(v) for v in open_lots.values())}"
        )

    print(
        f"[ok] portfolio_run_id={portfolio_run_id} processed_signals={processed_signals} "
        f"open_lots={sum(len(v) for v in open_lots.values())}"
    )


if __name__ == "__main__":
    simulate()
