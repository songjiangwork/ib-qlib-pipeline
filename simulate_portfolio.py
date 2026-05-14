#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import pandas as pd

from ib_qlib_pipeline.marketdata.daily_db import default_daily_db_path
from ib_qlib_pipeline.marketdata.price_provider import DailySqlitePriceProvider
from ib_qlib_pipeline.qlib_runtime import load_qlib_runtime_config
from ib_qlib_pipeline.webapi.db import init_db
from ib_qlib_pipeline.webapi.model_store import get_model
from ib_qlib_pipeline.webapi.portfolio_store import (
    OpenLot,
    close_portfolio_lot,
    create_portfolio_run,
    get_portfolio_run,
    insert_portfolio_lot,
    insert_portfolio_mark,
    load_open_lots,
    trade_date_has_existing_activity,
    update_portfolio_run_end_date,
)
from ib_qlib_pipeline.webapi.run_store import load_signal_map
from ib_qlib_pipeline.webapi.settings import Settings
from ib_qlib_pipeline.webapi.strategy_store import ensure_default_strategies, get_strategy_by_key, strategy_snapshot_for_portfolio
from ib_qlib_pipeline.ranking.ranking_loader import read_available_trading_days


def status(message: str) -> None:
    print(message, flush=True)


@dataclass
class StrategyRuntimeConfig:
    strategy_id: int | None
    strategy_key: str
    strategy_name: str
    strategy_type: str
    buy_top_n: int
    hold_top_n: int
    target_notional: float
    signal_timing: str
    execution_timing: str
    trade_price_basis: str
    max_open_positions: int | None = None
    max_position_notional: float | None = None
    max_position_pct: float | None = None
    initial_capital: float | None = None
    fee_bps: float | None = None
    slippage_bps: float | None = None
    gap_up_limit_pct: float | None = None
    gap_down_limit_pct: float | None = None
    allow_reentry: bool | None = None
    snapshot: dict[str, object] | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Simulate per-stock lots using T-1 rankings and T open execution.",
    )
    parser.add_argument("--start-date", default="2025-01-01", help="Start signal date in YYYY-MM-DD format")
    parser.add_argument("--end-date", default=None, help="End signal date in YYYY-MM-DD format")
    parser.add_argument("--name", default=None, help="Optional portfolio run label")
    parser.add_argument("--append-run-id", type=int, default=None, help="Append new signal days into an existing portfolio run")
    parser.add_argument("--model-id", type=int, default=None, help="Only simulate rankings from the specified model id")
    parser.add_argument("--strategy-key", default=None, help="Optional registered strategy key to apply for a new portfolio run")
    parser.add_argument("--buy-top-n", type=int, default=10, help="Buy new symbols from top N each signal day")
    parser.add_argument("--hold-top-n", type=int, default=20, help="Continue holding while symbol stays within top N")
    parser.add_argument("--target-notional", type=float, default=10000.0, help="Dollar notional per new lot")
    parser.add_argument("--notes", default=None, help="Optional notes stored with the portfolio run")
    return parser.parse_args()


@lru_cache(maxsize=1024)
def load_price_frame(project_root_str: str, symbol: str, qlib_csv_dir_str: str | None = None) -> pd.DataFrame:
    if qlib_csv_dir_str:
        path = Path(qlib_csv_dir_str) / f"{symbol}.csv"
    else:
        path = Path(project_root_str) / "data" / "processed" / "qlib_csv" / f"{symbol}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing price file for {symbol}: {path}")
    frame = pd.read_csv(path, usecols=["date", "open", "close"])
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    frame = frame.sort_values("date").reset_index(drop=True)
    return frame


def get_price(
    project_root: Path,
    symbol: str,
    trade_date: dt.date,
    field: str,
    *,
    qlib_csv_dir: Path | None = None,
) -> float | None:
    db_path = default_daily_db_path(project_root)
    if db_path.exists():
        provider = DailySqlitePriceProvider(db_path)
        bars = provider.list_price_bars(
            symbol=symbol,
            interval="1d",
            start_date=trade_date,
            end_date=trade_date,
        )
        if bars:
            value = bars[-1].get(field)
            return float(value) if value is not None else None
    try:
        frame = load_price_frame(
            str(project_root),
            symbol,
            str(qlib_csv_dir) if qlib_csv_dir is not None else None,
        )
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


def updated_run_name(existing_name: str, end_signal_date: dt.date) -> str:
    return re.sub(r"\d{4}-\d{2}-\d{2}$", end_signal_date.isoformat(), existing_name)


def strategy_config_from_snapshot(snapshot: dict[str, object] | None, *, fallback_name: str) -> StrategyRuntimeConfig | None:
    if not snapshot:
        return None
    buy_top_n = snapshot.get("buy_top_n")
    hold_top_n = snapshot.get("hold_top_n")
    target_notional = snapshot.get("target_notional")
    if buy_top_n is None or hold_top_n is None or target_notional is None:
        return None
    return StrategyRuntimeConfig(
        strategy_id=int(snapshot["strategy_id"]) if snapshot.get("strategy_id") is not None else None,
        strategy_key=str(snapshot.get("strategy_key") or "snapshot"),
        strategy_name=str(snapshot.get("strategy_name") or fallback_name),
        strategy_type=str(snapshot.get("strategy_type") or "ranking_hold_band"),
        buy_top_n=int(buy_top_n),
        hold_top_n=int(hold_top_n),
        target_notional=float(target_notional),
        signal_timing=str(snapshot.get("signal_timing") or "t_close"),
        execution_timing=str(snapshot.get("execution_timing") or "t_plus_1_open"),
        trade_price_basis=str(snapshot.get("trade_price_basis") or "open"),
        max_open_positions=int(snapshot["max_open_positions"]) if snapshot.get("max_open_positions") is not None else None,
        max_position_notional=float(snapshot["max_position_notional"]) if snapshot.get("max_position_notional") is not None else None,
        max_position_pct=float(snapshot["max_position_pct"]) if snapshot.get("max_position_pct") is not None else None,
        initial_capital=float(snapshot["initial_capital"]) if snapshot.get("initial_capital") is not None else None,
        fee_bps=float(snapshot["fee_bps"]) if snapshot.get("fee_bps") is not None else None,
        slippage_bps=float(snapshot["slippage_bps"]) if snapshot.get("slippage_bps") is not None else None,
        gap_up_limit_pct=float(snapshot["gap_up_limit_pct"]) if snapshot.get("gap_up_limit_pct") is not None else None,
        gap_down_limit_pct=float(snapshot["gap_down_limit_pct"]) if snapshot.get("gap_down_limit_pct") is not None else None,
        allow_reentry=bool(snapshot["allow_reentry"]) if snapshot.get("allow_reentry") is not None else None,
        snapshot=dict(snapshot),
    )


def resolve_strategy_runtime(
    *,
    db_path: Path,
    run_name: str,
    strategy_key: str | None,
    buy_top_n: int,
    hold_top_n: int,
    target_notional: float,
) -> StrategyRuntimeConfig:
    registered = get_strategy_by_key(db_path, strategy_key) if strategy_key else None
    if registered is None and strategy_key is None and buy_top_n == 10 and hold_top_n == 20 and abs(float(target_notional) - 10000.0) < 1e-9:
        registered = get_strategy_by_key(db_path, "top10_hold20_default")
    if registered is not None:
        snapshot = strategy_snapshot_for_portfolio(db_path, int(registered["id"]))
        return strategy_config_from_snapshot(snapshot, fallback_name=run_name) or StrategyRuntimeConfig(
            strategy_id=int(registered["id"]),
            strategy_key=str(registered["key"]),
            strategy_name=str(registered["name"]),
            strategy_type=str(registered["strategy_type"]),
            buy_top_n=int(registered["buy_top_n"]),
            hold_top_n=int(registered["hold_top_n"]),
            target_notional=float(registered["target_notional"]),
            signal_timing=str(registered.get("signal_timing") or "t_close"),
            execution_timing=str(registered.get("execution_timing") or "t_plus_1_open"),
            trade_price_basis=str(registered.get("trade_price_basis") or "open"),
            max_open_positions=int(registered["max_open_positions"]) if registered.get("max_open_positions") is not None else None,
            max_position_notional=float(registered["max_position_notional"]) if registered.get("max_position_notional") is not None else None,
            max_position_pct=float(registered["max_position_pct"]) if registered.get("max_position_pct") is not None else None,
            initial_capital=float(registered["initial_capital"]) if registered.get("initial_capital") is not None else None,
            fee_bps=float(registered["fee_bps"]) if registered.get("fee_bps") is not None else None,
            slippage_bps=float(registered["slippage_bps"]) if registered.get("slippage_bps") is not None else None,
            gap_up_limit_pct=float(registered["gap_up_limit_pct"]) if registered.get("gap_up_limit_pct") is not None else None,
            gap_down_limit_pct=float(registered["gap_down_limit_pct"]) if registered.get("gap_down_limit_pct") is not None else None,
            allow_reentry=bool(registered["allow_reentry"]) if registered.get("allow_reentry") is not None else None,
            snapshot=snapshot,
        )
    snapshot = {
        "strategy_key": strategy_key or "ad_hoc",
        "strategy_name": run_name,
        "strategy_type": "ranking_hold_band",
        "buy_top_n": buy_top_n,
        "hold_top_n": hold_top_n,
        "target_notional": target_notional,
        "signal_timing": "t_close",
        "execution_timing": "t_plus_1_open",
        "trade_price_basis": "open",
    }
    return strategy_config_from_snapshot(snapshot, fallback_name=run_name)  # type: ignore[return-value]


def capped_target_notional(strategy: StrategyRuntimeConfig) -> float:
    value = float(strategy.target_notional)
    if strategy.max_position_notional is not None:
        value = min(value, float(strategy.max_position_notional))
    if strategy.initial_capital is not None and strategy.max_position_pct is not None:
        pct_cap = float(strategy.initial_capital) * float(strategy.max_position_pct) / 100.0
        value = min(value, pct_cap)
    return value


def adjusted_execution_price(raw_price: float, *, side: str, strategy: StrategyRuntimeConfig) -> float:
    adjustment_bps = float(strategy.slippage_bps or 0.0) + float(strategy.fee_bps or 0.0)
    if adjustment_bps == 0:
        return raw_price
    factor = 1.0 + adjustment_bps / 10000.0 if side == "buy" else 1.0 - adjustment_bps / 10000.0
    return raw_price * factor


def previous_close(
    project_root: Path,
    symbol: str,
    trade_date: dt.date,
    *,
    qlib_csv_dir: Path | None = None,
) -> float | None:
    try:
        frame = load_price_frame(
            str(project_root),
            symbol,
            str(qlib_csv_dir) if qlib_csv_dir is not None else None,
        )
    except FileNotFoundError:
        return None
    history = frame.loc[frame["date"] < trade_date, "close"]
    if history.empty:
        return None
    return float(history.iloc[-1])


def blocked_by_gap_filter(
    project_root: Path,
    symbol: str,
    trade_date: dt.date,
    entry_open: float,
    strategy: StrategyRuntimeConfig,
    *,
    qlib_csv_dir: Path | None = None,
) -> bool:
    if strategy.gap_up_limit_pct is None and strategy.gap_down_limit_pct is None:
        return False
    prev_close = previous_close(project_root, symbol, trade_date, qlib_csv_dir=qlib_csv_dir)
    if prev_close is None or prev_close <= 0:
        return False
    gap_pct = ((entry_open / prev_close) - 1.0) * 100.0
    if strategy.gap_up_limit_pct is not None and gap_pct > float(strategy.gap_up_limit_pct):
        return True
    if strategy.gap_down_limit_pct is not None and gap_pct < float(strategy.gap_down_limit_pct):
        return True
    return False


def simulate() -> None:
    args = parse_args()
    settings = Settings.load()
    init_db(settings.db_path)
    ensure_default_strategies(settings.db_path)

    project_root = settings.project_root

    start_date = dt.date.fromisoformat(args.start_date)
    end_date = dt.date.fromisoformat(args.end_date) if args.end_date else None
    append_run_id = args.append_run_id

    existing_run_name: str | None = None
    existing_end_date: dt.date | None = None
    if append_run_id is not None:
        existing_run = get_portfolio_run(settings.db_path, append_run_id)
        if existing_run is None:
            raise SystemExit(f"Portfolio run not found: {append_run_id}")

        existing_end = existing_run.get("end_signal_date")
        if not existing_end:
            raise SystemExit(f"Portfolio run {append_run_id} has no end_signal_date")
        existing_end_date = dt.date.fromisoformat(str(existing_end))
        existing_run_name = str(existing_run["name"])
        if existing_end_date > start_date:
            start_date = existing_end_date
        if end_date is not None and end_date < start_date:
            raise SystemExit(
                f"No new signal days to append: existing end_signal_date={existing_end_date.isoformat()} "
                f"already covers requested range ending at {end_date.isoformat()}"
            )

        existing_strategy = strategy_config_from_snapshot(existing_run.get("strategy_config"), fallback_name=existing_run_name)
        if existing_strategy is None:
            existing_strategy = resolve_strategy_runtime(
                db_path=settings.db_path,
                run_name=existing_run_name,
                strategy_key=None,
                buy_top_n=int(existing_run["buy_top_n"]),
                hold_top_n=int(existing_run["hold_top_n"]),
                target_notional=float(existing_run["target_notional"]),
            )
        args.buy_top_n = existing_strategy.buy_top_n
        args.hold_top_n = existing_strategy.hold_top_n
        args.target_notional = existing_strategy.target_notional
        if args.model_id is None:
            args.model_id = int(existing_run["model_id"]) if existing_run.get("model_id") is not None else None
        elif existing_run.get("model_id") is not None and int(existing_run["model_id"]) != int(args.model_id):
            raise SystemExit(
                f"Model mismatch: portfolio run {append_run_id} uses model_id={existing_run['model_id']}, "
                f"but --model-id={args.model_id} was provided"
            )

    model = get_model(settings.db_path, int(args.model_id)) if args.model_id is not None else None
    model_details = dict(model.get("details") or {}) if model is not None else {}
    config_path = None
    if model_details.get("config_path"):
        config_path = (project_root / str(model_details["config_path"])).resolve()
    runtime_cfg = load_qlib_runtime_config(project_root, config_path=config_path)
    qlib_csv_dir = runtime_cfg.qlib_csv_dir
    trading_days = read_available_trading_days(project_root, config_path=config_path)
    next_day = next_trade_date_map(trading_days)

    signal_map = load_signal_map(
        settings.db_path,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat() if end_date else None,
        model_id=args.model_id,
    )
    if not signal_map:
        raise SystemExit("No ranking runs found in DB for the selected signal date range")

    signal_days = sorted(dt.date.fromisoformat(value) for value in signal_map.keys())
    if end_date is None:
        end_date = signal_days[-1]
    if append_run_id is not None:
        portfolio_run_id = append_run_id
        open_lots = load_open_lots(settings.db_path, portfolio_run_id)
        strategy_runtime = existing_strategy
        status(
            f"[info] appending portfolio_run_id={portfolio_run_id} "
            f"from {start_date.isoformat()} to {end_date.isoformat()} "
            f"existing_open_lots={sum(len(v) for v in open_lots.values())}"
        )
    else:
        model_suffix = f"-model{args.model_id}" if args.model_id is not None else ""
        run_name = (
            args.name
            or f"top{args.buy_top_n}-hold{args.hold_top_n}{model_suffix}-{args.start_date}-to-{end_date.isoformat()}"
        )
        strategy_runtime = resolve_strategy_runtime(
            db_path=settings.db_path,
            run_name=run_name,
            strategy_key=args.strategy_key,
            buy_top_n=args.buy_top_n,
            hold_top_n=args.hold_top_n,
            target_notional=args.target_notional,
        )
        portfolio_run_id = create_portfolio_run(
            db_path=settings.db_path,
            name=run_name,
            strategy_id=strategy_runtime.strategy_id,
            strategy="t_minus_1_rank__t_open_execute",
            strategy_config=strategy_runtime.snapshot,
            buy_top_n=strategy_runtime.buy_top_n,
            hold_top_n=strategy_runtime.hold_top_n,
            target_notional=strategy_runtime.target_notional,
            start_signal_date=args.start_date,
            end_signal_date=end_date.isoformat(),
            universe_id=int(model["universe_id"]) if model is not None and model.get("universe_id") is not None else None,
            notes=args.notes,
        )
        open_lots = {}

    processed_signals = 0
    last_processed_signal_date: dt.date | None = None

    for signal_date in signal_days:
        if signal_date < start_date or signal_date > end_date:
            continue
        trade_date = next_day.get(signal_date)
        if trade_date is None:
            status(f"[skip] {signal_date.isoformat()} has no next trade date")
            continue
        if append_run_id is not None and trade_date_has_existing_activity(settings.db_path, portfolio_run_id, trade_date):
            status(
                f"[skip] signal={signal_date.isoformat()} trade={trade_date.isoformat()} "
                f"already applied to portfolio_run_id={portfolio_run_id}"
            )
            last_processed_signal_date = signal_date
            continue

        bucket = signal_map[signal_date.isoformat()]
        run_id = int(bucket["run_id"])
        top20 = list(bucket["top20"])[: strategy_runtime.hold_top_n]
        top10 = top20[: strategy_runtime.buy_top_n]
        top20_set = set(top20)
        top10_set = set(top10)
        rank_map = dict(bucket["rank_map"])

        for symbol, lots in list(open_lots.items()):
            if symbol in top20_set:
                continue
            raw_exit_price = get_price(project_root, symbol, trade_date, "open", qlib_csv_dir=qlib_csv_dir)
            if raw_exit_price is None:
                status(f"[warn] missing open price for sell {symbol} on {trade_date.isoformat()}, keeping lot open")
                continue
            exit_price = adjusted_execution_price(raw_exit_price, side="sell", strategy=strategy_runtime)
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
            if strategy_runtime.max_open_positions is not None and sum(len(v) for v in open_lots.values()) >= strategy_runtime.max_open_positions:
                break
            raw_entry_price = get_price(project_root, symbol, trade_date, "open", qlib_csv_dir=qlib_csv_dir)
            if raw_entry_price is None or raw_entry_price <= 0:
                status(f"[warn] missing open price for buy {symbol} on {trade_date.isoformat()}, skipping")
                continue
            if blocked_by_gap_filter(
                project_root,
                symbol,
                trade_date,
                raw_entry_price,
                strategy_runtime,
                qlib_csv_dir=qlib_csv_dir,
            ):
                status(f"[skip] gap filter blocked buy {symbol} on {trade_date.isoformat()}")
                continue
            entry_price = adjusted_execution_price(raw_entry_price, side="buy", strategy=strategy_runtime)
            effective_target_notional = capped_target_notional(strategy_runtime)
            shares = int(effective_target_notional // entry_price)
            if shares <= 0:
                status(f"[warn] zero shares for {symbol} on {trade_date.isoformat()} at price {entry_price}")
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
                target_notional=effective_target_notional,
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
                    target_notional=effective_target_notional,
                )
            ]

        for symbol, lots in open_lots.items():
            close_price = get_price(project_root, symbol, trade_date, "close", qlib_csv_dir=qlib_csv_dir)
            if close_price is None:
                status(f"[warn] missing close price for mark {symbol} on {trade_date.isoformat()}, skipping mark")
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
        last_processed_signal_date = signal_date
        status(
            f"[ok] signal={signal_date.isoformat()} trade={trade_date.isoformat()} "
            f"held={sum(len(v) for v in open_lots.values())}"
        )

    effective_end_date = last_processed_signal_date or existing_end_date or end_date
    if effective_end_date is None:
        raise SystemExit("Unable to determine effective end signal date")
    new_name = updated_run_name(existing_run_name, effective_end_date) if existing_run_name is not None else None
    update_portfolio_run_end_date(
        db_path=settings.db_path,
        portfolio_run_id=portfolio_run_id,
        end_signal_date=effective_end_date.isoformat(),
        name=new_name,
    )

    status(
        f"[ok] portfolio_run_id={portfolio_run_id} processed_signals={processed_signals} "
        f"open_lots={sum(len(v) for v in open_lots.values())}"
    )


if __name__ == "__main__":
    simulate()
