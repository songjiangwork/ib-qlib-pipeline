from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

from sqlalchemy import select

from ..dborm.models import PortfolioRun, Strategy, Universe
from ..dborm.session import create_session_factory_for_path


DEFAULT_STRATEGIES: list[dict[str, Any]] = [
    {
        "key": "top10_hold20_default",
        "name": "Top10 Hold20 Default",
        "strategy_type": "ranking_hold_band",
        "description": "Buy top 10 from prior-day ranking at next open, keep holdings while remaining in top 20.",
        "entry_rule": "Buy when symbol enters top 10 on T-1 ranking and is not already held.",
        "exit_rule": "Exit when symbol falls out of top 20 on a later signal date.",
        "signal_timing": "t_close",
        "execution_timing": "t_plus_1_open",
        "trade_price_basis": "open",
        "buy_top_n": 10,
        "hold_top_n": 20,
        "target_notional": 10000.0,
        "allow_reentry": True,
        "details": {"legacy_portfolio_strategy": "t_minus_1_rank__t_open_execute"},
        "config": {
            "buy_top_n": 10,
            "hold_top_n": 20,
            "target_notional": 10000.0,
            "trade_price_basis": "open",
            "allow_reentry": True,
        },
    }
]


STRATEGY_FIELD_NAMES = [
    "strategy_type",
    "description",
    "entry_rule",
    "exit_rule",
    "signal_timing",
    "execution_timing",
    "trade_price_basis",
    "buy_top_n",
    "hold_top_n",
    "hold_days",
    "target_notional",
    "initial_capital",
    "max_open_positions",
    "max_position_notional",
    "max_position_pct",
    "fee_bps",
    "slippage_bps",
    "gap_up_limit_pct",
    "gap_down_limit_pct",
    "allow_reentry",
]


def _session_for_db(db_path: Path):
    return create_session_factory_for_path(db_path)()


def _strategy_to_dict(row: Strategy) -> dict[str, Any]:
    details = json.loads(row.details_json) if row.details_json else None
    config = json.loads(row.config_json) if row.config_json else None
    return {
        "id": row.id,
        "universe_id": row.universe_id,
        "universe_key": row.universe.key if row.universe is not None else None,
        "universe_name": row.universe.name if row.universe is not None else None,
        "key": row.key,
        "name": row.name,
        "strategy_type": row.strategy_type,
        "description": row.description,
        "entry_rule": row.entry_rule,
        "exit_rule": row.exit_rule,
        "signal_timing": row.signal_timing,
        "execution_timing": row.execution_timing,
        "trade_price_basis": row.trade_price_basis,
        "buy_top_n": row.buy_top_n,
        "hold_top_n": row.hold_top_n,
        "hold_days": row.hold_days,
        "target_notional": row.target_notional,
        "initial_capital": row.initial_capital,
        "max_open_positions": row.max_open_positions,
        "max_position_notional": row.max_position_notional,
        "max_position_pct": row.max_position_pct,
        "fee_bps": row.fee_bps,
        "slippage_bps": row.slippage_bps,
        "gap_up_limit_pct": row.gap_up_limit_pct,
        "gap_down_limit_pct": row.gap_down_limit_pct,
        "allow_reentry": row.allow_reentry,
        "details": details,
        "config": config,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _strategy_snapshot(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "strategy_id": item.get("id"),
        "strategy_key": item.get("key"),
        "strategy_name": item.get("name"),
        "strategy_type": item.get("strategy_type"),
        "entry_rule": item.get("entry_rule"),
        "exit_rule": item.get("exit_rule"),
        "signal_timing": item.get("signal_timing"),
        "execution_timing": item.get("execution_timing"),
        "trade_price_basis": item.get("trade_price_basis"),
        "buy_top_n": item.get("buy_top_n"),
        "hold_top_n": item.get("hold_top_n"),
        "hold_days": item.get("hold_days"),
        "target_notional": item.get("target_notional"),
        "initial_capital": item.get("initial_capital"),
        "max_open_positions": item.get("max_open_positions"),
        "max_position_notional": item.get("max_position_notional"),
        "max_position_pct": item.get("max_position_pct"),
        "fee_bps": item.get("fee_bps"),
        "slippage_bps": item.get("slippage_bps"),
        "gap_up_limit_pct": item.get("gap_up_limit_pct"),
        "gap_down_limit_pct": item.get("gap_down_limit_pct"),
        "allow_reentry": item.get("allow_reentry"),
        "details": item.get("details"),
        "config": item.get("config"),
    }


def ensure_default_strategies(db_path: Path) -> None:
    with _session_for_db(db_path) as session:
        sp500 = session.execute(select(Universe).where(Universe.key == "sp500")).scalar_one_or_none()
        for payload in DEFAULT_STRATEGIES:
            existing = session.execute(select(Strategy).where(Strategy.key == payload["key"])).scalar_one_or_none()
            if existing is None:
                now = dt.datetime.now(dt.timezone.utc).isoformat()
                item = Strategy(
                    universe_id=sp500.id if sp500 is not None else None,
                    key=str(payload["key"]),
                    name=str(payload["name"]),
                    strategy_type=str(payload["strategy_type"]),
                    description=payload.get("description"),
                    entry_rule=payload.get("entry_rule"),
                    exit_rule=payload.get("exit_rule"),
                    signal_timing=payload.get("signal_timing"),
                    execution_timing=payload.get("execution_timing"),
                    trade_price_basis=payload.get("trade_price_basis"),
                    buy_top_n=payload.get("buy_top_n"),
                    hold_top_n=payload.get("hold_top_n"),
                    hold_days=payload.get("hold_days"),
                    target_notional=payload.get("target_notional"),
                    initial_capital=payload.get("initial_capital"),
                    max_open_positions=payload.get("max_open_positions"),
                    max_position_notional=payload.get("max_position_notional"),
                    max_position_pct=payload.get("max_position_pct"),
                    fee_bps=payload.get("fee_bps"),
                    slippage_bps=payload.get("slippage_bps"),
                    gap_up_limit_pct=payload.get("gap_up_limit_pct"),
                    gap_down_limit_pct=payload.get("gap_down_limit_pct"),
                    allow_reentry=payload.get("allow_reentry"),
                    details_json=json.dumps(payload.get("details")) if payload.get("details") is not None else None,
                    config_json=json.dumps(payload.get("config")) if payload.get("config") is not None else None,
                    created_at=now,
                    updated_at=now,
                )
                session.add(item)
        session.commit()


def list_strategies(db_path: Path) -> list[dict[str, Any]]:
    with _session_for_db(db_path) as session:
        rows = session.execute(select(Strategy).outerjoin(Universe).order_by(Strategy.created_at.desc())).scalars().all()
        return [_strategy_to_dict(row) for row in rows]


def get_strategy(db_path: Path, strategy_id: int) -> dict[str, Any] | None:
    with _session_for_db(db_path) as session:
        row = session.get(Strategy, strategy_id)
        if row is None:
            return None
        _ = row.universe
        return _strategy_to_dict(row)


def get_strategy_by_key(db_path: Path, key: str) -> dict[str, Any] | None:
    with _session_for_db(db_path) as session:
        row = session.execute(select(Strategy).where(Strategy.key == key)).scalar_one_or_none()
        if row is None:
            return None
        _ = row.universe
        return _strategy_to_dict(row)


def create_strategy(db_path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    with _session_for_db(db_path) as session:
        item = Strategy(
            universe_id=payload.get("universe_id"),
            key=str(payload["key"]),
            name=str(payload["name"]),
            strategy_type=str(payload["strategy_type"]),
            description=payload.get("description"),
            entry_rule=payload.get("entry_rule"),
            exit_rule=payload.get("exit_rule"),
            signal_timing=payload.get("signal_timing"),
            execution_timing=payload.get("execution_timing"),
            trade_price_basis=payload.get("trade_price_basis"),
            buy_top_n=payload.get("buy_top_n"),
            hold_top_n=payload.get("hold_top_n"),
            hold_days=payload.get("hold_days"),
            target_notional=payload.get("target_notional"),
            initial_capital=payload.get("initial_capital"),
            max_open_positions=payload.get("max_open_positions"),
            max_position_notional=payload.get("max_position_notional"),
            max_position_pct=payload.get("max_position_pct"),
            fee_bps=payload.get("fee_bps"),
            slippage_bps=payload.get("slippage_bps"),
            gap_up_limit_pct=payload.get("gap_up_limit_pct"),
            gap_down_limit_pct=payload.get("gap_down_limit_pct"),
            allow_reentry=payload.get("allow_reentry"),
            details_json=json.dumps(payload["details"]) if payload.get("details") is not None else None,
            config_json=json.dumps(payload["config"]) if payload.get("config") is not None else None,
            created_at=now,
            updated_at=now,
        )
        session.add(item)
        session.commit()
        session.refresh(item)
        _ = item.universe
        return _strategy_to_dict(item)


def update_strategy(db_path: Path, strategy_id: int, payload: dict[str, Any]) -> dict[str, Any] | None:
    with _session_for_db(db_path) as session:
        item = session.get(Strategy, strategy_id)
        if item is None:
            return None
        for name in ["universe_id", "key", "name", *STRATEGY_FIELD_NAMES]:
            if name in payload:
                setattr(item, name, payload[name])
        if "details" in payload:
            item.details_json = json.dumps(payload["details"]) if payload["details"] is not None else None
        if "config" in payload:
            item.config_json = json.dumps(payload["config"]) if payload["config"] is not None else None
        item.updated_at = dt.datetime.now(dt.timezone.utc).isoformat()
        session.commit()
        session.refresh(item)
        _ = item.universe
        return _strategy_to_dict(item)


def strategy_snapshot_for_portfolio(db_path: Path, strategy_id: int | None) -> dict[str, Any] | None:
    if strategy_id is None:
        return None
    item = get_strategy(db_path, strategy_id)
    if item is None:
        return None
    return _strategy_snapshot(item)


def backfill_portfolio_run_strategies(db_path: Path) -> None:
    default_strategy = get_strategy_by_key(db_path, "top10_hold20_default")
    if default_strategy is None:
        return
    snapshot_json = json.dumps(_strategy_snapshot(default_strategy))
    with _session_for_db(db_path) as session:
        rows = session.execute(select(PortfolioRun)).scalars().all()
        changed = False
        for row in rows:
            if row.strategy_id is None and row.buy_top_n == 10 and row.hold_top_n == 20 and abs(float(row.target_notional) - 10000.0) < 1e-9:
                row.strategy_id = int(default_strategy["id"])
                changed = True
            if row.strategy_config_json is None and row.strategy_id == default_strategy["id"]:
                row.strategy_config_json = snapshot_json
                changed = True
        if changed:
            session.commit()
