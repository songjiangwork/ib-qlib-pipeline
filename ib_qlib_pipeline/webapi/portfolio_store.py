from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import case, func, select

from ..dborm.models import ModelRef, PortfolioLot, PortfolioMark, PortfolioRun, Run
from ..dborm.session import create_session_factory_for_path


@dataclass
class OpenLot:
    id: int
    symbol: str
    entry_run_id: int
    entry_signal_date: str
    entry_trade_date: str
    entry_rank: int
    entry_price_open: float
    shares: int
    target_notional: float


def _session_for_db(db_path: Path):
    return create_session_factory_for_path(db_path)()


def _portfolio_summary_query():
    return (
        select(
            PortfolioRun.id,
            PortfolioRun.name,
            PortfolioRun.strategy,
            PortfolioRun.buy_top_n,
            PortfolioRun.hold_top_n,
            PortfolioRun.target_notional,
            PortfolioRun.start_signal_date,
            PortfolioRun.end_signal_date,
            PortfolioRun.created_at,
            PortfolioRun.notes,
            func.count(PortfolioLot.id).label("lot_count"),
            func.sum(case((PortfolioLot.status == "open", 1), else_=0)).label("open_lot_count"),
            func.sum(case((PortfolioLot.status == "closed", 1), else_=0)).label("closed_lot_count"),
            func.avg(
                case(
                    (PortfolioLot.exit_trade_date.is_not(None), func.julianday(PortfolioLot.exit_trade_date) - func.julianday(PortfolioLot.entry_trade_date)),
                    else_=None,
                )
            ).label("avg_hold_days"),
            func.avg(PortfolioLot.realized_return_pct).label("avg_return_pct"),
            func.sum(case((PortfolioLot.realized_pnl.is_not(None), PortfolioLot.realized_pnl), else_=0)).label("total_realized_pnl"),
            func.sum(case((PortfolioLot.realized_return_pct.is_not(None), 1), else_=0)).label("closed_with_return_count"),
            func.sum(case((PortfolioLot.realized_return_pct > 0, 1), else_=0)).label("win_lot_count"),
            func.min(Run.model_id).label("model_id"),
            func.max(Run.model_id).label("max_model_id"),
            func.min(ModelRef.key).label("model_key"),
            func.min(ModelRef.name).label("model_name"),
            func.min(ModelRef.model_class).label("model_class"),
            func.min(ModelRef.workflow_base).label("workflow_base"),
        )
        .select_from(PortfolioRun)
        .outerjoin(PortfolioLot, PortfolioLot.portfolio_run_id == PortfolioRun.id)
        .outerjoin(Run, Run.id == PortfolioLot.entry_run_id)
        .outerjoin(ModelRef, ModelRef.id == Run.model_id)
        .group_by(PortfolioRun.id)
    )


def _normalize_summary_row(row: Any) -> dict[str, Any]:
    item = dict(row._mapping)
    closed_with_return_count = int(item.get("closed_with_return_count") or 0)
    win_lot_count = int(item.get("win_lot_count") or 0)
    item["win_rate_pct"] = (win_lot_count / closed_with_return_count * 100.0) if closed_with_return_count else None
    if item.get("model_id") != item.get("max_model_id"):
        item["model_key"] = "mixed"
        item["model_name"] = "Mixed"
        item["model_class"] = None
        item["workflow_base"] = None
    item.pop("max_model_id", None)
    item.pop("closed_with_return_count", None)
    item.pop("win_lot_count", None)
    return item


def _lot_to_dict(lot: PortfolioLot) -> dict[str, Any]:
    return {
        "id": lot.id,
        "portfolio_run_id": lot.portfolio_run_id,
        "symbol": lot.symbol,
        "entry_run_id": lot.entry_run_id,
        "entry_signal_date": lot.entry_signal_date,
        "entry_trade_date": lot.entry_trade_date,
        "entry_rank": lot.entry_rank,
        "entry_price_open": lot.entry_price_open,
        "shares": lot.shares,
        "target_notional": lot.target_notional,
        "exit_run_id": lot.exit_run_id,
        "exit_signal_date": lot.exit_signal_date,
        "exit_trade_date": lot.exit_trade_date,
        "exit_rank": lot.exit_rank,
        "exit_price_open": lot.exit_price_open,
        "realized_pnl": lot.realized_pnl,
        "realized_return_pct": lot.realized_return_pct,
        "status": lot.status,
    }


def _mark_to_dict(mark: PortfolioMark) -> dict[str, Any]:
    return {
        "id": mark.id,
        "portfolio_lot_id": mark.portfolio_lot_id,
        "trade_date": mark.trade_date,
        "close_price": mark.close_price,
        "market_value": mark.market_value,
        "unrealized_pnl": mark.unrealized_pnl,
        "unrealized_return_pct": mark.unrealized_return_pct,
        "is_in_top20": mark.is_in_top20,
        "is_in_top10": mark.is_in_top10,
    }


def create_portfolio_run(
    *,
    db_path: Path,
    name: str,
    strategy: str,
    buy_top_n: int,
    hold_top_n: int,
    target_notional: float,
    start_signal_date: str,
    end_signal_date: str | None,
    notes: str | None = None,
) -> int:
    created_at = dt.datetime.now(dt.timezone.utc).isoformat()
    with _session_for_db(db_path) as session:
        run = PortfolioRun(
            name=name,
            strategy=strategy,
            buy_top_n=buy_top_n,
            hold_top_n=hold_top_n,
            target_notional=target_notional,
            start_signal_date=start_signal_date,
            end_signal_date=end_signal_date,
            created_at=created_at,
            notes=notes,
        )
        session.add(run)
        session.commit()
        session.refresh(run)
        return int(run.id)


def update_portfolio_run_end_date(
    *,
    db_path: Path,
    portfolio_run_id: int,
    end_signal_date: str,
    name: str | None = None,
) -> None:
    with _session_for_db(db_path) as session:
        run = session.get(PortfolioRun, portfolio_run_id)
        if run is None:
            return
        run.end_signal_date = end_signal_date
        if name is not None:
            run.name = name
        session.commit()


def load_open_lots(db_path: Path, portfolio_run_id: int) -> dict[str, list[OpenLot]]:
    with _session_for_db(db_path) as session:
        rows = session.execute(
            select(PortfolioLot)
            .where(
                PortfolioLot.portfolio_run_id == portfolio_run_id,
                PortfolioLot.status == "open",
            )
            .order_by(PortfolioLot.symbol, PortfolioLot.id)
        ).scalars().all()
    lots: dict[str, list[OpenLot]] = {}
    for row in rows:
        lot = OpenLot(
            id=int(row.id),
            symbol=str(row.symbol),
            entry_run_id=int(row.entry_run_id),
            entry_signal_date=str(row.entry_signal_date),
            entry_trade_date=str(row.entry_trade_date),
            entry_rank=int(row.entry_rank),
            entry_price_open=float(row.entry_price_open),
            shares=int(row.shares),
            target_notional=float(row.target_notional),
        )
        lots.setdefault(lot.symbol, []).append(lot)
    return lots


def insert_portfolio_lot(
    *,
    db_path: Path,
    portfolio_run_id: int,
    symbol: str,
    entry_run_id: int,
    entry_signal_date: str,
    entry_trade_date: str,
    entry_rank: int,
    entry_price_open: float,
    shares: int,
    target_notional: float,
) -> int:
    with _session_for_db(db_path) as session:
        lot = PortfolioLot(
            portfolio_run_id=portfolio_run_id,
            symbol=symbol,
            entry_run_id=entry_run_id,
            entry_signal_date=entry_signal_date,
            entry_trade_date=entry_trade_date,
            entry_rank=entry_rank,
            entry_price_open=entry_price_open,
            shares=shares,
            target_notional=target_notional,
            status="open",
        )
        session.add(lot)
        session.commit()
        session.refresh(lot)
        return int(lot.id)


def close_portfolio_lot(
    *,
    db_path: Path,
    lot_id: int,
    exit_run_id: int,
    exit_signal_date: str,
    exit_trade_date: str,
    exit_rank: int | None,
    exit_price_open: float,
) -> None:
    with _session_for_db(db_path) as session:
        lot = session.get(PortfolioLot, lot_id)
        if lot is None:
            raise RuntimeError(f"Lot {lot_id} not found")
        entry_price = float(lot.entry_price_open)
        shares = int(lot.shares)
        lot.exit_run_id = exit_run_id
        lot.exit_signal_date = exit_signal_date
        lot.exit_trade_date = exit_trade_date
        lot.exit_rank = exit_rank
        lot.exit_price_open = exit_price_open
        lot.realized_pnl = (exit_price_open - entry_price) * shares
        lot.realized_return_pct = ((exit_price_open / entry_price) - 1.0) * 100.0
        lot.status = "closed"
        session.commit()


def insert_portfolio_mark(
    *,
    db_path: Path,
    portfolio_lot_id: int,
    trade_date: str,
    close_price: float,
    market_value: float,
    unrealized_pnl: float,
    unrealized_return_pct: float,
    is_in_top20: bool,
    is_in_top10: bool,
) -> None:
    with _session_for_db(db_path) as session:
        existing = session.execute(
            select(PortfolioMark).where(
                PortfolioMark.portfolio_lot_id == portfolio_lot_id,
                PortfolioMark.trade_date == trade_date,
            )
        ).scalar_one_or_none()
        if existing is None:
            existing = PortfolioMark(
                portfolio_lot_id=portfolio_lot_id,
                trade_date=trade_date,
                close_price=close_price,
                market_value=market_value,
                unrealized_pnl=unrealized_pnl,
                unrealized_return_pct=unrealized_return_pct,
                is_in_top20=1 if is_in_top20 else 0,
                is_in_top10=1 if is_in_top10 else 0,
            )
            session.add(existing)
        else:
            existing.close_price = close_price
            existing.market_value = market_value
            existing.unrealized_pnl = unrealized_pnl
            existing.unrealized_return_pct = unrealized_return_pct
            existing.is_in_top20 = 1 if is_in_top20 else 0
            existing.is_in_top10 = 1 if is_in_top10 else 0
        session.commit()


def list_portfolio_runs(db_path: Path) -> list[dict[str, Any]]:
    with _session_for_db(db_path) as session:
        rows = session.execute(_portfolio_summary_query().order_by(PortfolioRun.created_at.desc())).all()
    return [_normalize_summary_row(row) for row in rows]


def get_portfolio_run(db_path: Path, portfolio_run_id: int) -> dict[str, Any] | None:
    with _session_for_db(db_path) as session:
        row = session.execute(
            _portfolio_summary_query().where(PortfolioRun.id == portfolio_run_id)
        ).first()
    if row is None:
        return None
    return _normalize_summary_row(row)


def list_portfolio_lots(
    db_path: Path,
    portfolio_run_id: int,
    *,
    symbol: str | None = None,
    status: str | None = None,
) -> list[dict[str, Any]]:
    query = select(PortfolioLot).where(PortfolioLot.portfolio_run_id == portfolio_run_id)
    if symbol is not None:
        query = query.where(PortfolioLot.symbol == symbol)
    if status is not None:
        query = query.where(PortfolioLot.status == status)
    query = query.order_by(PortfolioLot.entry_trade_date, PortfolioLot.symbol, PortfolioLot.id)
    with _session_for_db(db_path) as session:
        rows = session.execute(query).scalars().all()
    return [_lot_to_dict(row) for row in rows]


def list_portfolio_marks(db_path: Path, lot_id: int) -> list[dict[str, Any]]:
    with _session_for_db(db_path) as session:
        rows = session.execute(
            select(PortfolioMark)
            .where(PortfolioMark.portfolio_lot_id == lot_id)
            .order_by(PortfolioMark.trade_date)
        ).scalars().all()
    return [_mark_to_dict(row) for row in rows]
