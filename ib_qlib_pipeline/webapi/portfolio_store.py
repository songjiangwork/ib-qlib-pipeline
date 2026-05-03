from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .db import connect


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
    with connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO portfolio_runs (
                name, strategy, buy_top_n, hold_top_n, target_notional,
                start_signal_date, end_signal_date, created_at, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                name,
                strategy,
                buy_top_n,
                hold_top_n,
                target_notional,
                start_signal_date,
                end_signal_date,
                created_at,
                notes,
            ),
        )
        return int(cursor.lastrowid)


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
    with connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO portfolio_lots (
                portfolio_run_id, symbol, entry_run_id, entry_signal_date, entry_trade_date,
                entry_rank, entry_price_open, shares, target_notional, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')
            """,
            (
                portfolio_run_id,
                symbol,
                entry_run_id,
                entry_signal_date,
                entry_trade_date,
                entry_rank,
                entry_price_open,
                shares,
                target_notional,
            ),
        )
        return int(cursor.lastrowid)


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
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT entry_price_open, shares
            FROM portfolio_lots
            WHERE id = ?
            """,
            (lot_id,),
        ).fetchone()
        if row is None:
            raise RuntimeError(f"Lot {lot_id} not found")
        entry_price = float(row["entry_price_open"])
        shares = int(row["shares"])
        realized_pnl = (exit_price_open - entry_price) * shares
        realized_return_pct = ((exit_price_open / entry_price) - 1.0) * 100.0
        conn.execute(
            """
            UPDATE portfolio_lots
            SET exit_run_id = ?,
                exit_signal_date = ?,
                exit_trade_date = ?,
                exit_rank = ?,
                exit_price_open = ?,
                realized_pnl = ?,
                realized_return_pct = ?,
                status = 'closed'
            WHERE id = ?
            """,
            (
                exit_run_id,
                exit_signal_date,
                exit_trade_date,
                exit_rank,
                exit_price_open,
                realized_pnl,
                realized_return_pct,
                lot_id,
            ),
        )


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
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio_marks (
                portfolio_lot_id, trade_date, close_price, market_value,
                unrealized_pnl, unrealized_return_pct, is_in_top20, is_in_top10
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                portfolio_lot_id,
                trade_date,
                close_price,
                market_value,
                unrealized_pnl,
                unrealized_return_pct,
                1 if is_in_top20 else 0,
                1 if is_in_top10 else 0,
            ),
        )


def list_portfolio_runs(db_path: Path) -> list[dict[str, Any]]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT pr.*,
                   COUNT(pl.id) AS lot_count,
                   SUM(CASE WHEN pl.status = 'open' THEN 1 ELSE 0 END) AS open_lot_count,
                   MIN(r.model_id) AS model_id,
                   MAX(r.model_id) AS max_model_id,
                   MIN(m.key) AS model_key,
                   MIN(m.name) AS model_name,
                   MIN(m.model_class) AS model_class
            FROM portfolio_runs pr
            LEFT JOIN portfolio_lots pl ON pl.portfolio_run_id = pr.id
            LEFT JOIN runs r ON r.id = pl.entry_run_id
            LEFT JOIN models m ON m.id = r.model_id
            GROUP BY pr.id
            ORDER BY pr.created_at DESC
            """
        ).fetchall()
    items = []
    for row in rows:
        item = dict(row)
        if item.get("model_id") != item.get("max_model_id"):
            item["model_key"] = "mixed"
            item["model_name"] = "Mixed"
            item["model_class"] = None
        item.pop("max_model_id", None)
        items.append(item)
    return items


def get_portfolio_run(db_path: Path, portfolio_run_id: int) -> dict[str, Any] | None:
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT pr.*,
                   COUNT(pl.id) AS lot_count,
                   SUM(CASE WHEN pl.status = 'open' THEN 1 ELSE 0 END) AS open_lot_count,
                   MIN(r.model_id) AS model_id,
                   MAX(r.model_id) AS max_model_id,
                   MIN(m.key) AS model_key,
                   MIN(m.name) AS model_name,
                   MIN(m.model_class) AS model_class
            FROM portfolio_runs pr
            LEFT JOIN portfolio_lots pl ON pl.portfolio_run_id = pr.id
            LEFT JOIN runs r ON r.id = pl.entry_run_id
            LEFT JOIN models m ON m.id = r.model_id
            WHERE pr.id = ?
            GROUP BY pr.id
            """,
            (portfolio_run_id,),
        ).fetchone()
    if row is None:
        return None
    item = dict(row)
    if item.get("model_id") != item.get("max_model_id"):
        item["model_key"] = "mixed"
        item["model_name"] = "Mixed"
        item["model_class"] = None
    item.pop("max_model_id", None)
    return item


def list_portfolio_lots(
    db_path: Path,
    portfolio_run_id: int,
    *,
    symbol: str | None = None,
    status: str | None = None,
) -> list[dict[str, Any]]:
    query = """
        SELECT *
        FROM portfolio_lots
        WHERE portfolio_run_id = ?
    """
    params: list[Any] = [portfolio_run_id]
    if symbol is not None:
        query += " AND symbol = ?"
        params.append(symbol)
    if status is not None:
        query += " AND status = ?"
        params.append(status)
    query += " ORDER BY entry_trade_date, symbol, id"
    with connect(db_path) as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(row) for row in rows]


def list_portfolio_marks(db_path: Path, lot_id: int) -> list[dict[str, Any]]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM portfolio_marks
            WHERE portfolio_lot_id = ?
            ORDER BY trade_date
            """,
            (lot_id,),
        ).fetchall()
    return [dict(row) for row in rows]
