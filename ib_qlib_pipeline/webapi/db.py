from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    timezone TEXT NOT NULL,
    day_of_week TEXT NOT NULL DEFAULT 'mon-fri',
    hour INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    client_id INTEGER NOT NULL DEFAULT 151,
    lookback_days INTEGER NOT NULL DEFAULT 7,
    workflow_base TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_triggered_at TEXT,
    last_run_id INTEGER,
    last_run_status TEXT
);

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    schedule_id INTEGER REFERENCES schedules(id) ON DELETE SET NULL,
    trigger_source TEXT NOT NULL,
    status TEXT NOT NULL,
    client_id INTEGER NOT NULL,
    lookback_days INTEGER NOT NULL,
    workflow_base TEXT NOT NULL,
    command TEXT NOT NULL,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    signal_date TEXT,
    ranking_csv_path TEXT,
    html_report_path TEXT,
    experiment_id TEXT,
    recorder_id TEXT,
    row_count INTEGER,
    log_output TEXT,
    error_text TEXT
);

CREATE TABLE IF NOT EXISTS recommendations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    rank INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    score REAL NOT NULL,
    percentile REAL,
    entry_price REAL,
    signal_date TEXT NOT NULL,
    UNIQUE(run_id, rank)
);

CREATE TABLE IF NOT EXISTS portfolio_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    strategy TEXT NOT NULL,
    buy_top_n INTEGER NOT NULL,
    hold_top_n INTEGER NOT NULL,
    target_notional REAL NOT NULL,
    start_signal_date TEXT NOT NULL,
    end_signal_date TEXT,
    created_at TEXT NOT NULL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS portfolio_lots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_run_id INTEGER NOT NULL REFERENCES portfolio_runs(id) ON DELETE CASCADE,
    symbol TEXT NOT NULL,
    entry_run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    entry_signal_date TEXT NOT NULL,
    entry_trade_date TEXT NOT NULL,
    entry_rank INTEGER NOT NULL,
    entry_price_open REAL NOT NULL,
    shares INTEGER NOT NULL,
    target_notional REAL NOT NULL,
    exit_run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL,
    exit_signal_date TEXT,
    exit_trade_date TEXT,
    exit_rank INTEGER,
    exit_price_open REAL,
    realized_pnl REAL,
    realized_return_pct REAL,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS portfolio_marks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_lot_id INTEGER NOT NULL REFERENCES portfolio_lots(id) ON DELETE CASCADE,
    trade_date TEXT NOT NULL,
    close_price REAL NOT NULL,
    market_value REAL NOT NULL,
    unrealized_pnl REAL NOT NULL,
    unrealized_return_pct REAL NOT NULL,
    is_in_top20 INTEGER NOT NULL,
    is_in_top10 INTEGER NOT NULL,
    UNIQUE(portfolio_lot_id, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_signal_date ON runs(signal_date DESC);
CREATE INDEX IF NOT EXISTS idx_recommendations_run_id ON recommendations(run_id, rank);
CREATE INDEX IF NOT EXISTS idx_recommendations_symbol_date ON recommendations(symbol, signal_date);
CREATE INDEX IF NOT EXISTS idx_portfolio_runs_created_at ON portfolio_runs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_portfolio_lots_run_symbol ON portfolio_lots(portfolio_run_id, symbol, status);
CREATE INDEX IF NOT EXISTS idx_portfolio_lots_entry_date ON portfolio_lots(entry_trade_date);
CREATE INDEX IF NOT EXISTS idx_portfolio_marks_lot_date ON portfolio_marks(portfolio_lot_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_portfolio_marks_trade_date ON portfolio_marks(trade_date);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)


def rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]
