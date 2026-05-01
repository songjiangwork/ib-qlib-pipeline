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

CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_signal_date ON runs(signal_date DESC);
CREATE INDEX IF NOT EXISTS idx_recommendations_run_id ON recommendations(run_id, rank);
CREATE INDEX IF NOT EXISTS idx_recommendations_symbol_date ON recommendations(symbol, signal_date);
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
