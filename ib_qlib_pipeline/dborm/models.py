from __future__ import annotations

from typing import Optional

from sqlalchemy import Float, ForeignKey, Index, Integer, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


class ModelRef(Base):
    __tablename__ = "models"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    universe_id: Mapped[Optional[int]] = mapped_column(ForeignKey("universes.id", ondelete="SET NULL"), nullable=True)
    key: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    model_class: Mapped[str] = mapped_column(Text, nullable=False)
    module_path: Mapped[str] = mapped_column(Text, nullable=False)
    workflow_base: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    details_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[str] = mapped_column(Text, nullable=False)

    universe: Mapped[Optional["Universe"]] = relationship(back_populates="models")
    runs: Mapped[list["Run"]] = relationship(back_populates="model")


class Universe(Base):
    __tablename__ = "universes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    symbols_file: Mapped[str] = mapped_column(Text, nullable=False)
    symbol_count: Mapped[int] = mapped_column(Integer, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    details_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[str] = mapped_column(Text, nullable=False)

    models: Mapped[list[ModelRef]] = relationship(back_populates="universe")
    symbols: Mapped[list["UniverseSymbol"]] = relationship(back_populates="universe", cascade="all, delete-orphan")
    runs: Mapped[list["Run"]] = relationship(back_populates="universe")
    portfolio_runs: Mapped[list["PortfolioRun"]] = relationship(back_populates="universe")
    strategies: Mapped[list["Strategy"]] = relationship(back_populates="universe")


class UniverseSymbol(Base):
    __tablename__ = "universe_symbols"
    __table_args__ = (
        UniqueConstraint("universe_id", "symbol"),
        Index("idx_universe_symbols_universe_order", "universe_id", "sort_order"),
        Index("idx_universe_symbols_symbol", "symbol"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    universe_id: Mapped[int] = mapped_column(ForeignKey("universes.id", ondelete="CASCADE"), nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    ib_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False)
    source_line: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[str] = mapped_column(Text, nullable=False)

    universe: Mapped[Universe] = relationship(back_populates="symbols")


class Schedule(Base):
    __tablename__ = "schedules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    schedule_type: Mapped[str] = mapped_column(Text, nullable=False, default="ranking", server_default="ranking")
    enabled: Mapped[bool] = mapped_column(nullable=False, default=True, server_default="1")
    timezone: Mapped[str] = mapped_column(Text, nullable=False)
    day_of_week: Mapped[str] = mapped_column(Text, nullable=False, default="mon-fri", server_default="mon-fri")
    hour: Mapped[int] = mapped_column(Integer, nullable=False)
    minute: Mapped[int] = mapped_column(Integer, nullable=False)
    client_id: Mapped[int] = mapped_column(Integer, nullable=False, default=151, server_default="151")
    lookback_days: Mapped[int] = mapped_column(Integer, nullable=False, default=7, server_default="7")
    workflow_base: Mapped[str] = mapped_column(Text, nullable=False)
    pipeline_start_date: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    pipeline_include_portfolio: Mapped[bool] = mapped_column(nullable=False, default=True, server_default="1")
    created_at: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[str] = mapped_column(Text, nullable=False)
    last_triggered_at: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    last_run_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    last_run_status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    runs: Mapped[list["Run"]] = relationship(back_populates="schedule")


class Run(Base):
    __tablename__ = "runs"

    __table_args__ = (
        Index("idx_runs_created_at", "created_at"),
        Index("idx_runs_signal_date", "signal_date"),
        Index("idx_runs_model_id", "model_id", "signal_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    schedule_id: Mapped[Optional[int]] = mapped_column(ForeignKey("schedules.id", ondelete="SET NULL"), nullable=True)
    model_id: Mapped[Optional[int]] = mapped_column(ForeignKey("models.id", ondelete="SET NULL"), nullable=True)
    universe_id: Mapped[Optional[int]] = mapped_column(ForeignKey("universes.id", ondelete="SET NULL"), nullable=True)
    trigger_source: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    client_id: Mapped[int] = mapped_column(Integer, nullable=False)
    lookback_days: Mapped[int] = mapped_column(Integer, nullable=False)
    workflow_base: Mapped[str] = mapped_column(Text, nullable=False)
    command: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    finished_at: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    signal_date: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ranking_csv_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    html_report_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    experiment_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    recorder_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    row_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    log_output: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    schedule: Mapped[Optional[Schedule]] = relationship(back_populates="runs")
    model: Mapped[Optional[ModelRef]] = relationship(back_populates="runs")
    universe: Mapped[Optional[Universe]] = relationship(back_populates="runs")
    recommendations: Mapped[list["Recommendation"]] = relationship(back_populates="run", cascade="all, delete-orphan")


class Recommendation(Base):
    __tablename__ = "recommendations"
    __table_args__ = (
        UniqueConstraint("run_id", "rank"),
        Index("idx_recommendations_run_id", "run_id", "rank"),
        Index("idx_recommendations_symbol_date", "symbol", "signal_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("runs.id", ondelete="CASCADE"), nullable=False)
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    percentile: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    entry_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    signal_date: Mapped[str] = mapped_column(Text, nullable=False)

    run: Mapped[Run] = relationship(back_populates="recommendations")


class Strategy(Base):
    __tablename__ = "strategies"
    __table_args__ = (Index("idx_strategies_universe_id", "universe_id", "key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    universe_id: Mapped[Optional[int]] = mapped_column(ForeignKey("universes.id", ondelete="SET NULL"), nullable=True)
    key: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    strategy_type: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    entry_rule: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    exit_rule: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    signal_timing: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    execution_timing: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    trade_price_basis: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    buy_top_n: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    hold_top_n: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    hold_days: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    target_notional: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    initial_capital: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    max_open_positions: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    max_position_notional: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    max_position_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    fee_bps: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    slippage_bps: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    gap_up_limit_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    gap_down_limit_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    allow_reentry: Mapped[Optional[bool]] = mapped_column(nullable=True)
    details_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    config_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[str] = mapped_column(Text, nullable=False)

    universe: Mapped[Optional[Universe]] = relationship(back_populates="strategies")
    portfolio_runs: Mapped[list["PortfolioRun"]] = relationship(back_populates="strategy_ref")


class PortfolioRun(Base):
    __tablename__ = "portfolio_runs"
    __table_args__ = (Index("idx_portfolio_runs_created_at", "created_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    universe_id: Mapped[Optional[int]] = mapped_column(ForeignKey("universes.id", ondelete="SET NULL"), nullable=True)
    strategy_id: Mapped[Optional[int]] = mapped_column(ForeignKey("strategies.id", ondelete="SET NULL"), nullable=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    strategy: Mapped[str] = mapped_column(Text, nullable=False)
    buy_top_n: Mapped[int] = mapped_column(Integer, nullable=False)
    hold_top_n: Mapped[int] = mapped_column(Integer, nullable=False)
    target_notional: Mapped[float] = mapped_column(Float, nullable=False)
    strategy_config_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    start_signal_date: Mapped[str] = mapped_column(Text, nullable=False)
    end_signal_date: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[str] = mapped_column(Text, nullable=False)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    universe: Mapped[Optional[Universe]] = relationship(back_populates="portfolio_runs")
    strategy_ref: Mapped[Optional[Strategy]] = relationship(back_populates="portfolio_runs")
    lots: Mapped[list["PortfolioLot"]] = relationship(back_populates="portfolio_run", cascade="all, delete-orphan")


class PortfolioLot(Base):
    __tablename__ = "portfolio_lots"
    __table_args__ = (
        Index("idx_portfolio_lots_run_symbol", "portfolio_run_id", "symbol", "status"),
        Index("idx_portfolio_lots_entry_date", "entry_trade_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    portfolio_run_id: Mapped[int] = mapped_column(ForeignKey("portfolio_runs.id", ondelete="CASCADE"), nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    entry_run_id: Mapped[int] = mapped_column(ForeignKey("runs.id", ondelete="CASCADE"), nullable=False)
    entry_signal_date: Mapped[str] = mapped_column(Text, nullable=False)
    entry_trade_date: Mapped[str] = mapped_column(Text, nullable=False)
    entry_rank: Mapped[int] = mapped_column(Integer, nullable=False)
    entry_price_open: Mapped[float] = mapped_column(Float, nullable=False)
    shares: Mapped[int] = mapped_column(Integer, nullable=False)
    target_notional: Mapped[float] = mapped_column(Float, nullable=False)
    exit_run_id: Mapped[Optional[int]] = mapped_column(ForeignKey("runs.id", ondelete="SET NULL"), nullable=True)
    exit_signal_date: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    exit_trade_date: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    exit_rank: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    exit_price_open: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    realized_pnl: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    realized_return_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)

    portfolio_run: Mapped[PortfolioRun] = relationship(back_populates="lots")
    marks: Mapped[list["PortfolioMark"]] = relationship(back_populates="lot", cascade="all, delete-orphan")


class PortfolioMark(Base):
    __tablename__ = "portfolio_marks"
    __table_args__ = (
        UniqueConstraint("portfolio_lot_id", "trade_date"),
        Index("idx_portfolio_marks_lot_date", "portfolio_lot_id", "trade_date"),
        Index("idx_portfolio_marks_trade_date", "trade_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    portfolio_lot_id: Mapped[int] = mapped_column(ForeignKey("portfolio_lots.id", ondelete="CASCADE"), nullable=False)
    trade_date: Mapped[str] = mapped_column(Text, nullable=False)
    close_price: Mapped[float] = mapped_column(Float, nullable=False)
    market_value: Mapped[float] = mapped_column(Float, nullable=False)
    unrealized_pnl: Mapped[float] = mapped_column(Float, nullable=False)
    unrealized_return_pct: Mapped[float] = mapped_column(Float, nullable=False)
    is_in_top20: Mapped[int] = mapped_column(Integer, nullable=False)
    is_in_top10: Mapped[int] = mapped_column(Integer, nullable=False)

    lot: Mapped[PortfolioLot] = relationship(back_populates="marks")


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (Index("idx_jobs_created_at", "created_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_type: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    finished_at: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    requested_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    log_output: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    steps: Mapped[list["JobStep"]] = relationship(back_populates="job", cascade="all, delete-orphan")


class JobStep(Base):
    __tablename__ = "job_steps"
    __table_args__ = (Index("idx_job_steps_job_order", "job_id", "step_order"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    step_order: Mapped[int] = mapped_column(Integer, nullable=False)
    step_name: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    command: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    started_at: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    finished_at: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    log_output: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    job: Mapped[Job] = relationship(back_populates="steps")


class PriceDaily(Base):
    __tablename__ = "prices_daily"
    __table_args__ = (
        UniqueConstraint("symbol", "trade_date"),
        Index("idx_prices_daily_symbol_date", "symbol", "trade_date"),
        Index("idx_prices_daily_trade_date", "trade_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    trade_date: Mapped[str] = mapped_column(Text, nullable=False)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    factor: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    source: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[str] = mapped_column(Text, nullable=False)
