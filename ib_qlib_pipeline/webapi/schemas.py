from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class ScheduleCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    schedule_type: Literal["ranking", "daily_close_pipeline"] = "daily_close_pipeline"
    hour: int = Field(ge=0, le=23)
    minute: int = Field(ge=0, le=59)
    day_of_week: str = Field(default="mon-fri")
    timezone: str = Field(default="America/Edmonton")
    client_id: int = Field(default=151, ge=0)
    lookback_days: int = Field(default=7, ge=1, le=90)
    workflow_base: str = Field(default="examples/workflow_us_lgb_2020_port.yaml")
    pipeline_start_date: str | None = None
    pipeline_include_portfolio: bool = True
    enabled: bool = True


class ScheduleUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    schedule_type: Literal["ranking", "daily_close_pipeline"] | None = None
    hour: int | None = Field(default=None, ge=0, le=23)
    minute: int | None = Field(default=None, ge=0, le=59)
    day_of_week: str | None = None
    timezone: str | None = None
    client_id: int | None = Field(default=None, ge=0)
    lookback_days: int | None = Field(default=None, ge=1, le=90)
    workflow_base: str | None = None
    pipeline_start_date: str | None = None
    pipeline_include_portfolio: bool | None = None
    enabled: bool | None = None


class ManualRunRequest(BaseModel):
    client_id: int = Field(default=151, ge=0)
    lookback_days: int = Field(default=7, ge=1, le=90)
    workflow_base: str = Field(default="examples/workflow_us_lgb_2020_port.yaml")


class RunQuery(BaseModel):
    limit: int = Field(default=50, ge=1, le=500)
    status: Literal["queued", "running", "succeeded", "failed"] | None = None
    signal_date: str | None = None


class DataRefreshJobRequest(BaseModel):
    client_id: int = Field(default=151, ge=0)
    start_date: str


class RankingBackfillJobRequest(BaseModel):
    signal_date: str
    model_id: int = Field(ge=1)
    client_id: int = Field(default=151, ge=0)


class PortfolioAppendJobRequest(BaseModel):
    portfolio_run_id: int = Field(ge=1)
    model_id: int | None = Field(default=None, ge=1)
    end_date: str


class DailyClosePipelineJobRequest(BaseModel):
    trade_date: str
    client_id: int = Field(default=151, ge=0)
    start_date: str
    include_portfolio: bool = True


class JobCancelRequest(BaseModel):
    reason: str | None = Field(default=None, max_length=500)


class UniverseCreate(BaseModel):
    key: str = Field(min_length=1, max_length=120)
    name: str = Field(min_length=1, max_length=160)
    symbols_file: str = Field(min_length=1, max_length=500)
    description: str | None = Field(default=None, max_length=500)
    details: dict[str, Any] | None = None


class UniverseUpdate(BaseModel):
    key: str | None = Field(default=None, min_length=1, max_length=120)
    name: str | None = Field(default=None, min_length=1, max_length=160)
    symbols_file: str | None = Field(default=None, min_length=1, max_length=500)
    description: str | None = Field(default=None, max_length=500)
    details: dict[str, Any] | None = None


class StrategyCreate(BaseModel):
    universe_id: int | None = Field(default=None, ge=1)
    key: str = Field(min_length=1, max_length=160)
    name: str = Field(min_length=1, max_length=200)
    strategy_type: str = Field(min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=1000)
    entry_rule: str | None = None
    exit_rule: str | None = None
    signal_timing: str | None = Field(default=None, max_length=120)
    execution_timing: str | None = Field(default=None, max_length=120)
    trade_price_basis: str | None = Field(default=None, max_length=120)
    buy_top_n: int | None = Field(default=None, ge=1)
    hold_top_n: int | None = Field(default=None, ge=1)
    hold_days: int | None = Field(default=None, ge=1)
    target_notional: float | None = Field(default=None, ge=0)
    initial_capital: float | None = Field(default=None, ge=0)
    max_open_positions: int | None = Field(default=None, ge=1)
    max_position_notional: float | None = Field(default=None, ge=0)
    max_position_pct: float | None = Field(default=None, ge=0, le=100)
    fee_bps: float | None = Field(default=None, ge=0)
    slippage_bps: float | None = Field(default=None, ge=0)
    gap_up_limit_pct: float | None = None
    gap_down_limit_pct: float | None = None
    allow_reentry: bool | None = None
    details: dict[str, Any] | None = None
    config: dict[str, Any] | None = None


class StrategyUpdate(BaseModel):
    universe_id: int | None = Field(default=None, ge=1)
    key: str | None = Field(default=None, min_length=1, max_length=160)
    name: str | None = Field(default=None, min_length=1, max_length=200)
    strategy_type: str | None = Field(default=None, min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=1000)
    entry_rule: str | None = None
    exit_rule: str | None = None
    signal_timing: str | None = Field(default=None, max_length=120)
    execution_timing: str | None = Field(default=None, max_length=120)
    trade_price_basis: str | None = Field(default=None, max_length=120)
    buy_top_n: int | None = Field(default=None, ge=1)
    hold_top_n: int | None = Field(default=None, ge=1)
    hold_days: int | None = Field(default=None, ge=1)
    target_notional: float | None = Field(default=None, ge=0)
    initial_capital: float | None = Field(default=None, ge=0)
    max_open_positions: int | None = Field(default=None, ge=1)
    max_position_notional: float | None = Field(default=None, ge=0)
    max_position_pct: float | None = Field(default=None, ge=0, le=100)
    fee_bps: float | None = Field(default=None, ge=0)
    slippage_bps: float | None = Field(default=None, ge=0)
    gap_up_limit_pct: float | None = None
    gap_down_limit_pct: float | None = None
    allow_reentry: bool | None = None
    details: dict[str, Any] | None = None
    config: dict[str, Any] | None = None
