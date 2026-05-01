from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class ScheduleCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    hour: int = Field(ge=0, le=23)
    minute: int = Field(ge=0, le=59)
    day_of_week: str = Field(default="mon-fri")
    timezone: str = Field(default="America/Edmonton")
    client_id: int = Field(default=151, ge=0)
    lookback_days: int = Field(default=7, ge=1, le=90)
    workflow_base: str = Field(default="examples/workflow_us_lgb_2020_port.yaml")
    enabled: bool = True


class ScheduleUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    hour: int | None = Field(default=None, ge=0, le=23)
    minute: int | None = Field(default=None, ge=0, le=59)
    day_of_week: str | None = None
    timezone: str | None = None
    client_id: int | None = Field(default=None, ge=0)
    lookback_days: int | None = Field(default=None, ge=1, le=90)
    workflow_base: str | None = None
    enabled: bool | None = None


class ManualRunRequest(BaseModel):
    client_id: int = Field(default=151, ge=0)
    lookback_days: int = Field(default=7, ge=1, le=90)
    workflow_base: str = Field(default="examples/workflow_us_lgb_2020_port.yaml")


class RunQuery(BaseModel):
    limit: int = Field(default=50, ge=1, le=500)
    status: Literal["queued", "running", "succeeded", "failed"] | None = None
    signal_date: str | None = None
