from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query

import datetime as dt

from .price_store import list_price_bars, list_price_history, summarize_performance
from .portfolio_store import (
    get_portfolio_run,
    list_portfolio_lots,
    list_portfolio_marks,
    list_portfolio_runs,
)
from .schemas import ManualRunRequest, ScheduleCreate, ScheduleUpdate
from .service import BusyError, NotFoundError, RankingBackendService
from .settings import Settings


def create_app() -> FastAPI:
    settings = Settings.load()
    service = RankingBackendService(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[override]
        app.state.service = service
        app.state.settings = settings
        service.start()
        try:
            yield
        finally:
            service.stop()

    app = FastAPI(
        title="IB Qlib Ranking Backend",
        version="0.1.0",
        lifespan=lifespan,
    )

    def get_service() -> RankingBackendService:
        return app.state.service

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/config")
    def get_config() -> dict[str, Any]:
        return {
            "project_root": str(settings.project_root),
            "db_path": str(settings.db_path),
            "timezone": settings.timezone,
            "default_workflow_base": settings.default_workflow_base,
            "run_script_path": str(settings.run_script_path),
        }

    @app.get("/api/schedules")
    def list_schedules() -> list[dict[str, Any]]:
        return get_service().list_schedules()

    @app.post("/api/schedules")
    def create_schedule(payload: ScheduleCreate) -> dict[str, Any]:
        return get_service().create_schedule(payload.model_dump())

    @app.patch("/api/schedules/{schedule_id}")
    def update_schedule(schedule_id: int, payload: ScheduleUpdate) -> dict[str, Any]:
        try:
            return get_service().update_schedule(schedule_id, payload.model_dump(exclude_none=True))
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.delete("/api/schedules/{schedule_id}")
    def delete_schedule(schedule_id: int) -> dict[str, bool]:
        try:
            get_service().delete_schedule(schedule_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"deleted": True}

    @app.get("/api/runs")
    def list_runs(
        limit: int = Query(default=50, ge=1, le=500),
        status: str | None = Query(default=None),
        signal_date: str | None = Query(default=None),
    ) -> list[dict[str, Any]]:
        return get_service().list_runs(limit=limit, status=status, signal_date=signal_date)

    @app.post("/api/runs")
    def trigger_run(payload: ManualRunRequest) -> dict[str, Any]:
        try:
            return get_service().trigger_manual_run(payload.model_dump())
        except BusyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/api/ranking-dates")
    def list_ranking_dates(
        limit: int = Query(default=20, ge=1, le=100),
        offset: int = Query(default=0, ge=0),
        query: str | None = Query(default=None),
    ) -> dict[str, Any]:
        return get_service().list_ranking_dates(limit=limit, offset=offset, query=query)

    @app.get("/api/runs/{run_id}")
    def get_run(run_id: int) -> dict[str, Any]:
        try:
            return get_service().get_run(run_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/runs/{run_id}/recommendations")
    def get_run_recommendations(
        run_id: int,
        horizons: str = Query(default="1,5,10,21"),
    ) -> dict[str, Any]:
        try:
            run = get_service().get_run(run_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        recommendations = get_service().get_run_recommendations(run_id)
        parsed_horizons = [int(part) for part in horizons.split(",") if part.strip()]
        summary = summarize_performance(settings.project_root, recommendations, parsed_horizons)
        return {
            "run": run,
            "horizons": parsed_horizons,
            "summary": summary,
            "recommendations": recommendations,
        }

    @app.get("/api/prices/{symbol}")
    def get_price_history(
        symbol: str,
        start_date: str | None = Query(default=None),
        end_date: str | None = Query(default=None),
    ) -> list[dict[str, Any]]:
        parsed_start = dt.date.fromisoformat(start_date) if start_date else None
        parsed_end = dt.date.fromisoformat(end_date) if end_date else None
        try:
            return list_price_history(
                settings.project_root,
                symbol=symbol,
                start_date=parsed_start,
                end_date=parsed_end,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/prices/{symbol}/bars")
    def get_price_bars(
        symbol: str,
        interval: str = Query(default="1d"),
        start_date: str | None = Query(default=None),
        end_date: str | None = Query(default=None),
    ) -> list[dict[str, Any]]:
        parsed_start = dt.date.fromisoformat(start_date) if start_date else None
        parsed_end = dt.date.fromisoformat(end_date) if end_date else None
        try:
            return list_price_bars(
                settings.project_root,
                symbol=symbol,
                interval=interval,
                start_date=parsed_start,
                end_date=parsed_end,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/portfolio-runs")
    def get_portfolio_runs() -> list[dict[str, Any]]:
        return list_portfolio_runs(settings.db_path)

    @app.get("/api/portfolio-runs/{portfolio_run_id}")
    def get_portfolio_run_detail(portfolio_run_id: int) -> dict[str, Any]:
        row = get_portfolio_run(settings.db_path, portfolio_run_id)
        if row is None:
            raise HTTPException(status_code=404, detail=f"Portfolio run {portfolio_run_id} not found")
        return row

    @app.get("/api/portfolio-runs/{portfolio_run_id}/lots")
    def get_portfolio_run_lots(
        portfolio_run_id: int,
        symbol: str | None = Query(default=None),
        status: str | None = Query(default=None),
    ) -> list[dict[str, Any]]:
        row = get_portfolio_run(settings.db_path, portfolio_run_id)
        if row is None:
            raise HTTPException(status_code=404, detail=f"Portfolio run {portfolio_run_id} not found")
        return list_portfolio_lots(settings.db_path, portfolio_run_id, symbol=symbol, status=status)

    @app.get("/api/portfolio-lots/{lot_id}/marks")
    def get_portfolio_lot_marks(lot_id: int) -> list[dict[str, Any]]:
        return list_portfolio_marks(settings.db_path, lot_id)

    @app.get("/api/portfolio-runs/{portfolio_run_id}/symbols/{symbol}")
    def get_portfolio_symbol_lifecycle(portfolio_run_id: int, symbol: str) -> dict[str, Any]:
        row = get_portfolio_run(settings.db_path, portfolio_run_id)
        if row is None:
            raise HTTPException(status_code=404, detail=f"Portfolio run {portfolio_run_id} not found")
        lots = list_portfolio_lots(settings.db_path, portfolio_run_id, symbol=symbol)
        return {
            "portfolio_run": row,
            "symbol": symbol,
            "lots": [
                {
                    **lot,
                    "marks": list_portfolio_marks(settings.db_path, int(lot["id"])),
                }
                for lot in lots
            ],
        }

    return app
