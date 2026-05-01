from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query

from .price_store import summarize_performance
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

    return app
