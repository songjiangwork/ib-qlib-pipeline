from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import sessionmaker


def database_url_from_env() -> str:
    explicit_url = os.getenv("RANKING_API_DB_URL")
    if explicit_url:
        return explicit_url

    project_root = Path(__file__).resolve().parents[2]
    db_path = Path(os.getenv("RANKING_API_DB_PATH", project_root / "data" / "app" / "ranking_service.db")).resolve()
    return f"sqlite:///{db_path}"


def create_engine_from_env(echo: bool = False) -> Engine:
    return create_engine(database_url_from_env(), echo=echo, future=True)


def create_session_factory(echo: bool = False) -> sessionmaker:
    engine = create_engine_from_env(echo=echo)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
