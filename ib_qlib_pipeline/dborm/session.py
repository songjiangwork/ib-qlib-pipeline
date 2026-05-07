from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import sessionmaker


def database_url_from_path(db_path: Path) -> str:
    return f"sqlite:///{db_path.resolve()}"


def database_url_from_env() -> str:
    explicit_url = os.getenv("RANKING_API_DB_URL")
    if explicit_url:
        return explicit_url

    project_root = Path(__file__).resolve().parents[2]
    db_path = Path(os.getenv("RANKING_API_DB_PATH", project_root / "data" / "app" / "ranking_service.db")).resolve()
    return database_url_from_path(db_path)


def create_engine_for_url(url: str, echo: bool = False) -> Engine:
    return create_engine(url, echo=echo, future=True)


def create_engine_for_path(db_path: Path, echo: bool = False) -> Engine:
    return create_engine_for_url(database_url_from_path(db_path), echo=echo)


def create_engine_from_env(echo: bool = False) -> Engine:
    return create_engine_for_url(database_url_from_env(), echo=echo)


def create_session_factory_for_url(url: str, echo: bool = False) -> sessionmaker:
    engine = create_engine_for_url(url, echo=echo)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def create_session_factory_for_path(db_path: Path, echo: bool = False) -> sessionmaker:
    engine = create_engine_for_path(db_path, echo=echo)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def create_session_factory(echo: bool = False) -> sessionmaker:
    return create_session_factory_for_url(database_url_from_env(), echo=echo)
