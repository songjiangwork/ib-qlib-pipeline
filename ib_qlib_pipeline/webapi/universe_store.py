from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

from sqlalchemy import delete, select

from ..dborm.models import ModelRef, PortfolioLot, PortfolioRun, Run, Universe, UniverseSymbol
from ..dborm.session import create_session_factory_for_path


DEFAULT_UNIVERSES: list[dict[str, Any]] = [
    {
        "key": "sp500",
        "name": "S&P 500",
        "symbols_file": "symbols/sp500_full_ib_map.txt",
        "description": "Primary production universe based on the current S&P 500 symbol map",
        "details": {
            "family": "equity",
            "scope": "sp500",
            "status": "active",
        },
    },
    {
        "key": "us_union_sp500_ndx_djia_sox",
        "name": "US Union 524",
        "symbols_file": "symbols/us_union_sp500_ndx_djia_sox.txt",
        "description": "Saved expansion universe combining S&P 500 with extra NDX and SOX names",
        "details": {
            "family": "equity",
            "scope": "union",
            "status": "experimental",
        },
    },
]


def _session_for_db(db_path: Path):
    return create_session_factory_for_path(db_path)()


def _count_symbols(symbols_path: Path) -> int:
    return len(_read_symbol_rows(symbols_path))


def _read_symbol_rows(symbols_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    sort_order = 0
    for raw in symbols_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        sort_order += 1
        parts = [part.strip() for part in line.split(",")]
        symbol = parts[0]
        ib_symbol = parts[1] if len(parts) > 1 and parts[1] else symbol
        rows.append(
            {
                "symbol": symbol,
                "ib_symbol": ib_symbol,
                "sort_order": sort_order,
                "source_line": line,
            }
        )
    return rows


def _resolve_symbols_path(project_root: Path, symbols_file: str) -> Path:
    path = Path(symbols_file)
    if not path.is_absolute():
        path = (project_root / path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Symbols file not found: {path}")
    return path


def _universe_to_dict(item: Universe) -> dict[str, Any]:
    return {
        "id": item.id,
        "key": item.key,
        "name": item.name,
        "symbols_file": item.symbols_file,
        "symbol_count": item.symbol_count,
        "description": item.description,
        "details_json": item.details_json,
        "details": json.loads(item.details_json) if item.details_json else None,
        "created_at": item.created_at,
        "updated_at": item.updated_at,
    }


def _universe_symbol_to_dict(item: UniverseSymbol) -> dict[str, Any]:
    return {
        "id": item.id,
        "universe_id": item.universe_id,
        "symbol": item.symbol,
        "ib_symbol": item.ib_symbol,
        "sort_order": item.sort_order,
        "source_line": item.source_line,
        "created_at": item.created_at,
        "updated_at": item.updated_at,
    }


def _sync_universe_symbols(session: Any, project_root: Path, universe: Universe) -> None:
    symbols_path = _resolve_symbols_path(project_root, universe.symbols_file)
    rows = _read_symbol_rows(symbols_path)
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    session.execute(delete(UniverseSymbol).where(UniverseSymbol.universe_id == universe.id))
    session.add_all(
        [
            UniverseSymbol(
                universe_id=int(universe.id),
                symbol=str(row["symbol"]),
                ib_symbol=str(row["ib_symbol"]),
                sort_order=int(row["sort_order"]),
                source_line=str(row["source_line"]),
                created_at=now,
                updated_at=now,
            )
            for row in rows
        ]
    )


def ensure_default_universes(db_path: Path, project_root: Path) -> None:
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    with _session_for_db(db_path) as session:
        for item in DEFAULT_UNIVERSES:
            symbols_path = _resolve_symbols_path(project_root, str(item["symbols_file"]))
            symbol_count = _count_symbols(symbols_path)
            existing = session.execute(select(Universe).where(Universe.key == item["key"])).scalar_one_or_none()
            if existing is None:
                created = Universe(
                    key=str(item["key"]),
                    name=str(item["name"]),
                    symbols_file=str(item["symbols_file"]),
                    symbol_count=symbol_count,
                    description=str(item["description"]),
                    details_json=json.dumps(item["details"], ensure_ascii=True, sort_keys=True),
                    created_at=now,
                    updated_at=now,
                )
                session.add(created)
                session.flush()
                _sync_universe_symbols(session, project_root, created)
                continue
            existing.name = str(item["name"])
            existing.symbols_file = str(item["symbols_file"])
            existing.symbol_count = symbol_count
            existing.description = str(item["description"])
            existing.details_json = json.dumps(item["details"], ensure_ascii=True, sort_keys=True)
            existing.updated_at = now
            _sync_universe_symbols(session, project_root, existing)
        session.commit()


def list_universes(db_path: Path) -> list[dict[str, Any]]:
    with _session_for_db(db_path) as session:
        rows = session.execute(select(Universe).order_by(Universe.id)).scalars().all()
    return [_universe_to_dict(row) for row in rows]


def get_universe(db_path: Path, universe_id: int) -> dict[str, Any] | None:
    with _session_for_db(db_path) as session:
        item = session.get(Universe, universe_id)
    if item is None:
        return None
    return _universe_to_dict(item)


def get_universe_by_key(db_path: Path, key: str) -> dict[str, Any] | None:
    with _session_for_db(db_path) as session:
        item = session.execute(select(Universe).where(Universe.key == key)).scalar_one_or_none()
    if item is None:
        return None
    return _universe_to_dict(item)


def create_universe(db_path: Path, project_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    symbols_file = str(payload["symbols_file"]).strip()
    symbols_path = _resolve_symbols_path(project_root, symbols_file)
    symbol_count = _count_symbols(symbols_path)
    with _session_for_db(db_path) as session:
        item = Universe(
            key=str(payload["key"]).strip(),
            name=str(payload["name"]).strip(),
            symbols_file=symbols_file,
            symbol_count=symbol_count,
            description=str(payload["description"]).strip() if payload.get("description") else None,
            details_json=json.dumps(payload.get("details"), ensure_ascii=True, sort_keys=True)
            if payload.get("details") is not None
            else None,
            created_at=now,
            updated_at=now,
        )
        session.add(item)
        session.flush()
        _sync_universe_symbols(session, project_root, item)
        session.commit()
        session.refresh(item)
        return _universe_to_dict(item)


def update_universe(db_path: Path, project_root: Path, universe_id: int, payload: dict[str, Any]) -> dict[str, Any] | None:
    with _session_for_db(db_path) as session:
        item = session.get(Universe, universe_id)
        if item is None:
            return None
        if "key" in payload:
            item.key = str(payload["key"]).strip()
        if "name" in payload:
            item.name = str(payload["name"]).strip()
        if "symbols_file" in payload:
            symbols_file = str(payload["symbols_file"]).strip()
            symbols_path = _resolve_symbols_path(project_root, symbols_file)
            item.symbols_file = symbols_file
            item.symbol_count = _count_symbols(symbols_path)
        if "description" in payload:
            item.description = str(payload["description"]).strip() if payload["description"] else None
        if "details" in payload:
            item.details_json = (
                json.dumps(payload["details"], ensure_ascii=True, sort_keys=True)
                if payload["details"] is not None
                else None
            )
        item.updated_at = dt.datetime.now(dt.timezone.utc).isoformat()
        if "symbols_file" in payload:
            _sync_universe_symbols(session, project_root, item)
        session.commit()
        session.refresh(item)
        return _universe_to_dict(item)


def list_universe_symbols(db_path: Path, universe_id: int) -> list[dict[str, Any]]:
    with _session_for_db(db_path) as session:
        rows = session.execute(
            select(UniverseSymbol)
            .where(UniverseSymbol.universe_id == universe_id)
            .order_by(UniverseSymbol.sort_order, UniverseSymbol.id)
        ).scalars().all()
    return [_universe_symbol_to_dict(row) for row in rows]


def backfill_model_universes(db_path: Path, default_universe_key: str = "sp500") -> None:
    with _session_for_db(db_path) as session:
        universe = session.execute(select(Universe).where(Universe.key == default_universe_key)).scalar_one_or_none()
        if universe is None:
            return
        session.query(ModelRef).where(ModelRef.universe_id.is_(None)).update(
            {ModelRef.universe_id: int(universe.id)},
            synchronize_session=False,
        )
        session.commit()


def backfill_run_universes(db_path: Path) -> None:
    with _session_for_db(db_path) as session:
        rows = session.execute(select(Run).where(Run.universe_id.is_(None), Run.model_id.is_not(None))).scalars().all()
        changed = False
        for row in rows:
            model = session.get(ModelRef, row.model_id)
            if model is not None and model.universe_id is not None:
                row.universe_id = int(model.universe_id)
                changed = True
        if changed:
            session.commit()


def backfill_portfolio_run_universes(db_path: Path) -> None:
    with _session_for_db(db_path) as session:
        rows = session.execute(select(PortfolioRun).where(PortfolioRun.universe_id.is_(None))).scalars().all()
        changed = False
        for row in rows:
            first_lot = session.execute(
                select(PortfolioLot).where(PortfolioLot.portfolio_run_id == row.id).order_by(PortfolioLot.id).limit(1)
            ).scalar_one_or_none()
            if first_lot is None:
                continue
            entry_run = session.get(Run, first_lot.entry_run_id)
            if entry_run is not None and entry_run.universe_id is not None:
                row.universe_id = int(entry_run.universe_id)
                changed = True
        if changed:
            session.commit()
