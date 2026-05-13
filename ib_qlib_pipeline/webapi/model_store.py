from __future__ import annotations

import datetime as dt
import json
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, joinedload, sessionmaker

from ..dborm.models import ModelRef
from .universe_store import ensure_default_universes, get_universe_by_key


DEFAULT_MODELS: list[dict[str, Any]] = [
    {
        "universe_key": "sp500",
        "key": "lgb",
        "name": "LightGBM_Default",
        "model_class": "LGBModel",
        "module_path": "qlib.contrib.model.gbdt",
        "workflow_base": "examples/workflow_us_lgb_2020_port.yaml",
        "details": {
            "family": "gbdt",
            "variant": "default",
            "notes": "Current default model for existing backfill runs",
        },
    },
    {
        "universe_key": "sp500",
        "key": "xgb",
        "name": "XGBoost_Default",
        "model_class": "XGBModel",
        "module_path": "qlib.contrib.model.xgboost",
        "workflow_base": "examples/workflow_us_xgb_2020_port.yaml",
        "details": {
            "family": "gbdt",
            "variant": "default",
            "notes": "Alternative tree ensemble model",
        },
    },
    {
        "universe_key": "sp500",
        "key": "catboost",
        "name": "CatBoost_Default",
        "model_class": "CatBoostModel",
        "module_path": "qlib.contrib.model.catboost_model",
        "workflow_base": "examples/workflow_us_catboost_2020_port.yaml",
        "details": {
            "family": "gbdt",
            "variant": "default",
            "notes": "Alternative boosting model with CatBoost backend",
        },
    },
    {
        "universe_key": "sp500",
        "key": "lgb_5d",
        "name": "LightGBM_5D",
        "model_class": "LGBModel",
        "module_path": "qlib.contrib.model.gbdt",
        "workflow_base": "examples/workflow_us_lgb_2020_port_next_open_5d.yaml",
        "details": {
            "family": "gbdt",
            "variant": "next_open_5d",
            "notes": "Next-open to 5-day-close target for LightGBM",
        },
    },
    {
        "universe_key": "sp500",
        "key": "xgb_5d",
        "name": "XGBoost_5D",
        "model_class": "XGBModel",
        "module_path": "qlib.contrib.model.xgboost",
        "workflow_base": "examples/workflow_us_xgb_2020_port_next_open_5d.yaml",
        "details": {
            "family": "gbdt",
            "variant": "next_open_5d",
            "notes": "Next-open to 5-day-close target for XGBoost",
        },
    },
    {
        "universe_key": "sp500",
        "key": "catboost_5d",
        "name": "CatBoost_5D",
        "model_class": "CatBoostModel",
        "module_path": "qlib.contrib.model.catboost_model",
        "workflow_base": "examples/workflow_us_catboost_2020_port_next_open_5d.yaml",
        "details": {
            "family": "gbdt",
            "variant": "next_open_5d",
            "notes": "Next-open to 5-day-close target for CatBoost",
        },
    },
    {
        "universe_key": "us_union_sp500_ndx_djia_sox",
        "key": "lgb_union",
        "name": "LightGBM_Default_Union",
        "model_class": "LGBModel",
        "module_path": "qlib.contrib.model.gbdt",
        "workflow_base": "examples/workflow_us_lgb_2020_port.yaml",
        "details": {
            "family": "gbdt",
            "variant": "default",
            "scope": "union",
            "notes": "Default LightGBM model family for the union universe",
        },
    },
    {
        "universe_key": "us_union_sp500_ndx_djia_sox",
        "key": "xgb_union",
        "name": "XGBoost_Default_Union",
        "model_class": "XGBModel",
        "module_path": "qlib.contrib.model.xgboost",
        "workflow_base": "examples/workflow_us_xgb_2020_port.yaml",
        "details": {
            "family": "gbdt",
            "variant": "default",
            "scope": "union",
            "notes": "Default XGBoost model family for the union universe",
        },
    },
    {
        "universe_key": "us_union_sp500_ndx_djia_sox",
        "key": "catboost_union",
        "name": "CatBoost_Default_Union",
        "model_class": "CatBoostModel",
        "module_path": "qlib.contrib.model.catboost_model",
        "workflow_base": "examples/workflow_us_catboost_2020_port.yaml",
        "details": {
            "family": "gbdt",
            "variant": "default",
            "scope": "union",
            "notes": "Default CatBoost model family for the union universe",
        },
    },
    {
        "universe_key": "us_union_sp500_ndx_djia_sox",
        "key": "lgb_5d_union",
        "name": "LightGBM_5D_Union",
        "model_class": "LGBModel",
        "module_path": "qlib.contrib.model.gbdt",
        "workflow_base": "examples/workflow_us_lgb_2020_port_next_open_5d.yaml",
        "details": {
            "family": "gbdt",
            "variant": "next_open_5d",
            "scope": "union",
            "notes": "Next-open to 5-day-close LightGBM model family for the union universe",
        },
    },
    {
        "universe_key": "us_union_sp500_ndx_djia_sox",
        "key": "xgb_5d_union",
        "name": "XGBoost_5D_Union",
        "model_class": "XGBModel",
        "module_path": "qlib.contrib.model.xgboost",
        "workflow_base": "examples/workflow_us_xgb_2020_port_next_open_5d.yaml",
        "details": {
            "family": "gbdt",
            "variant": "next_open_5d",
            "scope": "union",
            "notes": "Next-open to 5-day-close XGBoost model family for the union universe",
        },
    },
    {
        "universe_key": "us_union_sp500_ndx_djia_sox",
        "key": "catboost_5d_union",
        "name": "CatBoost_5D_Union",
        "model_class": "CatBoostModel",
        "module_path": "qlib.contrib.model.catboost_model",
        "workflow_base": "examples/workflow_us_catboost_2020_port_next_open_5d.yaml",
        "details": {
            "family": "gbdt",
            "variant": "next_open_5d",
            "scope": "union",
            "notes": "Next-open to 5-day-close CatBoost model family for the union universe",
        },
    },
    {
        "universe_key": "us_union_sp500_ndx_djia_sox",
        "key": "lgb5_u16",
        "name": "LGB5_U16",
        "model_class": "LGBModel",
        "module_path": "qlib.contrib.model.gbdt",
        "workflow_base": "examples/wf_lgb5_u16.yaml",
        "details": {
            "family": "gbdt",
            "variant": "u16_5d",
            "scope": "union",
            "notes": "Union 5D experiment with 2016 training start and 8 threads",
        },
    },
    {
        "universe_key": "us_union_sp500_ndx_djia_sox",
        "key": "xgb5_u16",
        "name": "XGB5_U16",
        "model_class": "XGBModel",
        "module_path": "qlib.contrib.model.xgboost",
        "workflow_base": "examples/wf_xgb5_u16.yaml",
        "details": {
            "family": "gbdt",
            "variant": "u16_5d",
            "scope": "union",
            "notes": "Union 5D experiment with 2016 training start and 8 threads",
        },
    },
    {
        "universe_key": "us_union_sp500_ndx_djia_sox",
        "key": "cat5_u16",
        "name": "CAT5_U16",
        "model_class": "CatBoostModel",
        "module_path": "qlib.contrib.model.catboost_model",
        "workflow_base": "examples/wf_cat5_u16.yaml",
        "details": {
            "family": "gbdt",
            "variant": "u16_5d",
            "scope": "union",
            "notes": "Union 5D experiment with 2016 training start and 8 threads",
        },
    },
    {
        "universe_key": "us_union_sp500_ndx_djia_sox",
        "key": "cat1_u16",
        "name": "CAT1_U16",
        "model_class": "CatBoostModel",
        "module_path": "qlib.contrib.model.catboost_model",
        "workflow_base": "examples/u16_catboost_1d.yaml",
        "details": {
            "family": "gbdt",
            "variant": "u16_1d",
            "scope": "union",
            "config_path": "config_u16_catboost_1d.yaml",
            "notes": "Union 1D CatBoost experiment with 2016 training start and 8 threads",
        },
    },
]


@lru_cache(maxsize=8)
def _session_factory_for_path(db_path_str: str) -> sessionmaker:
    engine = create_engine(f"sqlite:///{Path(db_path_str).resolve()}", future=True)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def _session_for_db(db_path: Path) -> Session:
    return _session_factory_for_path(str(db_path))()


def _model_to_dict(model: ModelRef) -> dict[str, Any]:
    item = {
        "id": model.id,
        "universe_id": model.universe_id,
        "key": model.key,
        "name": model.name,
        "model_class": model.model_class,
        "module_path": model.module_path,
        "workflow_base": model.workflow_base,
        "details_json": model.details_json,
        "created_at": model.created_at,
        "updated_at": model.updated_at,
    }
    item["universe_key"] = model.universe.key if model.universe is not None else None
    item["universe_name"] = model.universe.name if model.universe is not None else None
    item["details"] = json.loads(model.details_json) if model.details_json else None
    return item


def ensure_default_models(db_path: Path, project_root: Path | None = None) -> None:
    if project_root is None:
        project_root = Path(__file__).resolve().parents[2]
    ensure_default_universes(db_path, project_root)
    sp500 = get_universe_by_key(db_path, "sp500")
    if sp500 is None:
        raise RuntimeError("Default universe 'sp500' is missing")
    universe_id_by_key: dict[str, int] = {"sp500": int(sp500["id"])}
    union = get_universe_by_key(db_path, "us_union_sp500_ndx_djia_sox")
    if union is not None:
        universe_id_by_key["us_union_sp500_ndx_djia_sox"] = int(union["id"])
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    with _session_for_db(db_path) as session:
        for model in DEFAULT_MODELS:
            universe_key = str(model.get("universe_key") or "sp500")
            universe_id = universe_id_by_key.get(universe_key)
            existing = session.execute(
                select(ModelRef).where(ModelRef.key == model["key"])
            ).scalar_one_or_none()
            if existing is None:
                session.add(
                    ModelRef(
                        universe_id=universe_id,
                        key=model["key"],
                        name=model["name"],
                        model_class=model["model_class"],
                        module_path=model["module_path"],
                        workflow_base=model["workflow_base"],
                        details_json=json.dumps(model["details"], ensure_ascii=True, sort_keys=True),
                        created_at=now,
                        updated_at=now,
                    )
                )
                continue
            existing.name = model["name"]
            existing.universe_id = universe_id
            existing.model_class = model["model_class"]
            existing.module_path = model["module_path"]
            existing.workflow_base = model["workflow_base"]
            existing.details_json = json.dumps(model["details"], ensure_ascii=True, sort_keys=True)
            existing.updated_at = now
        session.commit()


def list_models(db_path: Path) -> list[dict[str, Any]]:
    with _session_for_db(db_path) as session:
        rows = session.execute(
            select(ModelRef).options(joinedload(ModelRef.universe)).order_by(ModelRef.id)
        ).scalars().all()
    return [_model_to_dict(row) for row in rows]


def get_model(db_path: Path, model_id: int) -> dict[str, Any] | None:
    with _session_for_db(db_path) as session:
        row = session.execute(
            select(ModelRef).options(joinedload(ModelRef.universe)).where(ModelRef.id == model_id)
        ).scalar_one_or_none()
    if row is None:
        return None
    return _model_to_dict(row)


def get_model_by_key(db_path: Path, key: str) -> dict[str, Any] | None:
    with _session_for_db(db_path) as session:
        row = session.execute(
            select(ModelRef).options(joinedload(ModelRef.universe)).where(ModelRef.key == key)
        ).scalar_one_or_none()
    if row is None:
        return None
    return _model_to_dict(row)


def infer_model_from_workflow(project_root: Path, workflow_base: str) -> dict[str, Any]:
    workflow_path = project_root / workflow_base
    if not workflow_path.exists():
        raise FileNotFoundError(f"Workflow not found: {workflow_path}")
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    model_cfg = workflow.get("task", {}).get("model", {})
    model_class = str(model_cfg.get("class", "")).strip()
    module_path = str(model_cfg.get("module_path", "")).strip()
    if not model_class or not module_path:
        raise RuntimeError(f"Workflow missing task.model class/module_path: {workflow_path}")
    return {
        "model_class": model_class,
        "module_path": module_path,
    }


def resolve_or_create_model_for_workflow(
    db_path: Path,
    project_root: Path,
    workflow_base: str,
    universe_id: int | None = None,
) -> int:
    inferred = infer_model_from_workflow(project_root, workflow_base)
    sp500 = get_universe_by_key(db_path, "sp500")
    default_universe_id = universe_id if universe_id is not None else (int(sp500["id"]) if sp500 is not None else None)
    with _session_for_db(db_path) as session:
        row = session.execute(
            select(ModelRef).where(ModelRef.workflow_base == workflow_base).limit(1)
        ).scalar_one_or_none()
        if row is not None:
            return int(row.id)

        now = dt.datetime.now(dt.timezone.utc).isoformat()
        workflow_stem = Path(workflow_base).stem.lower()
        item = ModelRef(
            universe_id=default_universe_id,
            key=f"{workflow_stem}-{abs(hash((workflow_base, inferred['model_class'], inferred['module_path']))) % 100000}",
            name=workflow_stem,
            model_class=inferred["model_class"],
            module_path=inferred["module_path"],
            workflow_base=workflow_base,
            details_json=json.dumps({"auto_created": True, "workflow_stem": workflow_stem}, ensure_ascii=True, sort_keys=True),
            created_at=now,
            updated_at=now,
        )
        session.add(item)
        session.commit()
        session.refresh(item)
        return int(item.id)
