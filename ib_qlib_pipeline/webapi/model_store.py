from __future__ import annotations

import datetime as dt
import json
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from ..dborm.models import ModelRef


DEFAULT_MODELS: list[dict[str, Any]] = [
    {
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
        "key": model.key,
        "name": model.name,
        "model_class": model.model_class,
        "module_path": model.module_path,
        "workflow_base": model.workflow_base,
        "details_json": model.details_json,
        "created_at": model.created_at,
        "updated_at": model.updated_at,
    }
    item["details"] = json.loads(model.details_json) if model.details_json else None
    return item


def ensure_default_models(db_path: Path) -> None:
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    with _session_for_db(db_path) as session:
        for model in DEFAULT_MODELS:
            existing = session.execute(
                select(ModelRef).where(ModelRef.key == model["key"])
            ).scalar_one_or_none()
            if existing is None:
                session.add(
                    ModelRef(
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
            existing.model_class = model["model_class"]
            existing.module_path = model["module_path"]
            existing.workflow_base = model["workflow_base"]
            existing.details_json = json.dumps(model["details"], ensure_ascii=True, sort_keys=True)
            existing.updated_at = now
        session.commit()


def list_models(db_path: Path) -> list[dict[str, Any]]:
    with _session_for_db(db_path) as session:
        rows = session.execute(select(ModelRef).order_by(ModelRef.id)).scalars().all()
    return [_model_to_dict(row) for row in rows]


def get_model(db_path: Path, model_id: int) -> dict[str, Any] | None:
    with _session_for_db(db_path) as session:
        row = session.get(ModelRef, model_id)
    if row is None:
        return None
    return _model_to_dict(row)


def get_model_by_key(db_path: Path, key: str) -> dict[str, Any] | None:
    with _session_for_db(db_path) as session:
        row = session.execute(select(ModelRef).where(ModelRef.key == key)).scalar_one_or_none()
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


def resolve_or_create_model_for_workflow(db_path: Path, project_root: Path, workflow_base: str) -> int:
    inferred = infer_model_from_workflow(project_root, workflow_base)
    with _session_for_db(db_path) as session:
        row = session.execute(
            select(ModelRef).where(ModelRef.workflow_base == workflow_base).limit(1)
        ).scalar_one_or_none()
        if row is not None:
            return int(row.id)

        now = dt.datetime.now(dt.timezone.utc).isoformat()
        workflow_stem = Path(workflow_base).stem.lower()
        item = ModelRef(
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
