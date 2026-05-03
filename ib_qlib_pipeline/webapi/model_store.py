from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import yaml

from .db import connect, rows_to_dicts


DEFAULT_MODELS: list[dict[str, Any]] = [
    {
        "key": "lgb",
        "name": "LightGBM",
        "model_class": "LGBModel",
        "module_path": "qlib.contrib.model.gbdt",
        "workflow_base": "examples/workflow_us_lgb_2020_port.yaml",
        "details": {
            "family": "gbdt",
            "notes": "Current default model for existing backfill runs",
        },
    },
    {
        "key": "xgb",
        "name": "XGBoost",
        "model_class": "XGBModel",
        "module_path": "qlib.contrib.model.xgboost",
        "workflow_base": "examples/workflow_us_xgb_2020_port.yaml",
        "details": {
            "family": "gbdt",
            "notes": "Alternative tree ensemble model",
        },
    },
    {
        "key": "catboost",
        "name": "CatBoost",
        "model_class": "CatBoostModel",
        "module_path": "qlib.contrib.model.catboost_model",
        "workflow_base": "examples/workflow_us_catboost_2020_port.yaml",
        "details": {
            "family": "gbdt",
            "notes": "Alternative boosting model with CatBoost backend",
        },
    },
]


def ensure_default_models(db_path: Path) -> None:
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    with connect(db_path) as conn:
        for model in DEFAULT_MODELS:
            conn.execute(
                """
                INSERT INTO models (
                    key, name, model_class, module_path, workflow_base,
                    details_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    name = excluded.name,
                    model_class = excluded.model_class,
                    module_path = excluded.module_path,
                    workflow_base = excluded.workflow_base,
                    details_json = excluded.details_json,
                    updated_at = excluded.updated_at
                """,
                (
                    model["key"],
                    model["name"],
                    model["model_class"],
                    model["module_path"],
                    model["workflow_base"],
                    json.dumps(model["details"], ensure_ascii=True, sort_keys=True),
                    now,
                    now,
                ),
            )


def list_models(db_path: Path) -> list[dict[str, Any]]:
    with connect(db_path) as conn:
        rows = conn.execute("SELECT * FROM models ORDER BY id").fetchall()
    items = rows_to_dicts(rows)
    for item in items:
        item["details"] = json.loads(item["details_json"]) if item.get("details_json") else None
    return items


def get_model(db_path: Path, model_id: int) -> dict[str, Any] | None:
    with connect(db_path) as conn:
        row = conn.execute("SELECT * FROM models WHERE id = ?", (model_id,)).fetchone()
    if row is None:
        return None
    item = dict(row)
    item["details"] = json.loads(item["details_json"]) if item.get("details_json") else None
    return item


def get_model_by_key(db_path: Path, key: str) -> dict[str, Any] | None:
    with connect(db_path) as conn:
        row = conn.execute("SELECT * FROM models WHERE key = ?", (key,)).fetchone()
    if row is None:
        return None
    item = dict(row)
    item["details"] = json.loads(item["details_json"]) if item.get("details_json") else None
    return item


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
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT id
            FROM models
            WHERE workflow_base = ?
               OR (model_class = ? AND module_path = ?)
            ORDER BY CASE WHEN workflow_base = ? THEN 0 ELSE 1 END, id
            LIMIT 1
            """,
            (
                workflow_base,
                inferred["model_class"],
                inferred["module_path"],
                workflow_base,
            ),
        ).fetchone()
        if row is not None:
            model_id = int(row["id"])
            conn.execute(
                "UPDATE models SET workflow_base = COALESCE(workflow_base, ?), updated_at = ? WHERE id = ?",
                (workflow_base, dt.datetime.now(dt.timezone.utc).isoformat(), model_id),
            )
            return model_id

        now = dt.datetime.now(dt.timezone.utc).isoformat()
        key = inferred["model_class"].replace("Model", "").replace("Pytorch", "").lower()
        cursor = conn.execute(
            """
            INSERT INTO models (
                key, name, model_class, module_path, workflow_base,
                details_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"{key}-{abs(hash((workflow_base, inferred['model_class'], inferred['module_path']))) % 100000}",
                inferred["model_class"],
                inferred["model_class"],
                inferred["module_path"],
                workflow_base,
                json.dumps({"auto_created": True}, ensure_ascii=True, sort_keys=True),
                now,
                now,
            ),
        )
        return int(cursor.lastrowid)
