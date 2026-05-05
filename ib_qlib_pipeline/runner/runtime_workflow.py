from __future__ import annotations

import datetime as dt
from pathlib import Path

import yaml

from ib_qlib_pipeline.qlib_runtime import QlibRuntimeConfig, build_run_token


def load_base_workflow(project_root: Path, workflow_base: str) -> dict:
    base_path = project_root / workflow_base
    if not base_path.exists():
        raise SystemExit(f"Base workflow not found: {base_path}")
    return yaml.safe_load(base_path.read_text(encoding="utf-8"))


def write_runtime_workflow(runtime_cfg: QlibRuntimeConfig, filename: str, workflow: dict) -> Path:
    runtime_cfg.workspace_dir.mkdir(parents=True, exist_ok=True)
    runtime_wf = runtime_cfg.workspace_dir / filename
    runtime_wf.write_text(yaml.safe_dump(workflow, sort_keys=False, allow_unicode=False), encoding="utf-8")
    return runtime_wf


def build_daily_runtime_workflow(
    *,
    runtime_cfg: QlibRuntimeConfig,
    workflow_base: str,
    latest_trade_date: dt.date,
    backtest_end: dt.date,
    today: dt.date,
    base_workflow: dict | None = None,
) -> tuple[Path, str]:
    wf = yaml.safe_load(yaml.safe_dump(base_workflow, sort_keys=False, allow_unicode=False)) if base_workflow else load_base_workflow(runtime_cfg.project_root, workflow_base)
    workflow_stem = Path(workflow_base).stem.replace("workflow_", "")
    experiment_name = build_run_token(f"daily_{workflow_stem}_{latest_trade_date.isoformat()}")
    wf["data_handler_config"]["end_time"] = latest_trade_date.isoformat()
    wf["task"]["dataset"]["kwargs"]["segments"]["test"][1] = latest_trade_date.isoformat()
    wf["port_analysis_config"]["backtest"]["end_time"] = backtest_end.isoformat()
    wf["experiment_name"] = experiment_name
    runtime_wf = write_runtime_workflow(runtime_cfg, f"workflow_runtime_{today.isoformat()}.yaml", wf)
    return runtime_wf, experiment_name


def build_backfill_runtime_workflow(
    *,
    runtime_cfg: QlibRuntimeConfig,
    workflow_base: str,
    model_key: str,
    trade_date: dt.date,
    backtest_end: dt.date,
    base_workflow: dict,
) -> tuple[Path, str]:
    wf = yaml.safe_load(yaml.safe_dump(base_workflow, sort_keys=False, allow_unicode=False))
    workflow_stem = Path(workflow_base).stem.replace("workflow_", "")
    trade_date_str = trade_date.isoformat()
    experiment_name = build_run_token(f"backfill_{model_key}_{workflow_stem}_{trade_date_str}")
    wf["data_handler_config"]["end_time"] = trade_date_str
    wf["task"]["dataset"]["kwargs"]["segments"]["test"][1] = trade_date_str
    wf["task"]["record"] = [
        record
        for record in wf["task"]["record"]
        if str(record.get("class")) != "PortAnaRecord"
    ]
    wf["port_analysis_config"]["backtest"]["end_time"] = backtest_end.isoformat()
    wf["experiment_name"] = experiment_name
    runtime_wf = write_runtime_workflow(runtime_cfg, f"workflow_backfill_{trade_date_str}.yaml", wf)
    return runtime_wf, experiment_name
