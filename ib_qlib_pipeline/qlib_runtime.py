from __future__ import annotations

import datetime as dt
import os
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class QlibRuntimeConfig:
    project_root: Path
    data_dir: Path
    qlib_csv_dir: Path
    qlib_bin_dir: Path
    qlib_repo_path: Path
    qlib_python_bin: Path
    qlib_qrun_bin: Path
    workspace_dir: Path
    mlruns_dir: Path


def _resolve_path(project_root: Path, value: str | Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (project_root / path).resolve()


def load_qlib_runtime_config(project_root: Path, config_path: Path | None = None) -> QlibRuntimeConfig:
    cfg_path = config_path or (project_root / "config.yaml")
    raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    output_cfg = raw.get("output", {})
    qlib_cfg = raw.get("qlib", {})

    data_dir = _resolve_path(
        project_root,
        os.getenv("PROJECT_DATA_DIR", output_cfg.get("root_dir", "data")),
    )
    qlib_csv_dir = _resolve_path(
        project_root,
        output_cfg.get("qlib_csv_dir", "data/processed/qlib_csv"),
    )
    qlib_bin_dir = _resolve_path(
        project_root,
        output_cfg.get("qlib_bin_dir", "data/qlib/us_data_custom"),
    )
    qlib_repo_path = _resolve_path(
        project_root,
        os.getenv("QLIB_REPO_PATH", qlib_cfg.get("qlib_repo_path", "")),
    )
    qlib_python_bin = _resolve_path(
        project_root,
        os.getenv("QLIB_PYTHON_BIN", qlib_cfg.get("python_bin", "")),
    )
    qrun_default = qlib_cfg.get("qrun_bin") or str(qlib_python_bin.with_name("qrun"))
    qlib_qrun_bin = _resolve_path(
        project_root,
        os.getenv("QLIB_QRUN_BIN", qrun_default),
    )
    workspace_dir = _resolve_path(
        project_root,
        os.getenv("RUN_WORKSPACE_DIR", "reports/tmp"),
    )
    mlruns_dir = _resolve_path(
        project_root,
        os.getenv("MLRUNS_DIR", "mlruns"),
    )
    return QlibRuntimeConfig(
        project_root=project_root,
        data_dir=data_dir,
        qlib_csv_dir=qlib_csv_dir,
        qlib_bin_dir=qlib_bin_dir,
        qlib_repo_path=qlib_repo_path,
        qlib_python_bin=qlib_python_bin,
        qlib_qrun_bin=qlib_qrun_bin,
        workspace_dir=workspace_dir,
        mlruns_dir=mlruns_dir,
    )


def build_run_token(prefix: str) -> str:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}_{stamp}"
