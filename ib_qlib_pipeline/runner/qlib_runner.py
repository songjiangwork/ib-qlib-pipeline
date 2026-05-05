from __future__ import annotations

import os
from pathlib import Path

from ib_qlib_pipeline.qlib_runtime import QlibRuntimeConfig
from ib_qlib_pipeline.runner.artifact_locator import find_new_pred_artifact, list_experiment_recorders
from ib_qlib_pipeline.runner.common import run_cmd


def run_qlib_workflow(
    *,
    runtime_cfg: QlibRuntimeConfig,
    runtime_workflow_path: Path,
    experiment_name: str,
    cwd: Path,
    console_lines: list[str],
) -> tuple[Path, str, str]:
    qlib_qrun = runtime_cfg.qlib_qrun_bin
    if not qlib_qrun.exists():
        raise SystemExit(f"Missing qrun: {qlib_qrun}")

    qrun_env = os.environ.copy()
    qrun_env["GIT_DIR"] = str(runtime_cfg.qlib_repo_path / ".git")
    qrun_env["GIT_WORK_TREE"] = str(runtime_cfg.qlib_repo_path)

    _, before_recorders = list_experiment_recorders(runtime_cfg.mlruns_dir, experiment_name)
    run_cmd([str(qlib_qrun), str(runtime_workflow_path)], cwd=cwd, console_lines=console_lines, env=qrun_env)
    return find_new_pred_artifact(
        mlruns_dir=runtime_cfg.mlruns_dir,
        experiment_name=experiment_name,
        before_recorder_ids=before_recorders,
    )
