from __future__ import annotations

from pathlib import Path

import yaml


def list_experiment_recorders(mlruns_dir: Path, experiment_name: str) -> tuple[str | None, set[str]]:
    if not mlruns_dir.exists():
        return None, set()
    for exp_dir in mlruns_dir.iterdir():
        if not exp_dir.is_dir():
            continue
        meta_path = exp_dir / "meta.yaml"
        if not meta_path.exists():
            continue
        try:
            meta = yaml.safe_load(meta_path.read_text(encoding="utf-8")) or {}
        except Exception:  # noqa: BLE001
            continue
        if str(meta.get("name")) != experiment_name:
            continue
        recorder_ids = {
            child.name
            for child in exp_dir.iterdir()
            if child.is_dir() and (child / "meta.yaml").exists()
        }
        return exp_dir.name, recorder_ids
    return None, set()


def find_new_pred_artifact(
    *,
    mlruns_dir: Path,
    experiment_name: str,
    before_recorder_ids: set[str],
) -> tuple[Path, str, str]:
    exp_id, after_recorder_ids = list_experiment_recorders(mlruns_dir, experiment_name)
    if exp_id is None:
        raise RuntimeError(f"Experiment not found after qrun: {experiment_name}")
    new_recorders = sorted(after_recorder_ids - before_recorder_ids)
    if len(new_recorders) != 1:
        raise RuntimeError(
            f"Expected exactly 1 new recorder for experiment={experiment_name}, "
            f"found {len(new_recorders)}: {new_recorders}"
        )
    rec_id = new_recorders[0]
    pred_path = mlruns_dir / exp_id / rec_id / "artifacts" / "pred.pkl"
    if not pred_path.exists():
        raise RuntimeError(f"pred.pkl not found for experiment={experiment_name} recorder={rec_id}: {pred_path}")
    return pred_path, exp_id, rec_id
