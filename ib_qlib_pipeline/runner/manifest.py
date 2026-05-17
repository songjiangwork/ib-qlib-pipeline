from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


def _normalize_mapping(value: dict[str, Any] | None) -> dict[str, Any]:
    return dict(value or {})


def _path_to_text(value: str | Path | None, *, project_root: Path | None = None) -> str | None:
    if value is None:
        return None
    path = Path(value)
    if project_root is not None:
        try:
            return str(path.resolve().relative_to(project_root.resolve()))
        except ValueError:
            return str(path.resolve())
    return str(path)


def sha256_file(path: str | Path | None) -> str | None:
    if path is None:
        return None
    file_path = Path(path)
    if not file_path.exists():
        return None
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def sanitize_manifest_token(value: str | None, fallback: str = "run") -> str:
    text = (value or fallback).strip().lower()
    text = re.sub(r"[^a-z0-9._-]+", "_", text)
    text = text.strip("._-")
    return text or fallback


def build_manifest_path(
    project_root: Path,
    *,
    job_type: str,
    model_key: str | None = None,
    date_token: str | None = None,
    created_at: dt.datetime | None = None,
) -> Path:
    stamp = (created_at or dt.datetime.now(dt.timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    parts = [sanitize_manifest_token(job_type)]
    if model_key:
        parts.append(sanitize_manifest_token(model_key))
    if date_token:
        parts.append(sanitize_manifest_token(date_token))
    parts.append(stamp)
    return project_root / "reports" / "manifests" / f"{'_'.join(parts)}.json"


@dataclass
class RunArtifactManifest:
    manifest_version: int
    job_type: str
    status: str
    model_id: int | None = None
    model_key: str | None = None
    model_name: str | None = None
    universe_id: int | None = None
    universe_key: str | None = None
    signal_date: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    ranking_csv_path: str | None = None
    html_report_path: str | None = None
    experiment_name: str | None = None
    experiment_id: str | None = None
    recorder_id: str | None = None
    pred_path: str | None = None
    workflow_base: str | None = None
    runtime_workflow_path: str | None = None
    runtime_workflow_hash: str | None = None
    config_path: str | None = None
    qlib_csv_dir: str | None = None
    qlib_bin_dir: str | None = None
    created_at: str = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc).isoformat())
    finished_at: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["extra"] = _normalize_mapping(self.extra)
        return payload

    def write_json(self, path: str | Path) -> Path:
        manifest_path = Path(path)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        return manifest_path

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "RunArtifactManifest":
        data = dict(payload)
        data["extra"] = _normalize_mapping(data.get("extra"))
        return cls(**data)

    @classmethod
    def read_json(cls, path: str | Path) -> "RunArtifactManifest":
        manifest_path = Path(path)
        return cls.from_dict(json.loads(manifest_path.read_text(encoding="utf-8")))


def build_run_artifact_manifest(
    *,
    project_root: Path,
    manifest_path: str | Path | None = None,
    manifest_version: int = 1,
    job_type: str,
    status: str,
    model_id: int | None = None,
    model_key: str | None = None,
    model_name: str | None = None,
    universe_id: int | None = None,
    universe_key: str | None = None,
    signal_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    ranking_csv_path: str | Path | None = None,
    html_report_path: str | Path | None = None,
    experiment_name: str | None = None,
    experiment_id: str | None = None,
    recorder_id: str | None = None,
    pred_path: str | Path | None = None,
    workflow_base: str | None = None,
    runtime_workflow_path: str | Path | None = None,
    config_path: str | Path | None = None,
    qlib_csv_dir: str | Path | None = None,
    qlib_bin_dir: str | Path | None = None,
    created_at: str | None = None,
    finished_at: str | None = None,
    extra: dict[str, Any] | None = None,
) -> tuple[RunArtifactManifest, Path]:
    manifest_obj = RunArtifactManifest(
        manifest_version=manifest_version,
        job_type=job_type,
        status=status,
        model_id=model_id,
        model_key=model_key,
        model_name=model_name,
        universe_id=universe_id,
        universe_key=universe_key,
        signal_date=signal_date,
        start_date=start_date,
        end_date=end_date,
        ranking_csv_path=_path_to_text(ranking_csv_path, project_root=project_root),
        html_report_path=_path_to_text(html_report_path, project_root=project_root),
        experiment_name=experiment_name,
        experiment_id=experiment_id,
        recorder_id=recorder_id,
        pred_path=_path_to_text(pred_path, project_root=project_root),
        workflow_base=workflow_base,
        runtime_workflow_path=_path_to_text(runtime_workflow_path, project_root=project_root),
        runtime_workflow_hash=sha256_file(runtime_workflow_path),
        config_path=_path_to_text(config_path, project_root=project_root),
        qlib_csv_dir=_path_to_text(qlib_csv_dir, project_root=project_root),
        qlib_bin_dir=_path_to_text(qlib_bin_dir, project_root=project_root),
        created_at=created_at or dt.datetime.now(dt.timezone.utc).isoformat(),
        finished_at=finished_at,
        extra=_normalize_mapping(extra),
    )
    target_path = Path(manifest_path) if manifest_path is not None else build_manifest_path(
        project_root,
        job_type=job_type,
        model_key=model_key,
        date_token=signal_date or (f"{start_date}_{end_date}" if start_date and end_date else None),
    )
    return manifest_obj, target_path
