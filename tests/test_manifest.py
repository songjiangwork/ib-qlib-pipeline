from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from ib_qlib_pipeline.runner.manifest import RunArtifactManifest, build_run_artifact_manifest
from ib_qlib_pipeline.webapi.db import init_db
from ib_qlib_pipeline.webapi.job_executor import JobExecutor
from ib_qlib_pipeline.webapi.model_store import ensure_default_models
from ib_qlib_pipeline.webapi.run_store import create_queued_run, get_run
from ib_qlib_pipeline.webapi.settings import Settings
from ib_qlib_pipeline.webapi.universe_store import ensure_default_universes


class ManifestTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.project_root = Path(self._tmp.name)
        self.db_path = self.project_root / "ranking_service.db"
        init_db(self.db_path)
        self.repo_root = Path(__file__).resolve().parent.parent
        ensure_default_universes(self.db_path, self.repo_root)
        ensure_default_models(self.db_path, self.repo_root)
        self.settings = Settings(
            project_root=self.project_root,
            db_path=self.db_path,
            timezone="America/Denver",
            default_workflow_base="examples/workflow_us_lgb_2020_port.yaml",
            run_script_path=self.repo_root / "run_daily_ranking.sh",
            api_host="127.0.0.1",
            api_port=8001,
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_manifest_roundtrip_and_hash(self) -> None:
        runtime_workflow = self.project_root / "reports" / "tmp" / "workflow.yaml"
        runtime_workflow.parent.mkdir(parents=True, exist_ok=True)
        runtime_workflow.write_text("task: {}\n", encoding="utf-8")
        ranking_csv = self.project_root / "reports" / "rankings" / "ranking.csv"
        ranking_csv.parent.mkdir(parents=True, exist_ok=True)
        ranking_csv.write_text("rank,symbol,score,percentile,close,signal_date\n1,AAPL,0.1,99,100,2026-05-15\n", encoding="utf-8")

        manifest, manifest_path = build_run_artifact_manifest(
            project_root=self.project_root,
            job_type="backfill_ranking",
            status="succeeded",
            model_id=1,
            model_key="lgb",
            signal_date="2026-05-15",
            start_date="2026-05-15",
            end_date="2026-05-15",
            ranking_csv_path=ranking_csv,
            runtime_workflow_path=runtime_workflow,
            config_path=self.project_root / "config.yaml",
            qlib_csv_dir=self.project_root / "data/processed/qlib_csv",
            qlib_bin_dir=self.project_root / "data/qlib/us_data_custom",
        )
        manifest.write_json(manifest_path)
        loaded = RunArtifactManifest.read_json(manifest_path)

        self.assertEqual("reports/rankings/ranking.csv", loaded.ranking_csv_path)
        self.assertEqual("reports/tmp/workflow.yaml", loaded.runtime_workflow_path)
        self.assertIsNotNone(loaded.runtime_workflow_hash)
        self.assertEqual("backfill_ranking", loaded.job_type)

    def test_job_executor_finalizes_from_manifest_without_regex(self) -> None:
        run_id = create_queued_run(
            db_path=self.db_path,
            schedule_id=None,
            model_id=1,
            trigger_source="manual",
            client_id=151,
            lookback_days=7,
            workflow_base="examples/workflow_us_lgb_2020_port.yaml",
            command="run_daily_ranking.sh",
        )
        ranking_df = pd.DataFrame(
            [
                {
                    "rank": 1,
                    "symbol": "AAPL",
                    "score": 0.5,
                    "percentile": 99.0,
                    "close": 123.4,
                    "signal_date": "2026-05-15",
                }
            ]
        )
        ranking_csv = self.project_root / "reports" / "rankings" / "daily.csv"
        ranking_csv.parent.mkdir(parents=True, exist_ok=True)
        ranking_df.to_csv(ranking_csv, index=False)

        manifest, manifest_path = build_run_artifact_manifest(
            project_root=self.project_root,
            manifest_path=self.project_root / "reports" / "manifests" / "daily.json",
            job_type="daily_ranking",
            status="succeeded",
            signal_date="2026-05-15",
            start_date="2026-05-15",
            end_date="2026-05-15",
            ranking_csv_path=ranking_csv,
            experiment_id="exp123",
            recorder_id="rec456",
            workflow_base="examples/workflow_us_lgb_2020_port.yaml",
            config_path=self.project_root / "config.yaml",
        )
        manifest.write_json(manifest_path)

        executor = JobExecutor(self.settings, worker_id="worker-1")
        executor._finalize_run_from_manifest(  # noqa: SLF001
            run_id,
            None,
            manifest_path,
            "stdout can contain anything; no regex markers required",
        )

        run = get_run(self.db_path, run_id)
        self.assertIsNotNone(run)
        assert run is not None
        self.assertEqual("succeeded", run["status"])
        self.assertEqual("reports/rankings/daily.csv", run["ranking_csv_path"])
        self.assertEqual("reports/manifests/daily.json", run["manifest_path"])
        self.assertEqual("exp123", run["experiment_id"])
        self.assertEqual("rec456", run["recorder_id"])

    def test_job_executor_missing_manifest_raises_clear_error(self) -> None:
        executor = JobExecutor(self.settings, worker_id="worker-1")
        missing = self.project_root / "reports" / "manifests" / "missing.json"
        with self.assertRaisesRegex(RuntimeError, "Missing manifest JSON"):
            executor._load_manifest(missing)  # noqa: SLF001


if __name__ == "__main__":
    unittest.main()
