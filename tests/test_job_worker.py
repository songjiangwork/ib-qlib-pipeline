from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ib_qlib_pipeline.webapi.db import init_db
from ib_qlib_pipeline.webapi.job_store import create_job, get_job
from ib_qlib_pipeline.webapi.schedule_store import create_schedule, get_schedule
from ib_qlib_pipeline.webapi.settings import Settings
from ib_qlib_pipeline.webapi.worker import JobCanceledError, run_worker


class WorkerTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmp.name) / "test.db"
        init_db(self.db_path)
        self.project_root = Path(__file__).resolve().parent.parent
        self.settings = Settings(
            project_root=self.project_root,
            db_path=self.db_path,
            timezone="America/Denver",
            default_workflow_base="examples/workflow_us_lgb_2020_port.yaml",
            run_script_path=self.project_root / "run_daily_ranking.sh",
            api_host="127.0.0.1",
            api_port=8001,
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_run_worker_once_no_job_exits_cleanly(self) -> None:
        with patch("ib_qlib_pipeline.webapi.worker.Settings.load", return_value=self.settings):
            run_worker(once=True, poll_interval=0.0, worker_id="worker-1", stale_timeout_seconds=60)

    def test_run_worker_once_marks_job_succeeded_and_updates_schedule(self) -> None:
        schedule = create_schedule(
            self.db_path,
            {
                "name": "ranking",
                "schedule_type": "ranking",
                "enabled": True,
                "timezone": "America/Denver",
                "day_of_week": "mon-fri",
                "hour": 14,
                "minute": 40,
                "client_id": 151,
                "lookback_days": 7,
                "workflow_base": "examples/workflow_us_lgb_2020_port.yaml",
                "pipeline_start_date": None,
                "pipeline_include_portfolio": True,
            },
            "examples/workflow_us_lgb_2020_port.yaml",
        )
        job_id = create_job(
            self.db_path,
            job_type="scheduled_ranking_run",
            title="Scheduled ranking",
            payload_json='{"schedule_id": %d, "workflow_base": "examples/workflow_us_lgb_2020_port.yaml"}'
            % schedule["id"],
        )
        fake_executor = SimpleNamespace(execute_job=lambda job: None)
        with (
            patch("ib_qlib_pipeline.webapi.worker.Settings.load", return_value=self.settings),
            patch("ib_qlib_pipeline.webapi.worker.JobExecutor", return_value=fake_executor),
        ):
            run_worker(once=True, poll_interval=0.0, worker_id="worker-1", stale_timeout_seconds=60)

        job = get_job(self.db_path, job_id)
        updated_schedule = get_schedule(self.db_path, schedule["id"])
        self.assertEqual("succeeded", job["status"])
        self.assertEqual("worker-1", job["worker_id"])
        self.assertEqual("succeeded", updated_schedule["last_run_status"])

    def test_run_worker_once_marks_job_canceled(self) -> None:
        job_id = create_job(
            self.db_path,
            job_type="refresh_data",
            title="Refresh",
            payload_json="{}",
        )

        def _raise(_job):
            raise JobCanceledError("Job canceled")

        fake_executor = SimpleNamespace(execute_job=_raise)
        with (
            patch("ib_qlib_pipeline.webapi.worker.Settings.load", return_value=self.settings),
            patch("ib_qlib_pipeline.webapi.worker.JobExecutor", return_value=fake_executor),
        ):
            run_worker(once=True, poll_interval=0.0, worker_id="worker-1", stale_timeout_seconds=60)

        job = get_job(self.db_path, job_id)
        self.assertEqual("canceled", job["status"])
        self.assertEqual("Job canceled", job["cancel_reason"])


if __name__ == "__main__":
    unittest.main()
