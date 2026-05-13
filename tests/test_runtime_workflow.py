from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path

from ib_qlib_pipeline.runner.runtime_workflow import build_bulk_backfill_runtime_workflow
from ib_qlib_pipeline.qlib_runtime import QlibRuntimeConfig


class RuntimeWorkflowTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.project_root = Path(self._tmp.name)
        self.runtime_cfg = QlibRuntimeConfig(
            project_root=self.project_root,
            qlib_repo_path=self.project_root / "qlib",
            qlib_python_bin=self.project_root / "qlib" / ".venv" / "bin" / "python",
            qlib_qrun_bin=self.project_root / "qlib" / ".venv" / "bin" / "qrun",
            data_dir=self.project_root / "data_union",
            workspace_dir=self.project_root / "reports" / "tmp",
            mlruns_dir=self.project_root / "mlruns",
        )
        self.base_workflow = {
            "qlib_init": {"provider_uri": "data/qlib/us_data_custom", "region": "us"},
            "data_handler_config": {
                "start_time": "2020-01-02",
                "end_time": "2026-03-06",
                "fit_start_time": "2020-01-02",
                "fit_end_time": "2023-12-29",
                "instruments": "all",
            },
            "port_analysis_config": {"backtest": {"end_time": "2026-03-06"}},
            "task": {
                "dataset": {"kwargs": {"segments": {"train": ["2020-01-02", "2023-12-29"], "valid": ["2024-01-02", "2024-12-31"], "test": ["2025-01-02", "2026-03-06"]}}},
                "record": [
                    {"class": "SignalRecord"},
                    {"class": "SigAnaRecord"},
                    {"class": "PortAnaRecord"},
                ],
            },
        }

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_build_bulk_backfill_runtime_workflow_sets_test_window_and_disables_port(self) -> None:
        runtime_wf, experiment_name = build_bulk_backfill_runtime_workflow(
            runtime_cfg=self.runtime_cfg,
            workflow_base="examples/workflow_us_lgb_2020_port.yaml",
            model_key="lgb",
            start_date=dt.date(2025, 12, 1),
            end_date=dt.date(2025, 12, 5),
            backtest_end=dt.date(2025, 12, 4),
            base_workflow=self.base_workflow,
        )
        text = runtime_wf.read_text(encoding="utf-8")
        self.assertIn("2025-12-01", text)
        self.assertIn("2025-12-05", text)
        self.assertIn("2025-12-04", text)
        self.assertNotIn("PortAnaRecord", text)
        self.assertIn("bulk_backfill_lgb", experiment_name)


if __name__ == "__main__":
    unittest.main()
