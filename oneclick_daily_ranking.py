#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from ib_qlib_pipeline.runner.daily_ranking_runner import DailyRankingRunRequest, DailyRankingRunner


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="One-click: incremental data update + qlib run + ranking export")
    parser.add_argument("--client-id", type=int, default=151, help="IB client id (default: 151)")
    parser.add_argument("--lookback-days", type=int, default=7, help="Incremental fetch window in days (default: 7)")
    parser.add_argument(
        "--workflow-base",
        default="examples/workflow_us_lgb_2020_port.yaml",
        help="Base workflow yaml path",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parent
    runner = DailyRankingRunner(
        DailyRankingRunRequest(
            project_root=project_root,
            client_id=args.client_id,
            lookback_days=args.lookback_days,
            workflow_base=args.workflow_base,
        )
    )
    runner.run()


if __name__ == "__main__":
    main()
