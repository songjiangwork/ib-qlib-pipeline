from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path

from ib_qlib_pipeline.qlib_runtime import load_qlib_runtime_config
from ib_qlib_pipeline.ranking import TOP_N
from ib_qlib_pipeline.ranking.ranking_loader import (
    export_ranking_csv,
    load_ranking_dataframe,
    read_available_trading_days,
)
from ib_qlib_pipeline.reporting.company_enricher import fetch_topn_company_data
from ib_qlib_pipeline.reporting.html_report import build_html_report
from ib_qlib_pipeline.runner.common import log, run_cmd
from ib_qlib_pipeline.runner.qlib_runner import run_qlib_workflow
from ib_qlib_pipeline.runner.runtime_workflow import build_daily_runtime_workflow, load_base_workflow


@dataclass(frozen=True)
class DailyRankingRunRequest:
    project_root: Path
    client_id: int
    lookback_days: int
    workflow_base: str


class DailyRankingRunner:
    def __init__(self, request: DailyRankingRunRequest) -> None:
        self.request = request
        self.project_root = request.project_root
        self.runtime_cfg = load_qlib_runtime_config(self.project_root)
        self.console_lines: list[str] = []

    def run(self) -> dict[str, object]:
        today = dt.date.today()
        start_date = today - dt.timedelta(days=max(1, self.request.lookback_days))
        run_cmd(
            [
                str(self.project_root / ".venv" / "bin" / "python"),
                "run.py",
                "--config",
                "config.yaml",
                "--client-id",
                str(self.request.client_id),
                "--start-date",
                start_date.isoformat(),
                "--bar-size",
                "1 day",
                "--no-news",
                "--dump-bin",
            ],
            cwd=self.project_root,
            console_lines=self.console_lines,
        )

        runtime_wf, experiment_name, latest_trade_date, backtest_end = self._build_runtime_workflow(today)
        log(f"[ok] runtime workflow: {runtime_wf}", self.console_lines)
        log(f"[ok] latest_trade_date={latest_trade_date} backtest_end={backtest_end}", self.console_lines)
        log(f"[ok] experiment_name={experiment_name}", self.console_lines)

        pred_path, exp_id, rec_id = run_qlib_workflow(
            runtime_cfg=self.runtime_cfg,
            runtime_workflow_path=runtime_wf,
            experiment_name=experiment_name,
            cwd=self.project_root,
            console_lines=self.console_lines,
        )
        log(f"[ok] pred={pred_path}", self.console_lines)
        log(f"[ok] experiment_id={exp_id} recorder_id={rec_id}", self.console_lines)

        ranking_df = load_ranking_dataframe(self.project_root, pred_path, exp_id, rec_id)
        csv_path = export_ranking_csv(self.project_root, ranking_df, self.console_lines)
        log(f"[top{TOP_N}]", self.console_lines)
        topn_text = ranking_df[["rank", "symbol", "score", "close"]].head(TOP_N).to_string(index=False)
        print(topn_text)
        self.console_lines.extend(topn_text.splitlines())

        topn_details = fetch_topn_company_data(
            self.project_root,
            ranking_df.head(TOP_N),
            self.request.client_id,
            self.console_lines,
        )
        html_stamp = dt.datetime.now().strftime("%H%M%S")
        run_date_str = ranking_df["run_date"].iloc[0].date().isoformat()
        html_path = csv_path.with_name(f"sp500_ranking_{run_date_str}_{html_stamp}.html")
        build_html_report(ranking_df, topn_details, html_path, self.console_lines)
        log(f"[ok] html report exported: {html_path}", self.console_lines)
        return {
            "runtime_workflow": runtime_wf,
            "experiment_name": experiment_name,
            "pred_path": pred_path,
            "experiment_id": exp_id,
            "recorder_id": rec_id,
            "ranking_csv_path": csv_path,
            "html_report_path": html_path,
        }

    def _build_runtime_workflow(self, today: dt.date) -> tuple[Path, str, dt.date, dt.date]:
        trading_days = read_available_trading_days(self.project_root)
        latest_trade_date = trading_days[-1]
        backtest_end = trading_days[-2]
        if backtest_end < dt.date(2025, 1, 2):
            backtest_end = dt.date(2025, 1, 2)
        base_workflow = load_base_workflow(self.project_root, self.request.workflow_base)
        runtime_wf, experiment_name = build_daily_runtime_workflow(
            runtime_cfg=self.runtime_cfg,
            workflow_base=self.request.workflow_base,
            latest_trade_date=latest_trade_date,
            backtest_end=backtest_end,
            today=today,
            base_workflow=base_workflow,
        )
        return runtime_wf, experiment_name, latest_trade_date, backtest_end
