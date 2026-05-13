from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from ib_qlib_pipeline.ranking.ranking_loader import (
    build_ranking_for_signal_date,
    load_ranking_dataframe,
    read_prediction_dataframe,
)


class RankingLoaderTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.project_root = Path(self._tmp.name)
        price_dir = self.project_root / "data" / "processed" / "qlib_csv"
        price_dir.mkdir(parents=True, exist_ok=True)
        (self.project_root / "data" / "market").mkdir(parents=True, exist_ok=True)
        (price_dir / "AAA.csv").write_text(
            "\n".join(
                [
                    "symbol,date,open,high,low,close,volume,factor",
                    "AAA,2026-05-08,10,11,9,10.5,100,1",
                    "AAA,2026-05-09,11,12,10,11.5,100,1",
                ]
            ),
            encoding="utf-8",
        )
        (price_dir / "BBB.csv").write_text(
            "\n".join(
                [
                    "symbol,date,open,high,low,close,volume,factor",
                    "BBB,2026-05-08,20,21,19,20.5,100,1",
                    "BBB,2026-05-09,21,22,20,21.5,100,1",
                ]
            ),
            encoding="utf-8",
        )
        index = pd.MultiIndex.from_tuples(
            [
                (pd.Timestamp("2026-05-08"), "AAA"),
                (pd.Timestamp("2026-05-08"), "BBB"),
                (pd.Timestamp("2026-05-09"), "AAA"),
                (pd.Timestamp("2026-05-09"), "BBB"),
            ],
            names=["datetime", "instrument"],
        )
        frame = pd.DataFrame({"score": [0.2, 0.1, 0.3, 0.05]}, index=index)
        self.pred_path = self.project_root / "pred.pkl"
        frame.to_pickle(self.pred_path)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_read_prediction_dataframe_standardizes_columns(self) -> None:
        pred_df = read_prediction_dataframe(self.pred_path)
        self.assertEqual(["signal_datetime", "symbol", "score"], list(pred_df.columns))
        self.assertEqual(4, len(pred_df))
        self.assertEqual("AAA", pred_df.iloc[0]["symbol"])

    def test_build_ranking_for_signal_date_uses_lookup(self) -> None:
        pred_df = read_prediction_dataframe(self.pred_path)
        ranking = build_ranking_for_signal_date(
            pred_df=pred_df,
            signal_date=dt.date(2026, 5, 9),
            exp_id="exp1",
            rec_id="rec1",
            close_lookup={
                ("AAA", dt.date(2026, 5, 9)): 11.5,
                ("BBB", dt.date(2026, 5, 9)): 21.5,
            },
        )
        self.assertEqual(["AAA", "BBB"], ranking["symbol"].tolist())
        self.assertEqual([1, 2], ranking["rank"].tolist())
        self.assertEqual([11.5, 21.5], ranking["close"].tolist())
        self.assertEqual("exp1", ranking.iloc[0]["experiment_id"])
        self.assertEqual("rec1", ranking.iloc[0]["recorder_id"])

    def test_load_ranking_dataframe_defaults_to_latest_signal_date(self) -> None:
        ranking = load_ranking_dataframe(
            project_root=self.project_root,
            pred_path=self.pred_path,
            exp_id="exp2",
            rec_id="rec2",
        )
        self.assertEqual(dt.date(2026, 5, 9), pd.to_datetime(ranking.iloc[0]["signal_date"]).date())
        self.assertEqual(["AAA", "BBB"], ranking["symbol"].tolist())
        self.assertEqual([11.5, 21.5], ranking["close"].tolist())


if __name__ == "__main__":
    unittest.main()
