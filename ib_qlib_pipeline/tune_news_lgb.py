from __future__ import annotations

import argparse
import csv
import itertools
import json
import random
from pathlib import Path

import pandas as pd
import qlib
from qlib.utils import init_instance_by_config


NEWS_FIELDS = ["news_count", "news_sentiment", "news_negative_ratio"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Tune LGB + news feature subset for Alpha158News.")
    p.add_argument("--provider-uri", required=True)
    p.add_argument("--region", default="us")
    p.add_argument("--trials", type=int, default=16)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out-csv", default="data/processed/tuning/news_lgb_trials.csv")
    p.add_argument("--out-best-json", default="data/processed/tuning/news_lgb_best.json")
    return p.parse_args()


def _all_feature_subsets() -> list[list[str]]:
    out: list[list[str]] = [[]]
    for r in range(1, len(NEWS_FIELDS) + 1):
        for c in itertools.combinations(NEWS_FIELDS, r):
            out.append(list(c))
    return out


def _sample_params(rng: random.Random) -> dict:
    return {
        "loss": "mse",
        "colsample_bytree": rng.choice([0.6, 0.8, 1.0]),
        "learning_rate": rng.choice([0.01, 0.02, 0.03, 0.05, 0.08]),
        "subsample": rng.choice([0.7, 0.8, 0.9, 1.0]),
        "lambda_l1": rng.choice([0.0, 1.0, 10.0, 100.0, 200.0]),
        "lambda_l2": rng.choice([0.0, 10.0, 100.0, 300.0, 600.0]),
        "max_depth": rng.choice([6, 8, 10, 12]),
        "num_leaves": rng.choice([31, 63, 127, 210, 255]),
        "min_child_samples": rng.choice([10, 20, 40, 80]),
        "num_threads": 4,
    }


def _dataset_config(news_fields: list[str]) -> dict:
    return {
        "class": "DatasetH",
        "module_path": "qlib.data.dataset",
        "kwargs": {
            "handler": {
                "class": "Alpha158News",
                "module_path": "ib_qlib_pipeline.handlers",
                "kwargs": {
                    "start_time": "2020-01-02",
                    "end_time": "2026-03-06",
                    "fit_start_time": "2020-01-02",
                    "fit_end_time": "2023-12-29",
                    "instruments": "all",
                    "news_fields": news_fields,
                },
            },
            "segments": {
                "train": ["2020-01-02", "2023-12-29"],
                "valid": ["2024-01-02", "2024-12-31"],
                "test": ["2025-01-02", "2026-03-06"],
            },
        },
    }


def _model_config(params: dict) -> dict:
    return {
        "class": "LGBModel",
        "module_path": "qlib.contrib.model.gbdt",
        "kwargs": params,
    }


def _daily_ic(df: pd.DataFrame, method: str) -> float:
    vals = df.groupby(level="datetime").apply(lambda x: x["score"].corr(x["LABEL0"], method=method))
    vals = vals.dropna()
    if len(vals) == 0:
        return 0.0
    return float(vals.mean())


def _eval_valid_ic(model, dataset) -> tuple[float, float]:
    pred = model.predict(dataset, segment="valid")
    label = dataset.prepare("valid", col_set="label", data_key="learn")
    merged = pred.to_frame("score").join(label, how="inner").dropna()
    return _daily_ic(merged, "pearson"), _daily_ic(merged, "spearman")


def run() -> int:
    args = parse_args()
    rng = random.Random(args.seed)
    subsets = _all_feature_subsets()
    qlib.init(provider_uri=args.provider_uri, region=args.region)

    dataset_cache: dict[tuple[str, ...], object] = {}
    trial_rows: list[dict] = []

    for i in range(1, args.trials + 1):
        subset = rng.choice(subsets)
        subset_key = tuple(subset)
        if subset_key not in dataset_cache:
            dataset_cache[subset_key] = init_instance_by_config(_dataset_config(subset))
        dataset = dataset_cache[subset_key]

        params = _sample_params(rng)
        model = init_instance_by_config(_model_config(params))
        evals_result: dict = {}
        model.fit(dataset, evals_result=evals_result)
        valid_loss = float(min(evals_result["valid"]["l2"]))
        valid_ic, valid_rank_ic = _eval_valid_ic(model, dataset)
        row = {
            "trial": i,
            "news_fields": ",".join(subset),
            "valid_loss_min": valid_loss,
            "valid_ic": valid_ic,
            "valid_rank_ic": valid_rank_ic,
            **params,
        }
        trial_rows.append(row)
        print(
            f"trial={i:02d} fields={row['news_fields'] or '(none)'} "
            f"ic={valid_ic:.6f} ric={valid_rank_ic:.6f} valid_loss={valid_loss:.6f}"
        )

    trial_rows.sort(key=lambda x: (x["valid_ic"], x["valid_rank_ic"]), reverse=True)
    best = trial_rows[0]

    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(trial_rows[0].keys()))
        writer.writeheader()
        writer.writerows(trial_rows)

    out_best = Path(args.out_best_json)
    out_best.parent.mkdir(parents=True, exist_ok=True)
    out_best.write_text(json.dumps(best, indent=2), encoding="utf-8")

    print("best_trial", json.dumps(best, ensure_ascii=False))
    print("saved_csv", str(out_csv))
    print("saved_best", str(out_best))
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
