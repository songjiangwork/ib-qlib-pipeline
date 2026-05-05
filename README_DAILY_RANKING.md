# Daily Ranking One-Click

## Goal

One command to:

1. pull latest incremental price data from IB (`ib-qlib-pipeline`)
2. update Qlib binary data (`dump_bin`)
3. run Qlib workflow
4. export full ranking CSV (with close price)

## Files

- runner script: `run_daily_ranking.sh`
- CLI entrypoint: `oneclick_daily_ranking.py`
- orchestration: `ib_qlib_pipeline/runner/daily_ranking_runner.py`
- output ranking: `reports/rankings/sp500_ranking_YYYY-MM-DD.csv`
  - if same day re-run: `sp500_ranking_YYYY-MM-DD-01.csv`, `-02.csv`, ...

## Prerequisites

1. IB Gateway / TWS is running and API is enabled.
2. `config.yaml` points to reachable IB host/port.
3. Python env for pipeline exists:
   - `/home/song/projects/ib-qlib-pipeline/.venv`
4. Qlib env exists:
   - path comes from `config.yaml` / env
   - includes `qrun`

## Usage (minimal input)

```bash
cd /home/song/projects/ib-qlib-pipeline
./run_daily_ranking.sh
```

That is all.

## Optional params

Usually you do not need these, but available if needed:

```bash
./run_daily_ranking.sh --client-id 152 --lookback-days 7
```

- `--client-id`: IB client id (default `151`)
- `--lookback-days`: incremental fetch window (default `7`)

## What the script does internally

1. Runs:

```bash
python run.py --config config.yaml --client-id <id> --start-date <today-lookback> --bar-size "1 day" --no-news --dump-bin
```

2. Builds a runtime workflow yaml under:

`reports/tmp/workflow_runtime_YYYY-MM-DD.yaml`

- `data_handler_config.end_time = latest available trading day in qlib calendar`
- `dataset.test end = latest available trading day`
- `backtest end = previous trading day` (avoids Qlib last-day backtest boundary issue)
- `experiment_name = unique runtime token`

3. Runs qrun with that runtime yaml.
4. Locates the new recorder created by this specific run.
5. Reads that recorder's `pred.pkl`, merges same-day close, exports ranking csv.

## How Ranking Is Produced

After `qrun` finishes, `oneclick_daily_ranking.py` does **not** query the model again by hand.
It simply reads the prediction artifact that Qlib has already written for the latest run:

`mlruns/<experiment_id>/<recorder_id>/artifacts/pred.pkl`

The flow is:

1. `qrun` executes the workflow yaml.
2. The yaml defines:
   - model: `LGBModel`
   - dataset: `DatasetH`
   - handler: `Alpha158`
   - records: `SignalRecord`, `SigAnaRecord`, `PortAnaRecord`
3. Inside Qlib, `SignalRecord` calls:

```python
pred = self.model.predict(self.dataset)
self.save(**{"pred.pkl": pred})
```

So the prediction matrix is already persisted by Qlib into `pred.pkl`.

4. `DailyRankingRunner` writes a unique `experiment_name` into the runtime workflow.
5. Before `qrun`, it snapshots recorder ids under that experiment.
6. After `qrun`, it finds the new recorder created by this exact run.
7. It loads that recorder's `pred.pkl` into pandas.
8. It detects the latest prediction date (`signal_date`).
9. It filters the dataframe to only that one date.
10. It sorts all symbols by `score` descending.
11. It assigns:
   - `rank = 1, 2, 3, ...`
   - `percentile`
12. It looks up the same-day `close` price from `data/processed/qlib_csv/<SYMBOL>.csv`.
13. It writes the final ranking CSV to:

`reports/rankings/sp500_ranking_YYYY-MM-DD.csv`

So the ranking list is literally the **latest-day cross-sectional sort of Qlib's predicted scores**.

## How It Uses The Qlib Model

`oneclick_daily_ranking.py` itself does not define or train a model.
It delegates the full modeling step to Qlib through `qrun`.

The workflow yaml currently uses:

- `LGBModel`: the LightGBM model implementation from `qlib.contrib.model.gbdt`
- `Alpha158`: the standard Qlib feature handler
- `DatasetH`: Qlib dataset wrapper with `train` / `valid` / `test` segments

In practice, the script does this:

1. Refreshes price data and regenerates qlib bin data.
2. Builds a runtime workflow yaml with updated end dates.
3. Calls:

```bash
<QLIB_QRUN_BIN> reports/tmp/workflow_runtime_YYYY-MM-DD.yaml
```

4. Qlib then:
   - initializes data from `data/qlib/us_data_custom`
   - builds Alpha158 features
   - trains `LGBModel` on the configured `train` segment
   - validates on `valid`
   - predicts on `test`
   - stores predictions as `pred.pkl`
   - optionally runs signal analysis and portfolio backtest

So the daily ranking is based on the exact same Qlib workflow outputs, not on a separate custom scoring path.

## Relevant Code Paths

- CLI entry: `oneclick_daily_ranking.py`
- daily orchestration: `ib_qlib_pipeline/runner/daily_ranking_runner.py`
- runtime workflow builder: `ib_qlib_pipeline/runner/runtime_workflow.py`
- qrun executor: `ib_qlib_pipeline/runner/qlib_runner.py`
- artifact locator: `ib_qlib_pipeline/runner/artifact_locator.py`
- ranking loader/export: `ib_qlib_pipeline/ranking/ranking_loader.py`
- workflow config: `examples/workflow_us_lgb_2020_port.yaml`
- Qlib prediction artifact generation: `qlib.workflow.record_temp.SignalRecord`
- runtime config: `ib_qlib_pipeline/qlib_runtime.py`

## Runtime Path Config

These paths are no longer hardcoded inside the ranking scripts.

Priority is:

1. environment variables
2. `config.yaml`

Supported environment variables:

- `QLIB_REPO_PATH`
- `QLIB_PYTHON_BIN`
- `QLIB_QRUN_BIN`
- `PROJECT_DATA_DIR`
- `RUN_WORKSPACE_DIR`
- `MLRUNS_DIR`

If you want to change how the ranking is generated, the main levers are:

- change the workflow yaml model or dataset config
- change the test segment end date logic
- change how `ib_qlib_pipeline/ranking/ranking_loader.py` filters/sorts `pred.pkl`

## Output columns

- `run_date`
- `signal_date`
- `rank`
- `symbol`
- `score`
- `percentile`
- `close`
- `experiment_id`
- `recorder_id`

## Troubleshooting

- If script says missing `.venv`:
  - create pipeline venv and install dependencies.
- If cannot connect IB:
  - check `config.yaml` host/port/client_id and TWS API settings.
- If `qrun` missing:
  - verify `QLIB_QRUN_BIN` or `config.yaml -> qlib.qrun_bin` points to a valid binary.
