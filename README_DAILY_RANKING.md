# Daily Ranking One-Click

## Goal

One command to:

1. pull latest incremental price data from IB (`ib-qlib-pipeline`)
2. update Qlib binary data (`dump_bin`)
3. run Qlib workflow
4. export full ranking CSV (with close price)

## Files

- runner script: `run_daily_ranking.sh`
- core logic: `oneclick_daily_ranking.py`
- output ranking: `reports/rankings/sp500_ranking_YYYY-MM-DD.csv`
  - if same day re-run: `sp500_ranking_YYYY-MM-DD-01.csv`, `-02.csv`, ...

## Prerequisites

1. IB Gateway / TWS is running and API is enabled.
2. `config.yaml` points to reachable IB host/port.
3. Python env for pipeline exists:
   - `/home/song/projects/ib-qlib-pipeline/.venv`
4. Qlib env exists:
   - `/home/song/projects/qlib/.venv`
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

3. Runs qrun with that runtime yaml.
4. Reads latest `pred.pkl` from `mlruns`, merges same-day close, exports ranking csv.

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

4. `oneclick_daily_ranking.py` finds the newest `pred.pkl` under `mlruns/`.
5. It loads that pickle into pandas.
6. It detects the latest prediction date (`signal_date`).
7. It filters the dataframe to only that one date.
8. It sorts all symbols by `score` descending.
9. It assigns:
   - `rank = 1, 2, 3, ...`
   - `percentile`
10. It looks up the same-day `close` price from `data/processed/qlib_csv/<SYMBOL>.csv`.
11. It writes the final ranking CSV to:

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
/home/song/projects/qlib/.venv/bin/qrun reports/tmp/workflow_runtime_YYYY-MM-DD.yaml
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

- ranking loader/export: `oneclick_daily_ranking.py`
- workflow config: `examples/workflow_us_lgb_2020_port.yaml`
- Qlib prediction artifact generation: `qlib.workflow.record_temp.SignalRecord`

If you want to change how the ranking is generated, the main levers are:

- change the workflow yaml model or dataset config
- change the test segment end date logic
- change how `oneclick_daily_ranking.py` filters/sorts `pred.pkl`

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
  - verify `/home/song/projects/qlib/.venv/bin/qrun` exists.
