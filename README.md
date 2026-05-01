# IB -> Qlib Pipeline

用于从 IB API 拉取股票行情并清洗后输出为 Qlib 可导入的 CSV，并可选直接执行 `dump_bin.py` 生成 Qlib 二进制数据。

## 功能

- 参数化股票列表（默认 `META, ISRG`）
- 参数化时间区间（默认近 10 年，可改任意时段）
- 参数化 K 线粒度（默认 `1 day`）
- 输出标准 Qlib CSV：`date,symbol,open,high,low,close,volume,factor`
- 可选自动调用 Qlib `scripts/dump_bin.py`

## 目录结构

```text
ib-qlib-pipeline/
  ib_qlib_pipeline/
    pipeline.py
  symbols/
    default_us.txt
  examples/
    config.example.yaml
  data/
    raw/prices/
    raw/news/
    processed/qlib_csv/
    qlib/us_data_custom/
```

## 1. 安装

```bash
cd /home/song/projects/ib-qlib-pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

## 2. 配置

```bash
cp examples/config.example.yaml config.yaml
```

然后编辑 `config.yaml`。

你给的连接参数如下：
- `host: 127.0.0.1`
- `port: 7497`
- `client_id: 101`
- `account: DUXXXXXXX`
- `trading_mode: paper`

### WSL 注意事项（非常关键）

如果你的 IB 客户端在 WSL 外主机（Windows）上。

- 在 WSL2 中，`127.0.0.1` 通常指向 WSL 自己，不一定是 Windows 主机。
- 如果你发现连接失败，请把 `host` 改成 Windows 主机 IP（例如 `ipconfig` 查到的局域网 IP，或 WSL 网关 IP）。
- 同时确认 TWS/IB Gateway API 设置里允许该来源 IP 访问，并且端口是 paper 对应端口（常见 `7497`）。

## 3. 运行

### 默认配置运行

```bash
cd /home/song/projects/ib-qlib-pipeline
source .venv/bin/activate
python run.py --config config.yaml
```

### 覆盖参数运行

```bash
python run.py \
  --config config.yaml \
  --symbols META,ISRG \
  --start-date 2016-01-01 \
  --bar-size "1 day" \
  --no-news \
  --no-dump-bin
```

## 4. 生成 Qlib bin（可选）

如果你已经有 `/home/song/projects/qlib` 和其 Python 环境：

1. 在 `config.yaml` 中设置：
   - `qlib.enabled: true`
   - `qlib.qlib_repo_path: /home/song/projects/qlib`
   - `qlib.python_bin: /home/song/projects/qlib/.venv/bin/python`
2. 运行 pipeline 后会自动执行：
   - `dump_bin.py dump_all`

## 5. 新闻（实验性，可选）

- 默认主线不使用新闻：`data.with_news: false`（建议保持）。
- 若你要临时启用新闻，可用 `--with-news` 或配置里改 `with_news: true`。
- 新闻抓取依赖 `reqNewsProviders` + `reqHistoricalNews`，若权限不足只会 warning，不影响行情主流程。

## 6. 输出文件

- 原始行情：`data/raw/prices/<SYMBOL>.csv`
- Qlib CSV：`data/processed/qlib_csv/<SYMBOL>.csv`
- 原始新闻：`data/raw/news/<SYMBOL>.csv`
- Qlib bin（可选）：`data/qlib/us_data_custom`

## 7. 默认训练入口（无新闻主线）

- 推荐 workflow：`examples/workflow_us_lgb_2020_port.yaml`
- 数据目录：`data/qlib/us_data_custom`
- 新闻相关 workflow（仅实验）：`examples/experimental/`

## 8. SEC 财报抓取（10-K/10-Q）

```bash
cd /home/song/projects/ib-qlib-pipeline
source .venv/bin/activate
python ib_qlib_pipeline/sec_filings_backfill.py \
  --symbols-file symbols/sp500_full_ib_map.txt \
  --out-dir data/raw/sec_filings \
  --user-agent "your-app/1.0 your_email@example.com" \
  --forms 10-K,10-Q \
  --start-date 2016-01-01
```

## 9. SEC 特征并入与回测

```bash
python ib_qlib_pipeline/sec_features.py \
  --symbols-file symbols/sp500_full_ib_map.txt \
  --filings-dir data/raw/sec_filings \
  --price-dir data/processed/qlib_csv \
  --out-dir data/processed/qlib_csv_sec
```

```bash
/home/song/projects/qlib/.venv/bin/python /home/song/projects/qlib/scripts/dump_bin.py dump_all \
  --data_path data/processed/qlib_csv_sec \
  --qlib_dir data/qlib/us_data_sec \
  --freq day \
  --date_field_name date \
  --symbol_field_name symbol \
  --include_fields open,close,high,low,volume,factor,sec_is_10k_day,sec_is_10q_day,sec_days_since_filing \
  --file_suffix .csv
```

```bash
PYTHONPATH=/home/song/projects/ib-qlib-pipeline /home/song/projects/qlib/.venv/bin/qrun \
  examples/workflow_us_lgb_2020_sec_port.yaml
```

## 10. Backend API（调度 / 历史查询 / 推荐表现）

后端会做三件事：

- 定时或手动触发 `run_daily_ranking.sh`
- 将每次运行的推荐结果保存到 SQLite
- 提供 API 查询任意一次运行的推荐列表，以及后续 1 / 5 / 10 / 21 个交易日和截至最新收盘的涨跌表现

### 安装新增依赖

```bash
cd /home/song/projects/ib-qlib-pipeline
source .venv/bin/activate
pip install -r requirements.txt
```

### 启动后端

```bash
cd /home/song/projects/ib-qlib-pipeline
source .venv/bin/activate
python run_backend.py
```

或者直接后台启动：

```bash
cd /home/song/projects/ib-qlib-pipeline
./start_backend.sh
```

停止后台服务：

```bash
cd /home/song/projects/ib-qlib-pipeline
./stop_backend.sh
```

默认监听：

- `http://127.0.0.1:8000`
- OpenAPI 文档：`http://127.0.0.1:8000/docs`

默认 SQLite 路径：

- `data/app/ranking_service.db`

可选环境变量：

- `RANKING_API_DB_PATH`
- `RANKING_API_TIMEZONE`
- `RANKING_API_HOST`
- `RANKING_API_PORT`
- `RANKING_API_RUN_SCRIPT`

### 主要 API

1. 创建一个工作日定时任务（例如每天 09:35 跑一次）

```bash
curl -X POST http://127.0.0.1:8000/api/schedules \
  -H "Content-Type: application/json" \
  -d '{
    "name": "weekday-open-run",
    "hour": 9,
    "minute": 35,
    "day_of_week": "mon-fri",
    "timezone": "America/Edmonton",
    "client_id": 151,
    "lookback_days": 7,
    "workflow_base": "examples/workflow_us_lgb_2020_port.yaml",
    "enabled": true
  }'
```

2. 手动触发一次运行

```bash
curl -X POST http://127.0.0.1:8000/api/runs \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": 151,
    "lookback_days": 7,
    "workflow_base": "examples/workflow_us_lgb_2020_port.yaml"
  }'
```

3. 查看历史运行列表

```bash
curl "http://127.0.0.1:8000/api/runs?limit=20"
```

4. 查看某一次运行的推荐列表和后验表现

```bash
curl "http://127.0.0.1:8000/api/runs/1/recommendations?horizons=1,5,10,21"
```

返回结果里每个推荐会附带：

- `1d`, `5d`, `10d`, `21d`：按交易日向后看的收益
- `latest`：从当时推荐价格到当前最新收盘价的收益
- `summary`：该次推荐整体的平均收益率和胜率
