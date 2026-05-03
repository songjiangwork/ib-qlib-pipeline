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

## 10. Ranking Backend / SQLite / UI

这一层是在原始数据抓取和 Qlib workflow 之上，增加了一套可长期运行的服务：

- 定时或手动触发 `run_daily_ranking.sh`
- 将 ranking 结果写入 SQLite
- 回填历史 ranking 到 `reports/rankings/` 和数据库
- 将 ranking 转成 lot 级别的持仓生命周期
- 通过 Angular UI 查看某天 `TOP20` 和某只股票的完整进出场过程

### 10.1 主要脚本的职责

`run_daily_ranking.sh`
- 面向“当天实时运行”
- 会更新本地数据，并生成最新 ranking

`backfill_rankings.py`
- 面向“历史回放”
- 使用已有本地数据重建历史 ranking
- 生成 CSV / HTML，并写入 SQLite
- 常用 `--html-mode cached`，不依赖 IB Gateway 在线

`simulate_portfolio.py`
- 面向“持仓生命周期回放”
- 读取 SQLite 中已有 ranking
- 按规则生成每只股票的买入、卖出和每日 mark
- 不依赖 IB Gateway 在线

### 10.2 启动后端

安装依赖：

```bash
cd /home/song/projects/ib-qlib-pipeline
source .venv/bin/activate
pip install -r requirements.txt
```

直接启动：

```bash
cd /home/song/projects/ib-qlib-pipeline
source .venv/bin/activate
python run_backend.py
```

后台启动：

```bash
cd /home/song/projects/ib-qlib-pipeline
RANKING_API_PORT=8001 ./start_backend.sh
```

停止：

```bash
cd /home/song/projects/ib-qlib-pipeline
./stop_backend.sh
```

默认配置：

- Host: `0.0.0.0`
- Port: `8000`
- 当前常用端口：`8001`
- OpenAPI: `http://127.0.0.1:8001/docs`
- SQLite: `data/app/ranking_service.db`

可选环境变量：

- `RANKING_API_DB_PATH`
- `RANKING_API_TIMEZONE`
- `RANKING_API_HOST`
- `RANKING_API_PORT`
- `RANKING_API_RUN_SCRIPT`

### 10.3 启动前端

前端工程在 `frontend/`，默认监听 `9991`，并将 `/api` 代理到后端。

启动：

```bash
cd /home/song/projects/ib-qlib-pipeline
FRONTEND_BACKEND_PORT=8001 ./start_frontend.sh
```

停止：

```bash
cd /home/song/projects/ib-qlib-pipeline
./stop_frontend.sh
```

也可以直接运行 Angular dev server：

```bash
cd /home/song/projects/ib-qlib-pipeline/frontend
npm start
```

访问：

- `http://127.0.0.1:9991/rankings`
- `http://127.0.0.1:9991/symbols/<SYMBOL>`

### 10.4 SQLite schema

当前没有 ORM，后端直接使用 `sqlite3 + 手写 SQL`。

主要表：

`schedules`
- 定时任务配置

`runs`
- 每次 ranking 执行记录

`recommendations`
- 某次 run 的完整推荐列表

`portfolio_runs`
- 一次 portfolio 生命周期回放任务

`portfolio_lots`
- 一只股票一次完整买入到卖出的 lot

`portfolio_marks`
- 某个 lot 在每个交易日的盯市记录

数据库文件：

- `data/app/ranking_service.db`

### 10.5 常用 API

基础：

- `GET /api/config`
- `GET /api/schedules`
- `POST /api/schedules`
- `PATCH /api/schedules/{id}`
- `DELETE /api/schedules/{id}`
- `GET /api/runs`
- `POST /api/runs`
- `GET /api/runs/{id}`
- `GET /api/runs/{id}/recommendations`

Ranking date 列表：

- `GET /api/ranking-dates?limit=20&offset=0`
- `GET /api/ranking-dates?query=2026-01&limit=20&offset=0`

Portfolio：

- `GET /api/portfolio-runs`
- `GET /api/portfolio-runs/{id}`
- `GET /api/portfolio-runs/{id}/lots`
- `GET /api/portfolio-runs/{id}/symbols/{symbol}`
- `GET /api/portfolio-lots/{lot_id}/marks`

示例：

```bash
curl "http://127.0.0.1:8001/api/ranking-dates?limit=20&offset=0"
curl "http://127.0.0.1:8001/api/runs/1/recommendations?horizons=1,5,10,21"
curl "http://127.0.0.1:8001/api/portfolio-runs/1/symbols/COIN"
```

### 10.6 历史 ranking 回填

从 `2025-01-01` 开始批量生成历史 ranking，并同时写文件和数据库：

```bash
cd /home/song/projects/ib-qlib-pipeline
source .venv/bin/activate
python backfill_rankings.py --start-date 2025-01-01 --html-mode cached
```

指定结束日期：

```bash
python backfill_rankings.py --start-date 2025-01-01 --end-date 2026-05-01 --html-mode cached
```

跳过已存在的 DB 和文件：

```bash
python backfill_rankings.py \
  --start-date 2025-02-01 \
  --end-date 2026-05-01 \
  --html-mode cached \
  --skip-existing-db \
  --skip-existing-files
```

说明：

- 输出 CSV: `reports/rankings/sp500_ranking_YYYY-MM-DD.csv`
- 输出 HTML: `reports/rankings/sp500_ranking_YYYY-MM-DD.html`
- 同时插入 `runs` 和 `recommendations`
- `cached` 模式不会为每个历史日期发实时 IB 请求

### 10.7 Portfolio lot 生命周期回放

基于历史 ranking 生成股票级别生命周期：

```bash
cd /home/song/projects/ib-qlib-pipeline
source .venv/bin/activate
python simulate_portfolio.py \
  --start-date 2025-01-02 \
  --end-date 2026-05-01 \
  --name top10-hold20-2025-01-02-to-2026-05-01
```

当前规则：

- 使用 `T-1` ranking
- 在 `T` 日开盘成交
- 新进 `TOP10` 时按 `10,000 USD` 买入整数股
- 只要股票仍在 `TOP20` 就继续持有
- 一旦跌出 `TOP20`，就在下一交易日开盘卖出
- 不再平衡，不设总资金上限

这套设计的重点不是组合净值，而是单只股票的生命周期：

- 什么时候买入
- 买入价格和股数
- 什么时候卖出
- 卖出价格
- 已实现盈亏
- 任意中间日期的未实现盈亏

### 10.8 当前前端页面

当前 Angular UI 主要是两页：

`/rankings`
- 左侧边栏：
  - `Portfolio Runs`
  - `Ranking Dates`
  - `Symbols`
- 右侧：
  - 当前选中 signal day 的 `TOP20`
  - 买入 / 持有状态
  - 简单 summary

`/symbols/:symbol`
- 查看该股票在当前 `portfolio run` 下的完整生命周期
- 包括：
  - 所有 lots
  - entry / exit
  - realized pnl
  - 每日 marks
  - 是否仍在 `TOP20 / TOP10`
