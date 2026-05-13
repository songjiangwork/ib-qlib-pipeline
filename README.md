# IB Qlib Pipeline

基于 IB API 和 Qlib 的多模型日线 ranking / portfolio 系统。

当前项目已经从“脚本集合”演进成一个可长期运行的小型服务，核心能力包括：

- 从 IB 拉取并增量更新日线数据
- 将共享日线同步写入 `market_daily.sqlite3`
- 生成 Qlib CSV 和 Qlib bin
- 运行多套 Qlib workflow 训练 / 验证 / 预测
- 使用 bulk backfill 作为默认训练 / 回填入口
- 导出 ranking CSV / HTML
- 将 ranking 写入 SQLite
- 基于 ranking 回放 portfolio lot / mark 生命周期
- 管理 universe / strategy / model / portfolio 元数据
- 通过 FastAPI + Angular UI 查看 ranking、portfolio、股票详情、模型比较和任务状态
- 通过 `/operations` 执行或定时调度 `Daily Close Pipeline`

## 当前主入口

当前主入口不是脚本，而是：

- 后端 API
- 前端 `/operations`
- `Daily Close Pipeline`

推荐日常使用方式：

1. 启动前后端
2. 打开 `/operations`
3. 手动执行或定时调度 `Daily Close Pipeline`

CLI 脚本仍然保留，但主要用于：

- 初始化历史 backfill
- 排错
- 补跑
- 开发验证

## 系统结构

```text
ib-qlib-pipeline/
  ib_qlib_pipeline/
    pipeline.py              # IB -> CSV -> qlib bin
    runner/                  # ranking / qrun orchestration
    ranking/                 # pred.pkl -> ranking dataframe / export
    reporting/               # HTML / company enrich
    dborm/                   # SQLAlchemy ORM models / session
    webapi/                  # FastAPI service / stores / jobs
  frontend/                  # Angular UI
  examples/                  # workflow yaml / config example
  data/
    raw/prices/
    raw/company_meta/
    processed/qlib_csv/
    qlib/us_data_custom/
    app/ranking_service.db
  reports/
    rankings/
    tmp/
  docs/
```

## 主要数据流

### 1. 日线数据更新

`run.py` / `pipeline.py` 负责：

- 从 IB 拉增量日线
- 写 `data/raw/prices/*.csv`
- 写共享 daily SQLite：`data/market/market_daily.sqlite3`
- 写 `data/processed/qlib_csv/*.csv`
- 可选更新 `company_meta`
- 调用 Qlib `dump_bin`

### 2. Ranking 生成

当前默认入口是 `backfill_rankings_bulk.py`，旧的 `backfill_rankings.py` 仍保留作兼容 / 单日补跑。

bulk backfill 负责：

- 生成 runtime workflow
- 调用 `qrun`
- 从本次唯一 experiment / recorder 读取一次 `pred.pkl`
- 再按多个 `signal_date` 切分并导出 ranking
- 生成 ranking CSV / HTML
- 写 SQLite `runs` / `recommendations`

### 3. Portfolio 回放

`simulate_portfolio.py` 负责：

- 读取已有 ranking
- 按你的业务规则回放交易
- 生成：
  - `portfolio_runs`
  - `portfolio_lots`
  - `portfolio_marks`

### 4. Web 任务层

后端 jobs 系统负责：

- `refresh-data`
- `backfill-ranking`
- `append-portfolio`
- `daily-close-pipeline`

前端 `/operations` 负责：

- 手动触发这些任务
- 查看实时日志
- 查看今日状态
- 配置 schedule

## Daily Close Pipeline

这是当前最重要的业务流程。

一次 `Daily Close Pipeline` 会：

1. 刷新最新日线数据
2. 更新 Qlib bin
3. 对所有已注册 workflow / model 做 ranking backfill
4. 对已有 portfolio run 的模型做 portfolio append
5. 自动补齐上次成功运行到今天之间缺失的交易日

这意味着：

- 服务不一定每天都开着
- 只要之后重新跑一次 pipeline，系统会尽量补齐中间缺失的 ranking 和 portfolio

当前 pipeline 已经是 universe-aware：

- `SP500` 模型走 `config.yaml`
- `Union` 模型走 `config_union.yaml`
- 特殊实验模型可通过 model-level `config_path` 覆盖 universe 默认配置

## 模型体系

当前默认有两组模型族：

- `LightGBM_Default`
- `XGBoost_Default`
- `CatBoost_Default`

以及：

- `LightGBM_5D`
- `XGBoost_5D`
- `CatBoost_5D`

此外当前还包含：

- `Union` universe 对应的默认 / 5D 模型族
- `U16` 2016 训练窗实验族
- `CAT1_U16` 这种单模型实验线

这些模型会在 SQLite `models` 表中独立记录，并在 UI 中分开显示，不会互相污染。

模型和实验当前主要按以下维度组织：

- `Universe`
- `Model`
- `Strategy`
- `Portfolio Run`

## 数据库

当前默认数据库：

- SQLite: `data/app/ranking_service.db`
- Daily market SQLite: `data/market/market_daily.sqlite3`

当前已经引入：

- `SQLAlchemy 2.x`
- `Alembic`

现状是：

- ORM / migration 基础设施已经就位
- 主要 store 正在逐步迁到 ORM
- 已有回归测试保护迁移过程

相关文档：

- [docs/orm-migration.md](docs/orm-migration.md)
- [docs/improvement-roadmap.md](docs/improvement-roadmap.md)

## 安装

```bash
cd /home/song/projects/ib-qlib-pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

复制配置：

```bash
cp examples/config.example.yaml config.yaml
```

## 配置

主要配置来源：

- `config.yaml`
- `config_union.yaml`
- 环境变量

常见的实验线配置还包括：

- `config_u16.yaml`
- `config_u16_catboost_1d.yaml`

原则上：

- 原始日线和 `market_daily.sqlite3` 共享复用
- `qlib_csv / qlib_bin` 按 universe 或实验线隔离

重点环境变量：

- `RANKING_API_DB_PATH`
- `RANKING_API_TIMEZONE`
- `RANKING_API_HOST`
- `RANKING_API_PORT`
- `QLIB_REPO_PATH`
- `QLIB_PYTHON_BIN`
- `QLIB_QRUN_BIN`
- `PROJECT_DATA_DIR`
- `RUN_WORKSPACE_DIR`
- `MLRUNS_DIR`

### WSL / Windows 注意事项

如果后端运行在 WSL，而 TWS / IB Gateway 在 Windows：

- `127.0.0.1` 不一定能从 WSL 直连 Windows
- 需要把 `host` 配成 Windows 主机可达地址
- 同时确认 TWS / IB Gateway API 权限和端口正确

如果要让局域网其它机器访问前端：

- 前端需监听 `0.0.0.0`
- Windows 防火墙需放行前后端端口
- 在 WSL 环境中通常还需要 `portproxy`

## 启动后端

开发方式：

```bash
cd /home/song/projects/ib-qlib-pipeline
source .venv/bin/activate
python run_backend.py
```

脚本方式：

```bash
cd /home/song/projects/ib-qlib-pipeline
RANKING_API_PORT=8001 ./start_backend.sh
./stop_backend.sh
```

常用地址：

- API: `http://127.0.0.1:8001`
- Docs: `http://127.0.0.1:8001/docs`

## 启动前端

```bash
cd /home/song/projects/ib-qlib-pipeline
FRONTEND_BACKEND_PORT=8001 ./start_frontend.sh
./stop_frontend.sh
```

前端默认：

- 监听 `0.0.0.0:9991`
- 将 `/api` 代理到后端

主要页面：

- `/dashboard`
- `/portfolios`
- `/rankings`
- `/symbols/:symbol`
- `/compare`
- `/operations`

当前前端已经把页面上下文同步到 URL。常见 query params：

- `u` = universe id
- `p` = portfolio run id
- `r` = ranking run id
- `symbols` = compare 页面 symbol 列表

## 推荐日常操作

### 手动跑当天流程

1. 启动前后端
2. 打开 `/operations`
3. 点击 `Daily Close Pipeline`

建议参数：

- `trade_date`: 当天交易日
- `client_id`: 你的 IB client id
- `start_date`: 最近几天的刷新起点
- `include_portfolio`: 勾选

### 配置自动定时

在 `/operations` 的 `Schedules` 中创建：

- 类型：`daily_close_pipeline`
- 时区：你的本地时区
- 时间：收盘后合适时间
- `include portfolio append`: 开启

## CLI 入口

以下脚本仍然可直接使用：

- `python run.py`
- `python backfill_rankings.py`
- `python backfill_rankings_bulk.py`
- `python simulate_portfolio.py`
- `./run_daily_ranking.sh`

但当前推荐顺序是：

- 日常运行优先用 `/operations`
- CLI 仅用于初始化 / 排错 / 特殊实验线 backfill

更底层的 one-click ranking 说明见：

- [README_DAILY_RANKING.md](README_DAILY_RANKING.md)

## 前端

前端是 Angular 项目，说明见：

- [frontend/README.md](frontend/README.md)

## 测试

当前已经建立一组 `unittest` 回归测试，重点覆盖：

- ORM store
- service 层关键业务逻辑
- 迁移过程中的行为不回归

运行：

```bash
cd /home/song/projects/ib-qlib-pipeline
source .venv/bin/activate
python -m unittest tests.test_webapi_stores -v
```

当前开发流程已经固定为：

1. 先补测试
2. 旧代码通过
3. 再迁移或改功能
4. 再跑同一组测试确认不回归

## 当前状态

当前代码已经：

- 接入 SQLAlchemy + Alembic
- 将主要 store 分阶段迁到 ORM
- 去掉了 `service.py` 中残留的直接 SQL 访问
- 建立了回归测试护栏

后续改进路线见：

- [docs/improvement-roadmap.md](docs/improvement-roadmap.md)
