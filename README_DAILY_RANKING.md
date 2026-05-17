# Daily Ranking CLI Notes

这个文档只说明底层 CLI ranking 链路。  
当前项目的日常主入口已经是：

- Web `/operations`
- `Daily Close Pipeline`

如果你只是想每天收盘后跑全流程，请优先使用 Web 页面或 schedule。  
本文件适用于：

- 排错
- 单独验证 ranking 生成链路
- 手工运行低层脚本

## 相关入口

- shell wrapper: `run_daily_ranking.sh`
- CLI entrypoint: `oneclick_daily_ranking.py`
- orchestration: `ib_qlib_pipeline/runner/daily_ranking_runner.py`

## 当前行为

`run_daily_ranking.sh` / `oneclick_daily_ranking.py` 会：

1. 运行 `run.py` 拉最新增量数据
2. 更新 Qlib bin
3. 生成 runtime workflow yaml
4. 调用 `qrun`
5. 仅定位“本次新 experiment / 新 recorder”的 `pred.pkl`
6. 读取最新 `signal_date` 的横截面预测
7. 生成 ranking CSV / HTML
8. 写结构化 manifest JSON 到 `reports/manifests/`

注意：

- 这个 one-click 链路本身**不负责把结果写进 Web 业务数据库**
- 正式入库和 portfolio 追加应优先通过：
  - `backfill_rankings.py`
  - `simulate_portfolio.py`
  - 或 `/operations`

## 最小运行方式

```bash
cd /home/song/projects/ib-qlib-pipeline
./run_daily_ranking.sh
```

常用可选参数：

```bash
./run_daily_ranking.sh --client-id 151 --lookback-days 7 --workflow-base examples/workflow_us_lgb_2020_port.yaml
```

## 核心输出

- runtime workflow:
  - `reports/tmp/workflow_runtime_YYYY-MM-DD.yaml`
- ranking csv:
  - `reports/rankings/sp500_ranking_YYYY-MM-DD.csv`
- html report:
  - `reports/rankings/*.html`
- manifest:
  - `reports/manifests/daily_ranking_*.json`

## 预测 artifact 定位

当前已经不再通过全局 `mtime` 猜最近的 `pred.pkl`。

现在流程是：

1. 运行前写入唯一 `experiment_name`
2. 运行前记录该 experiment 下已有 recorder
3. `qrun` 完成后只接受“本次新增 recorder”
4. 从该 recorder 读取：

```text
mlruns/<experiment_id>/<recorder_id>/artifacts/pred.pkl
```

这比旧的“扫整个 `mlruns` 取最新文件”稳定很多。

## 运行时路径配置

优先级：

1. 环境变量
2. `config.yaml`

支持的关键变量：

- `QLIB_REPO_PATH`
- `QLIB_PYTHON_BIN`
- `QLIB_QRUN_BIN`
- `PROJECT_DATA_DIR`
- `RUN_WORKSPACE_DIR`
- `MLRUNS_DIR`

## 适用边界

适合：

- 快速检查今日 ranking 能否成功跑通
- 验证 workflow / qrun / pred artifact 链路
- 调试 ranking 生成

不适合作为：

- 每日正式生产主流程
- 历史 backfill 主入口
- portfolio 生命周期更新主入口

这些场景应优先使用：

- `Daily Close Pipeline`
- `/operations`
- `backfill_rankings.py`
- `simulate_portfolio.py`
