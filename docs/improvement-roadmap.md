# Improvement Roadmap

这个文档把当前 `ib-qlib-pipeline` 的主要改进建议整理成后续执行清单。目标不是一次性大重构，而是按优先级逐步降低系统风险、提高可维护性，并保持现有业务流程可继续使用。

## 当前判断

项目已经从“脚本实验”演进成了一个可长期运行的小型系统，主线能力已经具备：

- IB 数据抓取与增量更新
- Qlib workflow 训练 / 验证 / 预测
- ranking CSV / HTML 产物
- SQLite 历史索引
- portfolio lot / mark 回放
- FastAPI 后端
- Angular 前端
- `/operations` 任务中心
- `Daily Close Pipeline`

下一步重点不应再是堆功能，而应转向：

- 明确边界
- 收敛状态管理
- 降低模型 / workflow 污染风险
- 让日常运行更可预测

## Phase 1: Stability

### 1. 建正式 migration 机制

当前 `ib_qlib_pipeline/webapi/db.py` 里的 `_migrate_schema()` 能工作，但已经开始承载较多历史补丁。后续继续扩表会越来越难追踪。

建议：

- 增加 `schema_version` 表
- 增加 `migrations/` 目录
- 每次 schema 变化用独立 migration 文件表示
- 启动时按顺序执行

目标：

- 让数据库演进可审计
- 让新机器和旧库都能稳定升级

### 2. 规范模型元数据

现在已经出现两套模型族：

- `*_Default`
- `*_5D`

继续发展下去，还会出现更多 workflow / label / version。

建议：

- 在 `models.details_json` 中显式保存：
  - `family`
  - `target`
  - `workflow_variant`
  - `version`
- UI 中不要只显示 `name`
- 后续所有比较逻辑都依赖这套元数据，而不是依赖命名约定

目标：

- 避免不同 workflow 混进同一模型族
- 让 UI / DB / backfill / portfolio 比较逻辑可长期扩展

### 3. 固化 workflow 目录规范

`examples/` 里已经同时存在：

- 默认 workflow
- 历史修订 workflow
- 实验 workflow

建议：

- 拆分目录，例如：
  - `examples/workflows/production/`
  - `examples/workflows/experimental/`
- 对正式使用的 workflow 加版本说明
- 避免长期把临时实验文件混在主目录

目标：

- 降低误用错误 workflow 的概率
- 让 pipeline 和 schedule 更容易绑定“正式模型”

## Phase 2: Service Structure

### 4. 继续拆 `webapi/service.py`

`ib_qlib_pipeline/webapi/service.py` 现在已经同时负责：

- jobs
- schedules
- ranking run orchestration
- daily close pipeline
- summary 聚合
- DB 写入协调

它已经成为新的“巨型服务文件”。

建议拆成：

- `jobs_service.py`
- `schedule_service.py`
- `operations_service.py`
- `ranking_run_service.py`

目标：

- 缩小单文件职责范围
- 降低以后改动互相影响的风险

### 5. 把核心业务脚本进一步模块化

当前入口仍然较多：

- `run.py`
- `backfill_rankings.py`
- `simulate_portfolio.py`
- `oneclick_daily_ranking.py`

虽然现在已经有模块拆分，但还可以继续统一到清晰的 service 层，例如：

- `MarketDataService`
- `RankingService`
- `PortfolioSimulationService`

然后：

- CLI 脚本调用 service
- Web API 调用同一套 service

目标：

- 减少“脚本路径”和“服务逻辑”的重复
- 让网页逐步成为主入口时，不需要复制业务逻辑

### 6. 强化 job 执行边界

当前 job 系统已经很好用，但本质上仍是：

- 单进程
- 线程 + subprocess

建议中期改进：

- 把 job 执行器边界抽清楚
- 明确 job / step 状态模型
- 为失败步骤预留独立重试能力

目标：

- 让任务中心从“能用”升级到“可运营”

## Phase 3: Data & Operations

### 7. 明确多层状态职责

当前系统状态分散在：

- 原始价格文件
- 处理后 Qlib CSV
- Qlib bin
- `mlruns`
- SQLite
- HTML / CSV 产物

建议：

- 明确每层职责
- 尽量避免一个目录同时承担“系统状态”和“展示产物”两种角色

例如中期可以考虑：

- `artifacts/rankings/` 存系统产物
- `reports/rankings/` 存面向人工查看的导出结果

目标：

- 降低状态不一致时的排查成本
- 让补跑与清理策略更清晰

### 8. 继续强化 Daily Close Pipeline

现在 `Daily Close Pipeline` 已经是主流程，方向是正确的。

后续建议：

- 让系统能更明确表达“今天哪一步已完成，哪一步缺失”
- 把 `refresh -> backfill -> append` 的状态视为真正的流程状态，而不是只看 job 是否结束
- 对缺失日期的自动补跑继续完善

目标：

- 真正做到“服务不一定每天开着，也能补齐缺口”
- 把日常运行从“手工理解流程”变成“系统自己知道流程状态”

### 9. 规范局域网 / WSL 访问部署文档

当前使用环境依赖：

- WSL
- Windows 防火墙
- `portproxy`
- 本地前后端端口

建议补单独运维文档，至少覆盖：

- 本机访问
- 局域网访问
- 后端端口
- 前端端口
- Windows 侧放行与转发

目标：

- 降低“过一段时间忘记怎么配”的运维成本

## Phase 4: Frontend & UX

### 10. 统一上下文展示

当前页面里已经有：

- model
- workflow
- portfolio run

但不同页面展示方式还不够统一。

建议：

- 在 `Daily Rankings`
- `Symbol Detail`
- `Compare`
- `Operations`

统一显示当前上下文条：

- 当前 model
- 当前 workflow
- 当前 portfolio run

目标：

- 降低用户迷失感
- 减少“当前看到的数据到底属于哪个模型”的疑问

### 11. 优化 Operations 页面层次

`/operations` 已经成为主入口，但内容较多：

- 今日状态
- schedules
- 手动操作
- recent jobs
- job detail

建议后续继续分层：

- 今日状态单独更突出
- schedule 与手动触发分块更清晰
- job detail 更像日志面板

目标：

- 让网页更像操作台，而不是调试台

### 12. Compare / Symbol Detail 继续产品化

这两块已经很有价值，后续可继续增强：

- 保存比较视图
- 更清晰的时间线
- 更明确的 entry / exit / hold 标注
- 更直观的策略差异解释

目标：

- 把 UI 从“能查数据”提升到“能帮助做决策”

## 推荐执行顺序

如果只按最务实的顺序推进，建议：

1. migration 机制
2. 模型元数据规范
3. workflow 目录与正式/实验分离
4. 拆 `webapi/service.py`
5. 强化 Daily Close Pipeline 状态表达
6. 优化 `/operations` 层次
7. 再考虑更大规模的 service / job executor 重构

## 备注

当前工作方式建议继续保持：

- 网页作为主入口
- 脚本作为初始化、补跑、排错工具

在系统完全稳定前，不建议强行删除 CLI 脚本。更好的方向是：

- 逐步弱化脚本的“人工主入口”角色
- 保留它们作为后端和运维的底层工具
