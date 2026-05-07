# ORM and Migration Plan

当前项目已引入 `SQLAlchemy 2.x + Alembic` 的基础骨架，用于接管后续 schema 演进。

## 已完成

- 增加 ORM 模型定义：
  - `ib_qlib_pipeline/dborm/`
- 增加数据库 URL / engine / session 工具：
  - `ib_qlib_pipeline/dborm/session.py`
- 初始化 Alembic：
  - `alembic.ini`
  - `alembic/env.py`
  - `alembic/versions/0001_initial_schema.py`
- 在开始改动前已备份当前 SQLite：
  - `data/app/backups/ranking_service_pre_orm_*.db`

## 当前策略

这一阶段只做两件事：

1. 用 ORM 表达当前 schema
2. 用 Alembic 接管未来 migration

刻意 **不** 在这一阶段重写现有所有 `sqlite3` 访问路径，以降低风险。

## 数据库 URL

优先顺序：

1. `RANKING_API_DB_URL`
2. `RANKING_API_DB_PATH`
3. 默认 `data/app/ranking_service.db`

如果不显式设置，Alembic 会自动落到当前 SQLite。

## 建议命令

安装依赖：

```bash
cd /home/song/projects/ib-qlib-pipeline
source .venv/bin/activate
pip install -r requirements.txt
```

查看当前 migration 版本：

```bash
alembic current
```

升级到最新：

```bash
alembic upgrade head
```

生成新 migration：

```bash
alembic revision --autogenerate -m "describe change"
```

## 下一步建议

- 先让 `alembic upgrade head` 成为部署和启动前的标准步骤
- 之后逐步把：
  - `model_store.py`
  - `portfolio_store.py`
  - `run_store.py`
  - 部分 `service.py` 查询
  迁移到 SQLAlchemy Session

## 注意

- 当前 `0001_initial_schema` 是“以现有 schema 为基线”的初始版本
- 如果生产库已经存在，不要先手工删除数据库
- 在任何较大 migration 前，继续保留 SQLite 备份习惯
