# ORM and Migration Plan

当前项目已经统一为：

- `Alembic` 负责所有正式 schema migration
- `init_db()` 只负责空库初始化和测试辅助

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

当前职责边界是：

1. `init_db()`
   - 创建全新空 SQLite 数据库
   - 供测试快速建表
   - 不再负责旧库升级

2. `Alembic`
   - 负责所有已有数据库升级
   - 负责新增字段 / 新表 / 新索引 / 新约束
   - 例如 universe、strategy、job worker、`job_log_lines` 这类 schema 变化

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

升级已有数据库到最新：

```bash
alembic upgrade head
```

生成新 migration：

```bash
alembic revision --autogenerate -m "describe change"
```

## 使用规则

- 新建本地开发库：
  - 可以通过 app/test 里的 `init_db()` 创建
- 升级已有数据库：
  - 必须运行 `alembic upgrade head`
- 不要继续在 `webapi/db.py` 中添加手写 `ALTER TABLE`
- 不要手工修改 SQLite schema
- 新增 schema 变化时，必须添加新的 Alembic revision

## 注意

- 当前 revision 链会逐步补齐历史上曾经由手写 `_migrate_schema()` 管理的字段和索引
- 如果生产库已经存在，不要先手工删除数据库
- 在任何较大 migration 前，继续保留 SQLite 备份习惯
