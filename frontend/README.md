# Frontend

Angular 前端，用于查看和操作整个 `ib-qlib-pipeline` 系统。

当前主要页面：

- `/rankings`
- `/symbols/:symbol`
- `/compare`
- `/operations`

## 作用

前端当前承担的主要职责：

- 查看某天 `TOP20` ranking
- 查看单只股票的 lot / mark / 价格图
- 比较不同模型和不同 portfolio run
- 手动触发 jobs
- 查看 job 日志和今日状态
- 管理 schedule

## 本地开发

推荐从项目根目录启动，而不是直接手工 `ng serve`：

```bash
cd /home/song/projects/ib-qlib-pipeline
FRONTEND_BACKEND_PORT=8001 ./start_frontend.sh
```

停止：

```bash
./stop_frontend.sh
```

默认行为：

- 监听 `0.0.0.0:9991`
- 将 `/api` 代理到后端

## 直接使用 Angular CLI

如果你只想在 `frontend/` 目录里单独调前端，也可以：

```bash
cd /home/song/projects/ib-qlib-pipeline/frontend
npm install
npm start
```

或：

```bash
ng serve --host 0.0.0.0 --port 9991 --proxy-config proxy.conf.json
```

## 构建

```bash
cd /home/song/projects/ib-qlib-pipeline
npm --prefix frontend run build
```

## 技术栈

- Angular
- TypeScript
- `lightweight-charts`

## 当前说明

这个前端不是独立产品，它依赖：

- 后端 API
- SQLite 中的 ranking / portfolio / jobs 数据
- `/operations` 里的任务执行模型

所以前端验证通常应和后端一起进行。详见根目录：

- [../README.md](../README.md)
