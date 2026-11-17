# 机房设备故障日志只读查询 API

值班分析员共用的**只读 SQL 入口**：固定两条 PostgreSQL 连接，提交单条
`SELECT` 与**总截止时间**，排队等待也计入预算；超时立即取消数据库语句，
确认事务结束才复用连接，无法确认则弃置连接并补建；迟到结果不会改写超时
回执。后续查询始终拿到属于自己的结果。

主库检修时，值班运维可通过 `POST /switchover` 把共享入口**切到 Compose
预置的备用只读库**：候选库须在切换总期限内通过 `fault_logs` 只读验证，
验证失败或期限耗尽时保持原库接单；验证通过立即生效——在途旧库查询按
各自原截止时间完成，尚未开始执行的排队请求及新请求读新库，旧连接用完
退出，成功响应以 `source` 字段标明实际数据源。

## 技术栈

- FastAPI（Swagger UI：`/docs`，ReDoc：`/redoc`）
- psycopg 3（异步连接 + libpq `PQcancel`）
- PostgreSQL 16 × 2（主库 + 备用只读库，各置一行可区分标记日志）
- Docker Compose 编排，预置 7 条故障日志与只读角色 `fault_ro`

## 快速开始（Docker Compose）

```bash
docker compose up --build
# API:    http://localhost:8000
# Swagger: http://localhost:8000/docs
# 主库:    localhost:5432   备用库: localhost:5433
```

自测（另开终端，容器内或本机均可）：

```bash
docker compose exec api python -m tests.selftest
```

> 数据卷复用时 `db/*.sql` 不会重跑；需要重新初始化请先
> `docker compose down -v`。

## 接口

### `POST /query`

```json
{
  "sql": "SELECT id, device_id, severity FROM fault_logs LIMIT 5",
  "timeout_ms": 2000
}
```

`timeout_ms` 是**包含排队时间**的总预算（单调时钟），从请求进入即计时。

| 场景 | HTTP | 回执要点 |
| --- | --- | --- |
| 成功 | 200 | `status=ok`，`rows` 为本次查询自身结果，`source` 标明实际数据源，附 `wait_ms`/`execution_ms` |
| 执行超时 | 504 | `error.code=query_timeout`，`status=timeout`，`source`、`cancelled`、`connection_discarded`、`late_result_ignored` |
| 排队期间预算耗尽 | 503 | `error.code=acquire_timeout`，语句**从未发送**到数据库 |
| SQL 不合法 | 400 | `error.code=invalid_sql` |

### `POST /switchover`

```json
{
  "target": "standby",
  "timeout_ms": 5000
}
```

把共享查询入口切到 Compose 预置的候选库（`primary` / `standby`，由
`DATABASE_DSN` / `STANDBY_DSN` 注入）。`timeout_ms` 是**切换总期限**，
包含等待连接名额与验证候选库的时间。

验证内容：在候选库上以只读会话执行 `SELECT count(*) FROM fault_logs`。
验证通过即翻转路由（`generation` +1），**无需等在途旧库查询结束**；
回执 `fault_logs_count` 为验证时清点的行数。

| 场景 | HTTP | 回执要点 |
| --- | --- | --- |
| 切换生效 | 200 | `status=switched`，`target`/`previous`/`generation`/`fault_logs_count` |
| 目标已是当前库 | 200 | `status=unchanged`（幂等，仍会做只读验证） |
| 候选库验证失败 | 502 | `error.code=switch_validation_failed`，`switched=false`，`target` 仍为原库 |
| 切换总期限耗尽 | 504 | `error.code=switch_timeout`，`switched=false`，`target` 仍为原库 |
| 候选库名未预置 | 400 | `error.code=unknown_target` |

### `GET /health`

返回连接池容量、空闲/占用数、累计创建与弃置数，以及当前接单的
`target`（数据源）与路由 `generation`。

## 关键设计

### 1. 容量恒为 2，排队计入总预算

`QueryConnectionPool` 用容量为 2 的信号量限制**物理后端数**，第三个请求
排队等待而非新开连接。`acquire(deadline)` 接收绝对单调时钟截止时间，
等待与执行共用同一预算；排队期间过期则直接返回 503，不发送任何语句。

### 2. 单条只读 SELECT 的三层防护

1. `app/validation.py` 仅允许单条 `SELECT`/`WITH`/`TABLE`/`VALUES`，拒绝
   多语句（额外分号）、写关键字、`SELECT ... INTO` 等。
2. 每个请求在显式 `BEGIN READ ONLY` 事务中执行，会话默认
   `default_transaction_read_only=on`，并设置局部 `statement_timeout` 兜底。
3. 数据库角色 `fault_ro` 仅有 `SELECT` 权限，且角色默认只读；直连也无法
   写入（`InsufficientPrivilege`）。

### 3. 超时处理协议（`app/executor.py`）

1. 语句作为 **shielded task** 运行，超时回执一旦确定就不会被迟到结果覆盖。
2. 截止时间到，调用 libpq `PQcancel`（经独立取消连接）请求服务器中止语句。
3. `await` 该 shielded 任务直至真正结束（收到 `QueryCanceled` 或拿到迟到
   结果），杜绝协程残留。
4. 执行 `ROLLBACK` 并核对 `transaction_status == IDLE`，**确认事务结束**才
   把连接放回池；无法确认则关闭弃置并补建，保持容量为 2。
5. 迟到完成的结果只做审计日志，回执固定为 504 且不携带 `rows`。

### 4. 连接级故障的弃置补建与无感重试

后端被带外终止（如 `pg_terminate_backend`）时，连接级错误触发弃置并由池
补建；只读语句在同一总预算内安全重试一次，客户端无感知。若补建暂时失败，
池临时缩水并在下一次获取时懒建补回，不会死锁。

### 5. 切换协议（`app/pool.py` 的 `switch_target`）

1. 路由是不可变对象 `Route(generation, name, dsn)`，翻转只是**一次引用
   替换**：验证通过立即生效，不等待在途旧库查询。
2. 每条连接记录打开时的代际。在途旧库查询持原连接按**原截止时间**完成；
   释放或取出时发现代际过期即关闭退役（旧连接用完退出），绝不复用旧库
   连接。排队请求拿到名额后才读路由，因此切换生效后尚未开始执行的排队
   请求与新请求都读新库。
3. 验证探针**借用一个连接名额**（与查询共用容量为 2 的信号量），在名额
   内新开一条到候选库的短连接做只读验证。因此验证与新旧查询交错时，
   **两库物理连接合计最多两条**；等待名额计入切换总期限；无论验证成功、
   失败还是超时，名额都如数归还（失败不丢连接名额）。
4. 验证失败（候选不可达、无 `fault_logs` 权限等）或期限耗尽时路由保持
   不变，原库继续接单，回执标明 `switched=false` 与当前 `target`。

## 无 Docker 的本地运行与自测

本机若无容器运行时，可用 `pgserver`（PyPI 上的免 root PostgreSQL 16）：

```bash
pip install -r requirements.txt pgserver
python tests/local_run.py        # 拉起主备两套 PG16 + uvicorn
python -m tests.selftest         # 端到端自测（含切换与交错用例）
python tests/test_protocol.py    # 超时/迟到结果/排队/切换协议直测
```

## 目录结构

```
app/                FastAPI 应用（config/pool/executor/validation/schemas/main）
db/01_init.sql      建表、预置日志、只读角色（Compose 首次初始化执行）
db/02_*_marker.sql  主/备库各自的 SOURCE_MARKER 标记行（区分两库日志）
tests/              selftest.py（HTTP 端到端）、test_protocol.py（协议直测）、
                    local_run.py（免 Docker 启动器，拉起主备双库）
Dockerfile / docker-compose.yml / requirements.txt
```
