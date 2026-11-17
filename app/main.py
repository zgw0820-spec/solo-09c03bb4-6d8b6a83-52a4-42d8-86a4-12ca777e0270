"""FastAPI application: a two-connection read-only fault-log query API.

Run locally::

    uvicorn app.main:app --host 0.0.0.0 --port 8000

Swagger UI is served at ``/docs``, ReDoc at ``/redoc``.
"""
from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .config import settings
from .errors import InvalidDeadlineError, QueryServiceError, UnknownTargetError
from .executor import QueryExecutor
from .pool import QueryConnectionPool
from .schemas import (
    HealthResponse,
    QueryRequest,
    QueryResponse,
    SwitchRequest,
    SwitchResponse,
)
from .validation import validate_single_select

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("api")

pool = QueryConnectionPool(settings)
executor = QueryExecutor(pool, settings)


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Eagerly open both connections at startup; close them on shutdown.
    await pool.open()
    try:
        yield
    finally:
        await pool.close()


app = FastAPI(
    title="机房设备故障日志只读查询 API",
    description=(
        "值班分析员共用的只读 SQL 查询入口。固定两条 PostgreSQL 连接，"
        "提交单条 SELECT 与总截止时间；排队时间计入预算，超时立即取消语句，"
        "确认事务结束才复用连接，无法确认则弃置并补建。"
        "主库检修时可通过 /switchover 把入口切到预置的备用只读库："
        "候选库须通过 fault_logs 只读验证，验证失败或期限耗尽保持原库接单；"
        "切换立即生效，在途旧库查询按原期限完成，排队及新请求读新库。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


@app.exception_handler(QueryServiceError)
async def _service_error_handler(_: Request, exc: QueryServiceError):
    return JSONResponse(
        status_code=exc.http_status,
        content={
            "error": {"code": exc.code, "message": str(exc)},
            **exc.extra,
        },
    )


@app.exception_handler(Exception)
async def _unexpected_handler(_: Request, exc: Exception):
    logger.exception("unhandled error")
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "internal_error",
                "message": f"{type(exc).__name__}: {exc}",
            }
        },
    )


@app.get("/health", response_model=HealthResponse, tags=["运维"])
async def health() -> HealthResponse:
    """连接池健康状态：容量、空闲/占用数、累计创建与弃置数、当前数据源。"""
    status = await pool.status()
    return HealthResponse(
        status="healthy",
        pool={
            "size": status.size,
            "idle": status.idle,
            "in_use": status.in_use,
            "total_created": status.total_created,
            "total_discarded": status.total_discarded,
            "closed": status.closed,
            "target": status.target,
            "generation": status.generation,
        },
    )


@app.post(
    "/query",
    response_model=QueryResponse,
    tags=["查询"],
    responses={
        400: {"description": "SQL 不符合单条只读 SELECT 约束"},
        422: {"description": "请求体或截止时间不合法"},
        503: {"description": "排队超时 / 连接被弃置"},
        504: {"description": "语句执行超过总截止时间"},
    },
)
async def query(body: QueryRequest) -> QueryResponse:
    """执行一条只读 SELECT，``timeout_ms`` 是包含排队时间的总预算。"""
    if body.timeout_ms <= 0:
        raise InvalidDeadlineError("timeout_ms 必须为正整数")

    sql = validate_single_select(body.sql, settings.max_sql_chars)

    # Absolute monotonic deadline: waiting + execution share one budget.
    deadline = time.monotonic() + body.timeout_ms / 1000.0
    # A deadline that expires while queued surfaces as a 503
    # acquire_timeout; an execution deadline as a 504 query_timeout.
    result = await executor.execute(sql, deadline)
    return QueryResponse(**result)


@app.post(
    "/switchover",
    response_model=SwitchResponse,
    tags=["运维"],
    responses={
        400: {"description": "候选库名未在 Compose 预置清单中"},
        422: {"description": "请求体或切换期限不合法"},
        502: {"description": "候选库只读验证失败，保持原库接单"},
        504: {"description": "切换总期限耗尽，保持原库接单"},
    },
)
async def switchover(body: SwitchRequest) -> SwitchResponse:
    """把共享查询入口切到预置候选库（如备用只读库 standby）。

    候选库必须在 ``timeout_ms`` 总期限内通过 ``fault_logs`` 只读验证；
    验证失败或期限耗尽时保持原库接单并返回对应错误回执。验证通过后
    立即生效：在途旧库查询按各自原截止时间完成，尚未开始执行的排队
    请求及新请求读新库，旧连接用完退出。验证探针占用一个连接名额，
    因此验证与新旧查询交错时两库物理连接合计仍不超过池容量。
    """
    if body.timeout_ms <= 0:
        raise InvalidDeadlineError("timeout_ms 必须为正整数")

    dsn = settings.candidates.get(body.target)
    if dsn is None:
        raise UnknownTargetError(
            f"未知候选库 {body.target!r}，可选："
            f"{', '.join(sorted(settings.candidates))}"
        )

    started = time.monotonic()
    deadline = started + body.timeout_ms / 1000.0
    result = await pool.switch_target(body.target, dsn, deadline)
    return SwitchResponse(
        status="switched" if result.changed else "unchanged",
        target=result.target,
        previous=result.previous,
        generation=result.generation,
        fault_logs_count=result.fault_logs_count,
        elapsed_ms=round((time.monotonic() - started) * 1000, 3),
    )
