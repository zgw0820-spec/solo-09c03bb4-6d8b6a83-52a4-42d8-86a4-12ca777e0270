"""Request/response schemas surfaced in the OpenAPI (Swagger) document."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    sql: str = Field(
        ...,
        description=(
            "单条只读 SELECT 语句（允许 WITH/VALUES/TABLE，"
            "禁止多语句、写操作、SELECT INTO 等）"
        ),
        examples=["SELECT id, device_id, severity FROM fault_logs LIMIT 5"],
    )
    timeout_ms: int = Field(
        ...,
        ge=1,
        le=60_000,
        description=(
            "总截止时间（毫秒），从请求进入起计时，包含排队等连接的时间"
        ),
    )


class QueryResponse(BaseModel):
    status: Literal["ok", "timeout"]
    executed: bool = Field(
        description="语句是否真正发送到了数据库后端"
    )
    source: str | None = Field(
        default=None,
        description="实际数据源（当前接单的候选库名，如 primary/standby）",
    )
    wait_ms: float | None = None
    execution_ms: float | None = None
    row_count: int | None = None
    rows: list[list[Any]] | None = None
    cancelled: bool | None = None
    connection_discarded: bool | None = None
    late_result_ignored: bool | None = None
    detail: str | None = None


class SwitchRequest(BaseModel):
    target: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description=(
            "Compose 预置的候选库名（primary=主库，standby=备用只读库）"
        ),
        examples=["standby"],
    )
    timeout_ms: int = Field(
        ...,
        ge=1,
        le=60_000,
        description=(
            "切换总期限（毫秒），从请求进入起计时，"
            "包含等待连接名额与验证候选库的时间"
        ),
    )


class SwitchResponse(BaseModel):
    status: Literal["switched", "unchanged"] = Field(
        description="switched=已切换到新目标；unchanged=目标本来就是当前库"
    )
    target: str = Field(description="切换生效后接单的候选库名")
    previous: str = Field(description="切换前接单的候选库名")
    generation: int = Field(description="路由代际，每次实际切换 +1")
    fault_logs_count: int = Field(
        description="验证时在新目标上只读清点出的 fault_logs 行数"
    )
    elapsed_ms: float = Field(description="本次切换实际耗时（毫秒）")


class ErrorResponse(BaseModel):
    error: dict[str, str]


class HealthResponse(BaseModel):
    status: Literal["healthy"]
    pool: dict[str, Any]
