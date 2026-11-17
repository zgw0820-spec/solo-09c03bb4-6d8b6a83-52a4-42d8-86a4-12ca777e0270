"""Typed application errors mapped to HTTP responses in main.py."""
from __future__ import annotations


class QueryServiceError(Exception):
    """Base class for errors that carry a stable machine-readable code."""

    code = "query_service_error"
    http_status = 500

    def __init__(self, message: str = "", extra: dict | None = None) -> None:
        super().__init__(message)
        self.extra = extra or {}


class InvalidSqlError(QueryServiceError):
    code = "invalid_sql"
    http_status = 400


class InvalidDeadlineError(QueryServiceError):
    code = "invalid_deadline"
    http_status = 422


class PoolAcquireTimeout(QueryServiceError):
    """Raised when no connection became available within the total deadline."""

    code = "acquire_timeout"
    http_status = 503


class QueryTimeout(QueryServiceError):
    """Raised when the statement did not finish before the total deadline."""

    code = "query_timeout"
    http_status = 504


class UnknownTargetError(QueryServiceError):
    """Raised when the switchover request names an unconfigured candidate."""

    code = "unknown_target"
    http_status = 400


class SwitchTimeout(QueryServiceError):
    """Raised when the switchover could not finish within its total deadline.

    The active target is left untouched: the original database keeps
    serving queries.
    """

    code = "switch_timeout"
    http_status = 504


class SwitchValidationFailed(QueryServiceError):
    """Raised when the candidate fails the read-only fault_logs probe.

    The active target is left untouched: the original database keeps
    serving queries.
    """

    code = "switch_validation_failed"
    http_status = 502
