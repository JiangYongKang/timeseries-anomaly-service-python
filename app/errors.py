"""错误分类：所有可预期失败均使用可区分的错误码。"""
from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    """失败原因分类，错误码字符串稳定、可被测试与客户端断言。"""

    SCHEMA_FIELD_MISSING = "SCHEMA_FIELD_MISSING"
    SCHEMA_TYPE_MISMATCH = "SCHEMA_TYPE_MISMATCH"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    DUPLICATE_TIMESTAMP = "DUPLICATE_TIMESTAMP"
    EMPTY_BATCH = "EMPTY_BATCH"
    INVALID_PARAMETER = "INVALID_PARAMETER"
    VERSION_NOT_FOUND = "VERSION_NOT_FOUND"
    NO_DATA_IN_RANGE = "NO_DATA_IN_RANGE"
    VERSION_CONFLICT = "VERSION_CONFLICT"
    RESOURCE_LIMIT_EXCEEDED = "RESOURCE_LIMIT_EXCEEDED"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class ServiceError(Exception):
    """业务错误基类，携带稳定错误码与可读消息。"""

    error_code: ErrorCode = ErrorCode.INTERNAL_ERROR

    def __init__(self, message: str = "", *, details: dict | None = None) -> None:
        super().__init__(message or self.error_code.value)
        self.message = message or self.error_code.value
        self.details = details or {}


class ValidationError(ServiceError):
    """数据校验失败基类。"""


class FieldMissingError(ValidationError):
    error_code = ErrorCode.SCHEMA_FIELD_MISSING


class TypeMismatchError(ValidationError):
    error_code = ErrorCode.SCHEMA_TYPE_MISMATCH


class InvalidTimestampError(ValidationError):
    error_code = ErrorCode.INVALID_TIMESTAMP


class DuplicateTimestampError(ValidationError):
    error_code = ErrorCode.DUPLICATE_TIMESTAMP


class EmptyBatchError(ValidationError):
    error_code = ErrorCode.EMPTY_BATCH


class InvalidParameterError(ServiceError):
    error_code = ErrorCode.INVALID_PARAMETER


class VersionNotFoundError(ServiceError):
    error_code = ErrorCode.VERSION_NOT_FOUND


class NoDataInRangeError(ServiceError):
    error_code = ErrorCode.NO_DATA_IN_RANGE


class VersionConflictError(ServiceError):
    error_code = ErrorCode.VERSION_CONFLICT


class ResourceLimitExceededError(ServiceError):
    error_code = ErrorCode.RESOURCE_LIMIT_EXCEEDED


class TimeoutError_(ServiceError):
    error_code = ErrorCode.TIMEOUT


class CancelledError_(ServiceError):
    error_code = ErrorCode.CANCELLED
