"""可区分的错误分类。

所有业务错误都继承 :class:`AnomalyServiceError`，并通过稳定的
``code`` 字符串暴露给调用方，便于测试与客户端按原因分支处理。
"""

from __future__ import annotations


class AnomalyServiceError(Exception):
    """业务错误基类。"""

    code: str = "internal_error"

    def __init__(self, message: str = "", *, code: str | None = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code


class ValidationError(AnomalyServiceError):
    """请求参数 / 数据不合法基类。"""

    code = "validation_error"


class MissingFieldError(ValidationError):
    """数据点缺少必填字段。"""

    code = "missing_field"


class TypeMismatchError(ValidationError):
    """字段类型与数据集 schema 不匹配。"""

    code = "type_mismatch"


class InvalidTimestampError(ValidationError):
    """时间戳缺失、无法解析或非法。"""

    code = "invalid_timestamp"


class DuplicateBatchError(ValidationError):
    """批次幂等键重复但载荷与首次不同。"""

    code = "duplicate_batch_conflict"


class DuplicatePointError(ValidationError):
    """同一数据版本内出现重复时间点。"""

    code = "duplicate_timestamp"


class SchemaEvolutionError(ValidationError):
    """追加数据与既有 schema 不兼容（非法结构演进）。"""

    code = "incompatible_schema"


class DatasetNotFoundError(AnomalyServiceError):
    """数据集或版本不存在。"""

    code = "dataset_not_found"


class VersionNotFoundError(AnomalyServiceError):
    code = "version_not_found"


class FieldNotFoundError(ValidationError):
    """查询/计算引用的字段不在该版本 schema 中。"""

    code = "field_not_found"


class InvalidWindowError(ValidationError):
    """窗口参数非法（零/负长度、起止倒置等）。"""

    code = "invalid_window"


class ResourceLimitError(AnomalyServiceError):
    """超出资源上限（数据点过多等）。"""

    code = "resource_limit_exceeded"


class QueryBudgetExceededError(ResourceLimitError):
    """单次查询超出窗口/数据量预算；与参数写错（422）明确区分。"""

    code = "query_budget_exceeded"


class StaleSchemaError(ValidationError):
    """追加请求基于已过时的 schema 发出（并发演进冲突），可安全重试。"""

    code = "stale_schema"


class TimeoutError_(AnomalyServiceError):
    """操作超过截止时间。"""

    code = "timeout"


class CancelledError_(AnomalyServiceError):
    """操作被显式取消。"""

    code = "cancelled"
