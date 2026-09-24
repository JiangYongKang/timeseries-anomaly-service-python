"""批次数据校验：字段缺失、类型不匹配、非法时间、批次内重复时间点。

线上输入格式（每个数据点）::

    {"ts": <int epoch 毫秒, UTC>, "values": {"字段名": <number>, ...}}

校验是全有或全无的：任一点非法则整个批次被拒绝，存储层不会看到任何数据。
"""
from __future__ import annotations

import math

from app.errors import (
    DuplicateTimestampError,
    EmptyBatchError,
    FieldMissingError,
    InvalidTimestampError,
    TypeMismatchError,
)
from app.models import DataPoint, FieldType, Schema

_BOOL = bool
_INT = int
_FLOAT = (int, float)
# 合理的 epoch 毫秒区间：[2000-01-01, 2100-01-01)
_MIN_TS_MS = 946_684_800_000
_MAX_TS_MS = 4_102_444_800_000


def validate_schema(raw: dict) -> Schema:
    """校验并解析 schema 定义（委托给 Schema.parse，语义一致）。"""
    return Schema.parse(raw)


def validate_batch(raw_points: list[dict], schema: Schema) -> list[DataPoint]:
    """校验一个导入批次。

    - 空批次（None / 非 list / 长度 0）-> EmptyBatchError
    - 行缺少 ts/values 或某字段缺失 -> FieldMissingError
    - ts 非整数毫秒或超出合理范围   -> InvalidTimestampError
    - 字段值类型不符 / NaN / Inf     -> TypeMismatchError
    - 批次内出现重复 ts              -> DuplicateTimestampError
    所有错误 details 都带 index / field，便于定位与区分。
    """
    if not isinstance(raw_points, list) or len(raw_points) == 0:
        raise EmptyBatchError(
            "批次必须是非空数组", details={"got_type": type(raw_points).__name__}
        )

    points: list[DataPoint] = []
    seen_ts: set[int] = set()

    for index, raw in enumerate(raw_points):
        if not isinstance(raw, dict):
            raise TypeMismatchError(
                "数据点必须是对象", details={"index": index, "got": type(raw).__name__}
            )

        # 1) 时间戳
        if "ts" not in raw:
            raise FieldMissingError(
                "数据点缺少必填字段 'ts'", details={"index": index, "field": "ts"}
            )
        ts = raw["ts"]
        # bool 是 int 的子类，显式排除
        if isinstance(ts, _BOOL) or not isinstance(ts, _INT):
            raise InvalidTimestampError(
                "'ts' 必须是 epoch 毫秒整数",
                details={"index": index, "field": "ts", "got": type(ts).__name__},
            )
        if not (_MIN_TS_MS <= ts < _MAX_TS_MS):
            raise InvalidTimestampError(
                "'ts' 超出支持的时间范围 [2000-01-01, 2100-01-01)",
                details={"index": index, "field": "ts", "value": ts},
            )
        if ts in seen_ts:
            raise DuplicateTimestampError(
                "批次内存在重复时间戳",
                details={"index": index, "field": "ts", "value": ts},
            )
        seen_ts.add(ts)

        # 2) values 容器
        if "values" not in raw:
            raise FieldMissingError(
                "数据点缺少必填字段 'values'",
                details={"index": index, "field": "values"},
            )
        values_raw = raw["values"]
        if not isinstance(values_raw, dict):
            raise TypeMismatchError(
                "'values' 必须是对象",
                details={"index": index, "field": "values",
                         "got": type(values_raw).__name__},
            )

        # 3) 逐字段
        values: dict[str, float] = {}
        for fname, ftype in schema.fields.items():
            if fname not in values_raw:
                raise FieldMissingError(
                    f"数据点缺少 schema 字段 {fname!r}",
                    details={"index": index, "field": fname},
                )
            val = values_raw[fname]
            if isinstance(val, _BOOL) or not isinstance(val, _FLOAT):
                raise TypeMismatchError(
                    f"字段 {fname!r} 的值必须是数值",
                    details={"index": index, "field": fname,
                             "expected": ftype.value, "got": type(val).__name__},
                )
            fval = float(val)
            if math.isnan(fval) or math.isinf(fval):
                raise TypeMismatchError(
                    f"字段 {fname!r} 的值不能是 NaN 或 Inf",
                    details={"index": index, "field": fname},
                )
            if ftype is FieldType.INT and isinstance(val, float) and not val.is_integer():
                raise TypeMismatchError(
                    f"字段 {fname!r} 声明为 int 但收到非整数值",
                    details={"index": index, "field": fname, "value": val},
                )
            values[fname] = fval

        points.append(DataPoint(timestamp=ts, values=values))

    return points
