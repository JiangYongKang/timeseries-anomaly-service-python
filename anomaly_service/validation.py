"""导入数据的校验：字段缺失、类型、时间戳、schema 演进。

时间戳语义：
- 整数：自 Unix epoch 起的纳秒；
- 字符串：ISO-8601（如 ``2024-01-01T00:00:00Z``），统一转换为纳秒。
"""

from __future__ import annotations

from datetime import UTC, datetime

from .errors import (
    DuplicatePointError,
    InvalidTimestampError,
    MissingFieldError,
    ResourceLimitError,
    SchemaEvolutionError,
    TypeMismatchError,
    ValidationError,
)
from .models import DataPoint, FieldSpec, FieldType, Schema

_NS = 1_000_000_000


class DataValidator:
    def __init__(self, max_points: int = 100_000) -> None:
        self.max_points = max_points

    # -- 时间戳 -----------------------------------------------------------

    @staticmethod
    def parse_timestamp(value: object) -> int:
        if isinstance(value, bool) or value is None:
            raise InvalidTimestampError(f"非法时间戳: {value!r}")
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            text = value.strip()
            if not text:
                raise InvalidTimestampError("时间戳为空字符串")
            if text.endswith(("Z", "z")):
                text = text[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(text)
            except ValueError as exc:
                raise InvalidTimestampError(f"无法解析时间戳: {value!r}") from exc
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.timestamp() * _NS  # type: ignore[return-value]
        raise InvalidTimestampError(f"不支持的时间戳类型: {type(value).__name__}")

    # -- 值类型 -----------------------------------------------------------

    @staticmethod
    def coerce_value(value: object, spec: FieldSpec) -> object:
        if value is None:
            if spec.nullable:
                return None
            raise MissingFieldError(f"字段 {spec.name!r} 值为 null 且不可为空")
        t = spec.type
        if t is FieldType.FLOAT:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise TypeMismatchError(
                    f"字段 {spec.name!r} 期望 float，实际 {type(value).__name__}"
                )
            return float(value)
        if t is FieldType.INT:
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeMismatchError(
                    f"字段 {spec.name!r} 期望 int，实际 {type(value).__name__}"
                )
            return value
        if t is FieldType.STRING:
            if not isinstance(value, str):
                raise TypeMismatchError(
                    f"字段 {spec.name!r} 期望 string，实际 {type(value).__name__}"
                )
            return value
        if t is FieldType.BOOL:
            if not isinstance(value, bool):
                raise TypeMismatchError(
                    f"字段 {spec.name!r} 期望 bool，实际 {type(value).__name__}"
                )
            return value
        raise TypeMismatchError(f"未知字段类型: {t}")

    # -- 批次校验 ---------------------------------------------------------

    def validate_points(
        self,
        raw_points: list[dict[str, object]],
        schema: Schema | None,
    ) -> tuple[list[DataPoint], Schema]:
        if not isinstance(raw_points, list):
            raise ValidationError("points 必须是数组")
        if len(raw_points) > self.max_points:
            raise ResourceLimitError(
                f"批次数据点 {len(raw_points)} 超过上限 {self.max_points}"
            )
        if not raw_points:
            raise ValidationError("空批次不允许导入", code="empty_batch")

        parsed: list[tuple[int, dict[str, object]]] = []
        for idx, raw in enumerate(raw_points):
            if not isinstance(raw, dict):
                raise TypeMismatchError(f"第 {idx} 个数据点不是对象")
            if "ts" not in raw:
                raise MissingFieldError(f"第 {idx} 个数据点缺少 ts 字段")
            ts = self.parse_timestamp(raw["ts"])
            parsed.append((ts, {k: v for k, v in raw.items() if k != "ts"}))

        if schema is None:
            schema = self._infer_schema(parsed)

        expected = set(schema.field_names())
        points: list[DataPoint] = []
        seen_ts: set[int] = set()
        for idx, (ts, values) in enumerate(parsed):
            keys = set(values)
            missing = expected - keys
            if missing:
                raise MissingFieldError(
                    f"第 {idx} 个数据点缺少字段: {sorted(missing)}"
                )
            extra = keys - expected
            if extra:
                raise ValidationError(
                    f"第 {idx} 个数据点存在未知字段: {sorted(extra)}",
                    code="unknown_field",
                )
            coerced: dict[str, object] = {}
            for spec in schema.fields:
                coerced[spec.name] = self.coerce_value(values[spec.name], spec)
            if ts in seen_ts:
                raise DuplicatePointError(f"批次内存在重复时间点: {ts}")
            seen_ts.add(ts)
            points.append(DataPoint(ts=ts, values=coerced))
        return points, schema

    @staticmethod
    def _infer_schema(
        parsed: list[tuple[int, dict[str, object]]],
    ) -> Schema:
        names = list(parsed[0][1].keys())
        specs: list[FieldSpec] = []
        for name in names:
            types: set[FieldType] = set()
            nullable = False
            for _, values in parsed:
                if name not in values:
                    raise MissingFieldError(f"数据点缺少字段: {name!r}")
                v = values[name]
                if isinstance(v, bool):
                    types.add(FieldType.BOOL)
                elif isinstance(v, int):
                    types.add(FieldType.INT)
                elif isinstance(v, float):
                    types.add(FieldType.FLOAT)
                elif isinstance(v, str):
                    types.add(FieldType.STRING)
                elif v is None:
                    nullable = True
                    continue
                else:
                    raise TypeMismatchError(
                        f"字段 {name!r} 含不支持的类型 {type(v).__name__}"
                    )
            if types == {FieldType.INT, FieldType.FLOAT}:
                ft = FieldType.FLOAT
            elif len(types) == 1:
                ft = next(iter(types))
            elif not types:
                if not nullable:
                    raise MissingFieldError(f"字段 {name!r} 无有效值，无法推断 schema")
                raise MissingFieldError(
                    f"字段 {name!r} 全部为 null，无法推断类型"
                )
            else:
                raise TypeMismatchError(
                    f"字段 {name!r} 在批次内类型不一致: {sorted(t.value for t in types)}"
                )
            specs.append(FieldSpec(name=name, type=ft, nullable=nullable))
        return Schema(tuple(specs))

    # -- schema 演进 ------------------------------------------------------

    def check_compatible_append(self, old: Schema, new: Schema) -> Schema:
        """返回合并后的演进 schema；不兼容则抛 SchemaEvolutionError。"""
        merged: list[FieldSpec] = list(old.fields)
        for old_spec in old.fields:
            new_spec = new.get(old_spec.name)
            if new_spec is None:
                raise SchemaEvolutionError(
                    f"追加数据缺少既有字段 {old_spec.name!r}，schema 演进不兼容"
                )
            if new_spec.type is not old_spec.type and not (
                old_spec.type is FieldType.INT and new_spec.type is FieldType.FLOAT
            ):
                raise SchemaEvolutionError(
                    f"字段 {old_spec.name!r} 类型不可由 {old_spec.type.value} "
                    f"演进为 {new_spec.type.value}"
                )
        # 追加批次中新增的字段视为可空列
        old_names = {f.name for f in old.fields}
        for spec in new.fields:
            if spec.name not in old_names:
                merged.append(FieldSpec(spec.name, spec.type, nullable=True))
        return Schema(tuple(merged))
