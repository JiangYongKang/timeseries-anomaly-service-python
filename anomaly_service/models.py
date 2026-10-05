"""核心数据模型（不可变值对象）。"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class FieldType(str, Enum):
    FLOAT = "float"
    INT = "int"
    STRING = "string"
    BOOL = "bool"


@dataclass(frozen=True)
class FieldSpec:
    name: str
    type: FieldType
    nullable: bool = False


@dataclass(frozen=True)
class DataPoint:
    ts: int  # 纳秒级 epoch 时间戳
    values: dict[str, object]


@dataclass(frozen=True)
class Schema:
    fields: tuple[FieldSpec, ...]

    def field_names(self) -> tuple[str, ...]:
        return tuple(f.name for f in self.fields)

    def get(self, name: str) -> FieldSpec | None:
        for f in self.fields:
            if f.name == name:
                return f
        return None


@dataclass(frozen=True)
class DatasetVersion:
    dataset: str
    version: int
    schema_: Schema
    points: tuple[DataPoint, ...]
    parent_version: int | None = None
    batch_id: str | None = None


@dataclass(frozen=True)
class ImportResult:
    dataset: str
    version: int
    appended: bool
    point_count: int
    batch_id: str | None
    replayed: bool  # True 表示命中幂等重放，未产生新副作用


class AnomalyStatus(str, Enum):
    NORMAL = "normal"
    ANOMALY = "anomaly"
    INSUFFICIENT_DATA = "insufficient_data"
    NO_DATA = "no_data"


@dataclass(frozen=True)
class AnomalyEvidence:
    metric: str
    window_value: float | None
    baseline_mean: float | None
    baseline_std: float | None
    zscore: float | None
    threshold: float
    reason: str


@dataclass(frozen=True)
class WindowResult:
    window_start: int
    window_end: int
    count: int
    aggregate: float | None
    status: AnomalyStatus
    evidence: AnomalyEvidence | None = None


@dataclass(frozen=True)
class QueryResult:
    dataset: str
    version: int
    metric_field: str
    window_seconds: int
    start_ts: int
    end_ts: int
    windows: tuple[WindowResult, ...]


#: 数据点缺少分组维度值时归入的显式分组键（不悄悄丢弃）。
MISSING_GROUP = "__missing__"


@dataclass(frozen=True)
class GroupResult:
    group_value: object  # 分组维度值；缺失时为 MISSING_GROUP
    windows: tuple[WindowResult, ...]


@dataclass(frozen=True)
class GroupedQueryResult:
    dataset: str
    version: int
    metric_field: str
    group_field: str
    window_seconds: int
    start_ts: int
    end_ts: int
    groups: tuple[GroupResult, ...]
