"""核心数据结构：数据点、Schema、版本快照、窗口聚合与异常结论。"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum

from app.errors import InvalidParameterError


class FieldType(str, Enum):
    FLOAT = "float"
    INT = "int"


@dataclass(frozen=True)
class Schema:
    """数据集结构：字段名 -> 类型。结构演进通过新版本承载。"""

    fields: dict[str, FieldType]

    @staticmethod
    def parse(raw: dict) -> Schema:
        """从 {"字段名": "float"|"int", ...} 构造 Schema。

        空 schema 或未知类型名抛 InvalidParameterError；
        数据行层面的字段缺失/类型不匹配由 validation 负责。
        """
        if not isinstance(raw, dict) or not raw:
            raise InvalidParameterError(
                "schema 必须是非空对象", details={"got": type(raw).__name__}
            )
        fields: dict[str, FieldType] = {}
        for name, type_name in raw.items():
            if not isinstance(name, str) or not name:
                raise InvalidParameterError(
                    "schema 字段名必须是非空字符串", details={"field": repr(name)}
                )
            try:
                fields[name] = FieldType(type_name)
            except ValueError:
                raise InvalidParameterError(
                    f"字段 {name!r} 的类型 {type_name!r} 不受支持",
                    details={"field": name, "type": str(type_name),
                             "supported": [t.value for t in FieldType]},
                )
        return Schema(fields=fields)


@dataclass(frozen=True)
class DataPoint:
    """单个时序点。timestamp 为 Unix  epoch 毫秒整数（UTC）。"""

    timestamp: int
    values: dict[str, float]


@dataclass(frozen=True)
class DatasetVersion:
    """不可变版本快照。

    - version_id: 版本唯一标识
    - schema: 该版本的结构
    - points: 按 timestamp 升序、时间点唯一的数据
    - parent_id: 追加场景下的父版本；首次导入为 None
    - batch_id: 导入批次幂等键，重复导入返回同一版本
    - created_at: 创建时间（epoch 秒）
    """

    version_id: str
    schema: Schema
    points: tuple[DataPoint, ...]
    parent_id: str | None
    batch_id: str
    created_at: float

    @property
    def time_range(self) -> tuple[int, int] | None:
        """返回 (最小时间戳, 最大时间戳)，无数据时为 None。"""
        if not self.points:
            return None
        return self.points[0].timestamp, self.points[-1].timestamp


class AggMethod(str, Enum):
    MEAN = "mean"
    SUM = "sum"
    MIN = "min"
    MAX = "max"


@dataclass(frozen=True)
class WindowAggregate:
    """单个时间窗口的聚合结果。count==0 表示空窗口。"""

    window_start: int
    window_end: int
    metric: str
    method: AggMethod
    value: float | None
    count: int
    missing: int


@dataclass(frozen=True)
class AnomalyEvidence:
    """异常判定依据。"""

    baseline_mean: float
    baseline_std: float
    baseline_n: int
    threshold: float
    observed: float
    zscore: float
    rule: str


@dataclass(frozen=True)
class AnomalyResult:
    """单窗口异常结论。"""

    window_start: int
    window_end: int
    metric: str
    is_anomaly: bool
    reason: str
    evidence: AnomalyEvidence | None


@dataclass(frozen=True)
class AnalysisReport:
    """一次分析的完整、可复现结论，绑定具体版本与参数。"""

    version_id: str
    metric: str
    method: AggMethod
    window: str
    start: int
    end: int
    params: dict
    aggregates: tuple[WindowAggregate, ...]
    anomalies: tuple[AnomalyResult, ...]
    computed_at: float = field(default_factory=lambda: time.time())
    report_id: str = field(default_factory=lambda: uuid.uuid4().hex)
