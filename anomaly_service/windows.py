"""时间窗口划分与聚合。

窗口语义（稳定且与输入顺序无关）：
- 固定窗口按 Unix epoch 对齐：``window_start = floor(ts / w) * w``，
  因此无论数据点以何种乱序到达，落桶结果完全相同；
- 查询区间为半开区间 ``[start_ts, end_ts)``；只输出**完整窗口**
  （``window_start >= start_ts`` 且 ``window_end <= end_ts``），
  不完整窗口绝不参与结论；
- 缺失值（``None``）跳过；窗口内无有效数值时聚合为 ``None``，
  交由检测器判为 no-data，绝不误报；
- 时间戳以纳秒计。
"""

from __future__ import annotations

from .errors import InvalidWindowError
from .models import DataPoint

_NS_PER_SEC = 1_000_000_000
_METHODS = ("mean", "sum", "min", "max", "count")

#: 内部分桶用的缺失分组哨兵（私有对象，绝不与真实数据值冲突）；
#: 由上层转换为对外可见的 ``models.MISSING_GROUP`` 标签。
MISSING_KEY: object = object()


def align_floor(ts: int, window_ns: int) -> int:
    """floor 除法（对齐负数时间戳同样正确）。"""
    return (ts // window_ns) * window_ns


def validate_range(start_ts: int, end_ts: int, window_seconds: int) -> int:
    if not isinstance(window_seconds, int) or isinstance(window_seconds, bool):
        raise InvalidWindowError("window_seconds 必须为整数")
    if window_seconds <= 0:
        raise InvalidWindowError("window_seconds 必须为正数")
    if not isinstance(start_ts, int) or not isinstance(end_ts, int):
        raise InvalidWindowError("start_ts/end_ts 必须为纳秒整数")
    if start_ts >= end_ts:
        raise InvalidWindowError("要求 start_ts < end_ts")
    window_ns = window_seconds * _NS_PER_SEC
    if (end_ts - start_ts) < window_ns:
        raise InvalidWindowError("查询区间短于一个窗口，无法形成完整窗口")
    return window_ns


def window_windows(
    points: tuple[DataPoint, ...],
    field_name: str,
    start_ts: int,
    end_ts: int,
    window_seconds: int,
) -> list[tuple[int, int, list[float]]]:
    window_ns = validate_range(start_ts, end_ts, window_seconds)
    buckets: dict[int, list[float]] = {}
    for p in points:
        if p.ts < start_ts or p.ts >= end_ts:
            continue
        w_start = align_floor(p.ts, window_ns)
        w_end = w_start + window_ns
        # 仅保留完整落在查询区间内的窗口
        if w_start < start_ts or w_end > end_ts:
            continue
        value = p.values.get(field_name)
        if value is None:
            buckets.setdefault(w_start, [])
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            # 非数值字段无法作为指标聚合：交由上层以 field_not_found 语义拒绝
            continue
        buckets.setdefault(w_start, []).append(float(value))
    result: list[tuple[int, int, list[float]]] = []
    for w_start in sorted(buckets):
        result.append((w_start, w_start + window_ns, buckets[w_start]))
    return result


def enumerate_window_slots(
    start_ts: int,
    end_ts: int,
    window_seconds: int,
) -> list[tuple[int, int]]:
    """枚举查询区间内所有完整窗口（包括无数据窗口），结果确定且无遗漏。"""
    window_ns = validate_range(start_ts, end_ts, window_seconds)
    first = align_floor(start_ts, window_ns)
    if first < start_ts:
        first += window_ns
    slots: list[tuple[int, int]] = []
    cur = first
    while cur + window_ns <= end_ts:
        slots.append((cur, cur + window_ns))
        cur += window_ns
    return slots


def aggregate(values: list[float], method: str) -> float | None:
    if method not in _METHODS:
        raise InvalidWindowError(
            f"未知聚合方法 {method!r}，支持: {', '.join(_METHODS)}"
        )
    if not values:
        return None
    if method == "mean":
        return sum(values) / len(values)
    if method == "sum":
        return sum(values)
    if method == "min":
        return min(values)
    if method == "max":
        return max(values)
    return float(len(values))


def bucket_by_group(
    points: tuple[DataPoint, ...],
    metric_field: str,
    group_field: str | None,
    start_ts: int,
    end_ts: int,
    window_seconds: int,
) -> tuple[dict[object, dict[int, list[float]]], int]:
    """单趟扫描：按分组维度 + 完整窗口落桶。

    返回 ``(分组值 -> 窗口起点 -> 有效数值列表, 区间内参与计算的点数)``。
    - ``group_field`` 为 None 时全部点归入唯一的 ``None`` 组（等价不分组）；
    - 数据点缺少分组键或值为 ``None`` 时归入私有哨兵 ``MISSING_KEY`` 组
      （由上层映射为显式标签），不会被悄悄丢弃；
    - 点数统计只计落在 ``[start_ts, end_ts)`` 内的点，供查询预算裁决。
    结果与输入顺序无关（落桶按 floor 对齐，天然可复现）。
    """
    window_ns = validate_range(start_ts, end_ts, window_seconds)
    buckets: dict[object, dict[int, list[float]]] = {}
    in_range = 0
    for p in points:
        if p.ts < start_ts or p.ts >= end_ts:
            continue
        w_start = align_floor(p.ts, window_ns)
        if w_start < start_ts or w_start + window_ns > end_ts:
            continue  # 不完整窗口不参与
        in_range += 1
        if group_field is None:
            group_key: object = None
        else:
            group_key = p.values.get(group_field)
            if group_key is None:
                group_key = MISSING_KEY
        value = p.values.get(metric_field)
        if value is None or isinstance(value, bool) or not isinstance(
            value, (int, float)
        ):
            continue
        buckets.setdefault(group_key, {}).setdefault(w_start, []).append(
            float(value)
        )
    return buckets, in_range
