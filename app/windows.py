"""时间窗口划分、聚合与缺失值处理。

语义约定（在同版本 + 同参数下结果恒定，与数据到达顺序无关）
-----------------------------------------------------------
- 时间戳为 epoch 毫秒；窗口以 Unix epoch (1970-01-01T00:00Z) 为网格起点对齐，
  例如 window=5m 时，窗口边界恒为 ...10:00, 10:05, 10:10...。
- 分析范围为左闭右开 ``[start, end)``，且 ``start``、``end`` 必须落在窗口网格上、
  ``end > start``。未对齐直接报 INVALID_PARAMETER，避免隐式截断产生歧义。
- 每个窗口对落入其内（``window_start <= ts < window_end``）的数据点聚合，
  存储层已保证时间点唯一且按时间升序，因此乱序导入不影响结论。
- 空窗口：``count == 0``、``value is None``，后续异常检测不得对其报警。
- 缺失值：若调用方给出期望采样间隔 ``interval_ms``，则
  ``missing = 窗口理论点数(window_ms/interval_ms) - count``；未给出时 missing=0。
"""
from __future__ import annotations

from app.errors import InvalidParameterError
from app.models import AggMethod, DatasetVersion, WindowAggregate

_UNIT_MS: dict[str, int] = {
    "s": 1_000,
    "m": 60_000,
    "h": 3_600_000,
    "d": 86_400_000,
}

MAX_WINDOWS_PER_QUERY = 100_000


def parse_window(window: str) -> int:
    """解析窗口表达式（``'<n><unit>'``，单位 s/m/h/d）为毫秒。"""
    if not isinstance(window, str) or len(window) < 2:
        raise InvalidParameterError(
            "window 必须形如 '30s'/'5m'/'1h'/'1d'", details={"window": repr(window)}
        )
    num_part, unit = window[:-1], window[-1]
    if unit not in _UNIT_MS or not num_part.isdigit():
        raise InvalidParameterError(
            "window 必须形如 '30s'/'5m'/'1h'/'1d'", details={"window": window}
        )
    n = int(num_part)
    if n <= 0:
        raise InvalidParameterError("window 数值必须为正整数", details={"window": window})
    return n * _UNIT_MS[unit]


def parse_interval(interval: str | None) -> int | None:
    """解析期望采样间隔；None 表示不计算缺失值计数。"""
    if interval is None:
        return None
    ms = parse_window(interval)
    return ms


def window_start_of(ts_ms: int, window_ms: int) -> int:
    """时间戳所属窗口的网格起点。"""
    return (ts_ms // window_ms) * window_ms


def validate_range(start: int, end: int, window_ms: int) -> None:
    """校验分析范围：整数毫秒、正区间、与窗口网格对齐，且窗口数不超上限。"""
    if isinstance(start, bool) or isinstance(end, bool):
        raise InvalidParameterError("start/end 必须是 epoch 毫秒整数")
    if not isinstance(start, int) or not isinstance(end, int):
        raise InvalidParameterError("start/end 必须是 epoch 毫秒整数")
    if end <= start:
        raise InvalidParameterError(
            "要求 end > start（区间左闭右开）", details={"start": start, "end": end}
        )
    if start % window_ms != 0 or end % window_ms != 0:
        raise InvalidParameterError(
            "start/end 必须与窗口网格对齐",
            details={"start": start, "end": end, "window_ms": window_ms},
        )
    n_windows = (end - start) // window_ms
    if n_windows > MAX_WINDOWS_PER_QUERY:
        from app.errors import ResourceLimitExceededError

        raise ResourceLimitExceededError(
            f"窗口数 {n_windows} 超过上限 {MAX_WINDOWS_PER_QUERY}",
            details={"windows": n_windows, "limit": MAX_WINDOWS_PER_QUERY},
        )


def default_range(version: DatasetVersion, window_ms: int) -> tuple[int, int]:
    """未显式给定时，以数据时间范围对齐网格作为分析范围。"""
    tr = version.time_range
    if tr is None:
        raise InvalidParameterError("版本无数据，无法推导分析范围，请显式指定 start/end")
    lo, hi = tr
    return window_start_of(lo, window_ms), window_start_of(hi, window_ms) + window_ms


def build_aggregates(
    version: DatasetVersion,
    metric: str,
    method: AggMethod,
    window_ms: int,
    start: int,
    end: int,
    *,
    interval_ms: int | None = None,
) -> list[WindowAggregate]:
    """对 ``[start, end)`` 的网格窗口逐个聚合，返回长度等于窗口数的稠密序列。"""
    if metric not in version.schema.fields:
        raise InvalidParameterError(
            f"指标 {metric!r} 不在版本 schema 中",
            details={"metric": metric,
                     "available": list(version.schema.fields)},
        )
    validate_range(start, end, window_ms)
    if interval_ms is not None and window_ms % interval_ms != 0:
        raise InvalidParameterError(
            "window 必须是 interval 的整数倍",
            details={"window_ms": window_ms, "interval_ms": interval_ms},
        )

    n_windows = (end - start) // window_ms
    buckets: list[list[float]] = [[] for _ in range(n_windows)]

    # 数据已按时间升序；仅扫描范围内的点
    for pt in version.points:
        if pt.timestamp < start:
            continue
        if pt.timestamp >= end:
            break
        idx = (pt.timestamp - start) // window_ms
        buckets[idx].append(pt.values[metric])

    expected = (
        window_ms // interval_ms if interval_ms else 0
    )
    result: list[WindowAggregate] = []
    for i, vals in enumerate(buckets):
        w_start = start + i * window_ms
        result.append(
            WindowAggregate(
                window_start=w_start,
                window_end=w_start + window_ms,
                metric=metric,
                method=method,
                value=_aggregate(vals, method),
                count=len(vals),
                missing=max(0, expected - len(vals)) if expected else 0,
            )
        )
    return result


def _aggregate(vals: list[float], method: AggMethod) -> float | None:
    if not vals:
        return None
    if method is AggMethod.MEAN:
        return sum(vals) / len(vals)
    if method is AggMethod.SUM:
        return sum(vals)
    if method is AggMethod.MIN:
        return min(vals)
    if method is AggMethod.MAX:
        return max(vals)
    raise InvalidParameterError("不支持的聚合方法", details={"method": str(method)})
