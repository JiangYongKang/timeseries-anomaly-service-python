"""窗口聚合与异常判定：稳定语义、空窗口保护、乱序无关、结论可复现。"""
from __future__ import annotations

import pytest

from app.anomaly import detect
from app.errors import InvalidParameterError, ResourceLimitExceededError
from app.models import AggMethod, DataPoint, Schema
from app.store import VersionStore
from app.windows import (
    MAX_WINDOWS_PER_QUERY,
    build_aggregates,
    default_range,
    parse_window,
    window_start_of,
)

T0 = 1577836800000


def _version(points):
    s = Schema.parse({"temp": "float"})
    return VersionStore().import_batch(
        dataset="d", batch_id="b", schema=s, points=points)[0]


def test_parse_window_units_and_rejection():
    assert parse_window("30s") == 30_000
    assert parse_window("5m") == 300_000
    assert parse_window("1h") == 3_600_000
    assert parse_window("1d") == 86_400_000
    for bad in ("", "m", "0m", "-5m", "5x", 123):
        with pytest.raises(InvalidParameterError):
            parse_window(bad)


def test_grid_alignment_is_epoch_based():
    assert window_start_of(T0 + 123, 60_000) == T0


def test_unaligned_range_rejected():
    v = _version([DataPoint(T0, {"temp": 1.0})])
    with pytest.raises(InvalidParameterError):
        build_aggregates(v, "temp", AggMethod.MEAN, 60_000, T0 + 1, T0 + 60_000)


def test_unknown_metric_rejected():
    v = _version([DataPoint(T0, {"temp": 1.0})])
    with pytest.raises(InvalidParameterError):
        build_aggregates(v, "nope", AggMethod.MEAN, 60_000, T0, T0 + 60_000)


def test_window_count_limit():
    v = _version([DataPoint(T0, {"temp": 1.0})])
    with pytest.raises(ResourceLimitExceededError):
        build_aggregates(v, "temp", AggMethod.MEAN, 60_000,
                         T0, T0 + (MAX_WINDOWS_PER_QUERY + 1) * 60_000)


def test_out_of_order_and_duplicate_points_do_not_change_result():
    """乱序导入 + 同时间窗口多点：结论只取决于内容，与到达顺序无关。"""
    rows = [
        DataPoint(T0 + 90_000, {"temp": 40.0}),
        DataPoint(T0 + 30_000, {"temp": 20.0}),
        DataPoint(T0, {"temp": 10.0}),
        DataPoint(T0 + 60_000, {"temp": 30.0}),
    ]
    v1 = _version(rows)
    v2 = _version(list(reversed(rows)))

    a1 = build_aggregates(v1, "temp", AggMethod.MEAN, 60_000, T0, T0 + 120_000)
    a2 = build_aggregates(v2, "temp", AggMethod.MEAN, 60_000, T0, T0 + 120_000)
    sig1 = [(x.window_start, x.value, x.count) for x in a1]
    sig2 = [(x.window_start, x.value, x.count) for x in a2]
    assert sig1 == sig2 == [
        (T0, 15.0, 2),
        (T0 + 60_000, 35.0, 2),
    ]


def test_methods_sum_min_max():
    v = _version([DataPoint(T0, {"temp": 10.0}),
                  DataPoint(T0 + 1, {"temp": 20.0})])
    for method, expected in [
        (AggMethod.SUM, 30.0), (AggMethod.MIN, 10.0),
        (AggMethod.MAX, 20.0), (AggMethod.MEAN, 15.0),
    ]:
        aggs = build_aggregates(v, "temp", method, 60_000, T0, T0 + 60_000)
        assert aggs[0].value == expected


def test_empty_window_has_none_value_and_no_anomaly():
    # 数据只落在第 1 和第 3 个窗口
    v = _version([DataPoint(T0, {"temp": 10.0}),
                  DataPoint(T0 + 120_000, {"temp": 12.0})])
    aggs = build_aggregates(v, "temp", AggMethod.MEAN, 60_000,
                            T0, T0 + 180_000)
    assert aggs[1].count == 0 and aggs[1].value is None
    res = detect(v, "temp", aggs, z_threshold=3.0, min_baseline=1)
    assert res[1].is_anomaly is False
    assert res[1].reason.startswith("NO_DATA")


def test_missing_count_with_interval():
    v = _version([DataPoint(T0, {"temp": 1.0})])
    aggs = build_aggregates(v, "temp", AggMethod.MEAN, 60_000,
                            T0, T0 + 120_000, interval_ms=30_000)
    # 每窗口期望 2 个点
    assert aggs[0].missing == 1
    assert aggs[1].missing == 2


def test_window_not_multiple_of_interval_rejected():
    v = _version([DataPoint(T0, {"temp": 1.0})])
    with pytest.raises(InvalidParameterError):
        build_aggregates(v, "temp", AggMethod.MEAN, 60_000,
                         T0, T0 + 60_000, interval_ms=45_000)


def test_insufficient_baseline_not_anomaly():
    v = _version([DataPoint(T0 + i * 60_000, {"temp": 100.0 + i})
                  for i in range(2)])
    aggs = build_aggregates(v, "temp", AggMethod.MEAN, 60_000,
                            T0, T0 + 120_000)
    res = detect(v, "temp", aggs, z_threshold=3.0, min_baseline=3)
    assert all(not r.is_anomaly for r in res)
    assert res[1].reason.startswith("INSUFFICIENT_BASELINE")


def test_zscore_anomaly_and_evidence():
    # 4 个稳定基线窗口 + 1 个尖峰
    pts = [DataPoint(T0 + i * 60_000, {"temp": 10.0}) for i in range(4)]
    pts.append(DataPoint(T0 + 4 * 60_000, {"temp": 100.0}))
    v = _version(pts)
    aggs = build_aggregates(v, "temp", AggMethod.MEAN, 60_000,
                            T0, T0 + 300_000)
    res = detect(v, "temp", aggs, z_threshold=3.0, min_baseline=3)
    assert [r.is_anomaly for r in res] == [False, False, False, False, True]
    spike = res[4]
    assert spike.evidence is not None
    assert spike.evidence.baseline_n == 4
    assert spike.evidence.baseline_mean == 10.0
    assert spike.evidence.threshold == 3.0
    assert spike.evidence.observed == 100.0
    assert "zscore" in spike.reason or "基线" in spike.reason


def test_baseline_uses_only_past_windows():
    """尖峰出现在早期时，其后的正常点不能被未来信息“污染”判定。"""
    pts = [DataPoint(T0, {"temp": 100.0})]  # 单点，基线不足
    pts += [DataPoint(T0 + i * 60_000, {"temp": 10.0}) for i in range(1, 5)]
    v = _version(pts)
    aggs = build_aggregates(v, "temp", AggMethod.MEAN, 60_000,
                            T0, T0 + 300_000)
    res = detect(v, "temp", aggs, z_threshold=3.0, min_baseline=2)
    # 第 0 窗基线不足非异常；后面窗口的基线会包含 100，但第 1 窗也只有1个历史窗
    assert res[0].is_anomaly is False
    # 结果对历史窗口数量的依赖是确定性的
    assert all(r.evidence is None or r.evidence.baseline_n >= 2 for r in res)


def test_result_reproducible_same_version_same_params():
    pts = [DataPoint(T0 + i * 60_000, {"temp": 10.0}) for i in range(4)]
    pts += [DataPoint(T0 + 4 * 60_000, {"temp": 95.0}),
            DataPoint(T0 + 5 * 60_000, {"temp": 10.0})]
    v = _version(pts)
    kw = {"z_threshold": 3.0, "min_baseline": 3}
    a = build_aggregates(v, "temp", AggMethod.MEAN, 60_000, T0, T0 + 360_000)
    r1 = detect(v, "temp", a, **kw)
    r2 = detect(v, "temp", a, **kw)
    sig = [(r.window_start, r.is_anomaly, r.reason,
            None if r.evidence is None else round(r.evidence.zscore, 12))
           for r in r1]
    assert sig == [(r.window_start, r.is_anomaly, r.reason,
                    None if r.evidence is None else round(r.evidence.zscore, 12))
                   for r in r2]


def test_default_range_covers_all_points():
    v = _version([DataPoint(T0 + 30_000, {"temp": 1.0}),
                  DataPoint(T0 + 90_000, {"temp": 2.0})])
    start, end = default_range(v, 60_000)
    assert (start, end) == (T0, T0 + 120_000)
