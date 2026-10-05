"""分组分析：按分类维度拆分序列，各自聚合、各自基线、结论可复现。"""

import pytest

from anomaly_service.errors import FieldNotFoundError, ValidationError
from anomaly_service.models import MISSING_GROUP, AnomalyStatus

from .conftest import log_grouped_query, log_import, log_snapshot, ts


def _seed_grouped(service, dataset="d"):
    """v1 无分类列；v2 追加带 dev 列的可空维度。"""
    r1 = service.import_data(
        dataset, [{"ts": ts(i), "x": 10.0} for i in range(5)], batch_id="b1"
    )
    log_import("seed-v1", [{"ts": ts(i), "x": 10.0} for i in range(5)], r1)
    pts = []
    for i in range(5, 10):
        pts.append({"ts": ts(i), "x": 10.0, "dev": "a"})
        pts.append({"ts": ts(i) + 1, "x": 10.0, "dev": "b"})
    pts.append({"ts": ts(9) + 2, "x": 99.0, "dev": "a"})
    r2 = service.import_data(dataset, pts, batch_id="b2", append=True)
    log_import("append-v2", pts, r2)
    log_snapshot(service, dataset, 2)
    return dataset


def test_grouped_query_splits_series_and_detects_per_group(service):
    _seed_grouped(service)
    result = service.query("d", "x", ts(0), ts(10), 1, group_by="dev")
    log_grouped_query("grouped", result)

    by_group = {g.group_value: g for g in result.groups}
    # 三条互不干扰的序列：a、b 与显式的缺失组
    assert set(by_group) == {"a", "b", MISSING_GROUP}

    # 组 a：窗口 9 内 x=99 拉高均值，相对自身恒定基线判异常
    a_windows = by_group["a"].windows
    assert a_windows[-1].status is AnomalyStatus.ANOMALY
    assert a_windows[-1].evidence is not None
    # 恒定基线（std=0）按规则 zscore 记为 None（除零保护）
    assert a_windows[-1].evidence.baseline_std == 0.0
    assert a_windows[-1].evidence.zscore is None

    # 组 b：全程恒定，最后一个窗口不异常（未被 a 的尖峰污染）
    b_windows = by_group["b"].windows
    assert b_windows[-1].status is AnomalyStatus.NORMAL

    # 缺失组：v1 的 5 个点归入 __missing__，有独立基线，绝不悄悄漏掉
    missing_windows = by_group[MISSING_GROUP].windows
    assert sum(w.count for w in missing_windows) == 5


def test_grouped_query_is_fully_reproducible(service):
    _seed_grouped(service)
    r1 = service.query("d", "x", ts(0), ts(10), 1, group_by="dev")
    r2 = service.query("d", "x", ts(0), ts(10), 1, group_by="dev")
    assert r1 == r2
    # 固定到同一版本 + 同参数 + 同区间，分组顺序与结论逐字节一致
    r3 = service.query("d", "x", ts(0), ts(10), 1, group_by="dev", version=2)
    assert r1 == r3


def test_grouped_empty_windows_and_missing_group_no_false_alarm(service):
    _seed_grouped(service)
    result = service.query("d", "x", ts(0), ts(10), 1, group_by="dev")
    log_grouped_query("no-false-alarm", result)
    for g in result.groups:
        for w in g.windows:
            if w.count == 0:
                # 空窗口一律 no_data，绝不误报
                assert w.status is AnomalyStatus.NO_DATA
                assert w.aggregate is None
    # 缺失组后半段完全无数据：不会出现 anomaly
    missing = next(g for g in result.groups if g.group_value == MISSING_GROUP)
    assert all(
        w.status is not AnomalyStatus.ANOMALY for w in missing.windows
    )


def test_grouped_query_all_points_in_one_group_when_dimension_absent(service):
    service.import_data(
        "solo", [{"ts": ts(i), "x": 1.0} for i in range(4)], batch_id="b1"
    )
    # 追加一个全部同值的分类列
    service.import_data(
        "solo",
        [{"ts": ts(4 + i), "x": 1.0, "region": "cn"} for i in range(4)],
        batch_id="b2",
        append=True,
    )
    result = service.query("solo", "x", ts(0), ts(8), 1, group_by="region")
    log_grouped_query("single-real-group", result)
    assert {g.group_value for g in result.groups} == {"cn", MISSING_GROUP}


def test_grouped_query_rejects_unknown_or_non_categorical_field(service):
    service.import_data(
        "d", [{"ts": ts(i), "x": 1.0, "ratio": 0.5} for i in range(4)],
        batch_id="b1",
    )
    with pytest.raises(FieldNotFoundError) as ei:
        service.query("d", "x", ts(0), ts(4), 1, group_by="nope")
    assert ei.value.code == "field_not_found"

    # float 列不可枚举，不是合法分类维度
    with pytest.raises(ValidationError) as ei:
        service.query("d", "x", ts(0), ts(4), 1, group_by="ratio")
    assert ei.value.code == "invalid_group_field"

    # 分组字段与指标字段参数写错仍是 422 族，而非预算/超时
    with pytest.raises(ValidationError):
        service.query("d", "x", ts(0), ts(4), 1, group_by="")


def test_grouped_query_on_int_dimension(service):
    pts = []
    for i in range(8):
        pts.append({"ts": ts(i), "x": float(i % 2), "dev_id": i % 3})
    service.import_data("d", pts, batch_id="b1")
    result = service.query("d", "x", ts(0), ts(8), 1, group_by="dev_id")
    log_grouped_query("int-dimension", result)
    assert {g.group_value for g in result.groups} == {0, 1, 2}
