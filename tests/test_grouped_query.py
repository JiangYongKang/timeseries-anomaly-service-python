"""分组分析：按分类维度拆序列、独立基线、缺失归属、空序列不误报、可复现。"""

import pytest

from anomaly_service.errors import FieldNotFoundError, ValidationError
from anomaly_service.models import MISSING_GROUP_KEY, AnomalyStatus

from .conftest import log, log_grouped_query, log_snapshot, ts


def _seed(service, dataset="d"):
    """两个设备 + 一个缺分类值的点；dev-a 最后一秒突增。

    时间戳在版本内必须唯一：dev-b 偏移 1ns，与 dev-a 落在同一 1s 窗口。
    """
    pts = []
    for i in range(5):
        pts.append({"ts": ts(i), "x": 10.0, "dev": "a"})
        pts.append({"ts": ts(i) + 1, "x": 20.0, "dev": "b"})
    pts.append({"ts": ts(5), "x": 99.0, "dev": "a"})
    pts.append({"ts": ts(5) + 1, "x": 20.1, "dev": None})
    log.info("[seed] input_points=%s", pts)
    service.import_data(dataset, pts, batch_id="seed")
    log_snapshot(service, dataset, 1)


def test_grouped_results_are_reproducible(service):
    _seed(service)
    r1 = service.query_grouped("d", "x", "dev", ts(0), ts(6), 1)
    r2 = service.query_grouped("d", "x", "dev", ts(0), ts(6), 1)
    log_grouped_query("reproducible", r1)
    # 同版本 + 同参数 + 同区间：逐字节相等
    assert r1 == r2
    # 分组顺序确定：缺失桶固定最前，其余按键排序
    assert [g.group_key for g in r1.groups] == [MISSING_GROUP_KEY, "a", "b"]


def test_each_group_has_independent_baseline(service):
    _seed(service)
    r = service.query_grouped("d", "x", "dev", ts(0), ts(6), 1)
    log_grouped_query("independent-baseline", r)
    by_key = {g.group_key: g for g in r.groups}
    # dev-a 最后一窗 99.0 相对自身基线（10.0）判 anomaly
    a_last = by_key["a"].windows[-1]
    assert a_last.status is AnomalyStatus.ANOMALY
    assert a_last.evidence is not None and a_last.evidence.zscore is None  # 恒定基线
    # dev-b 最后一窗无数据 → no_data；其基线不受 dev-a 突增影响
    b_last = by_key["b"].windows[-1]
    assert b_last.status is AnomalyStatus.NO_DATA
    b_prev = by_key["b"].windows[-2]
    assert b_prev.status is AnomalyStatus.NORMAL


def test_missing_group_value_explicitly_attributed(service):
    _seed(service)
    r = service.query_grouped("d", "x", "dev", ts(0), ts(6), 1)
    log_grouped_query("missing-group", r)
    by_key = {g.group_key: g for g in r.groups}
    # 缺分类值的点没有丢：归入 <missing> 桶且数值可见
    missing = by_key[MISSING_GROUP_KEY]
    last = missing.windows[-1]
    assert last.aggregate == 20.1
    assert last.count == 1
    # 缺失桶只有 1 个有效窗口，基线不足 → insufficient_data，绝不误报 anomaly
    assert last.status is AnomalyStatus.INSUFFICIENT_DATA
    for w in missing.windows[:-1]:
        assert w.status is AnomalyStatus.NO_DATA


def test_empty_series_and_empty_windows_never_false_positive(service):
    # 只有一个分组有数据；另一分组完全落在查询区间之外
    service.import_data(
        "d",
        [{"ts": ts(i), "x": 10.0, "dev": "a"} for i in range(6)]
        + [{"ts": ts(100), "x": 999.0, "dev": "far"}],
        batch_id="seed",
    )
    r = service.query_grouped("d", "x", "dev", ts(0), ts(6), 1)
    log_grouped_query("empty-series", r)
    # 区间外的分组根本不出现（区间内无数据点）
    assert [g.group_key for g in r.groups] == ["a"]
    # 区间内的空窗口（若有）也只会是 no_data
    statuses = {w.status for g in r.groups for w in g.windows}
    assert AnomalyStatus.ANOMALY not in statuses

    # 稀疏数据：大量空窗口不得误报
    service.import_data(
        "s",
        [{"ts": ts(0), "x": 1.0, "dev": "a"}, {"ts": ts(9), "x": 1.0, "dev": "a"}],
        batch_id="seed",
    )
    r2 = service.query_grouped("s", "x", "dev", ts(0), ts(10), 1)
    log_grouped_query("sparse", r2)
    statuses2 = [w.status for g in r2.groups for w in g.windows]
    assert AnomalyStatus.ANOMALY not in statuses2
    assert AnomalyStatus.NO_DATA in statuses2


def test_grouped_query_pinned_to_version(service):
    _seed(service)
    service.import_data(
        "d", [{"ts": ts(6), "x": 1.0, "dev": "a"}], batch_id="b2", append=True
    )
    log_snapshot(service, "d", 2)
    r_v1 = service.query_grouped("d", "x", "dev", ts(0), ts(6), 1, version=1)
    r_latest = service.query_grouped("d", "x", "dev", ts(0), ts(6), 1)
    log_grouped_query("pinned-v1", r_v1)
    assert r_v1.version == 1
    assert r_latest.version == 2
    # v1 的结论不受后续追加影响
    assert r_v1 == service.query_grouped("d", "x", "dev", ts(0), ts(6), 1, version=1)


def test_group_field_must_be_categorical(service):
    _seed(service)
    with pytest.raises(ValidationError) as ei:
        service.query_grouped("d", "x", "x", ts(0), ts(6), 1)  # 数值列不可分组
    assert ei.value.code == "invalid_group_field"
    with pytest.raises(ValidationError) as ei:
        service.query_grouped("d", "x", "nope", ts(0), ts(6), 1)
    assert ei.value.code == "invalid_group_field"
    with pytest.raises(FieldNotFoundError):
        service.query_grouped("d", "dev", "dev", ts(0), ts(6), 1)  # 指标须数值列
