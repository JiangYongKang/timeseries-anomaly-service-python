"""乱序、重复时间点、窗口划分、缺失值与异常结论的稳定性与可复现。"""

import random

import pytest

from anomaly_service.errors import (
    DuplicatePointError,
    InvalidWindowError,
)
from anomaly_service.models import AnomalyStatus as A

from .conftest import log_query, log_snapshot, ts


def test_out_of_order_import_rejected_for_exact_duplicate(service):
    # 乱序合法；批内重复时间点必须拒绝
    shuffled = [
        {"ts": ts(3), "x": 3.0},
        {"ts": ts(1), "x": 1.0},
        {"ts": ts(2), "x": 2.0},
    ]
    r = service.import_data("d", shuffled, batch_id="b1")
    assert r.version == 1

    dup = shuffled + [{"ts": ts(2), "x": 2.0}]
    with pytest.raises(DuplicatePointError) as ei:
        service.import_data("d2", dup, batch_id="x")
    assert ei.value.code == "duplicate_timestamp"


def test_window_results_independent_of_arrival_order(service):
    ordered = [{"ts": ts(i), "x": float(i + 1)} for i in range(8)]
    rng = random.Random(42)
    shuffled = ordered[:]
    rng.shuffle(shuffled)

    service.import_data("a", ordered, batch_id="b1")
    service.import_data("b", shuffled, batch_id="b1")

    qa = service.query("a", "x", ts(0), ts(8), 1)
    qb = service.query("b", "x", ts(0), ts(8), 1)
    log_query("ordered", qa)
    assert qa.windows == qb.windows
    assert [w.aggregate for w in qa.windows] == [float(i + 1) for i in range(8)]


def test_incomplete_windows_are_excluded(service):
    service.import_data("d", [{"ts": ts(i), "x": 1.0} for i in range(4)], batch_id="b")
    # 区间 [0.5s, 2.5s)：只有 [1,2) 完整；[0,.] 与 [2,..] 不完整
    q = service.query("d", "x", ts(0) + ts(1) // 2, ts(2) + ts(1) // 2, 1)
    assert len(q.windows) == 1
    assert q.windows[0].window_start == ts(1)


def test_invalid_window_params(service):
    service.import_data("d", [{"ts": ts(0), "x": 1.0}], batch_id="b")
    with pytest.raises(InvalidWindowError):
        service.query("d", "x", ts(0), ts(1), 0)
    with pytest.raises(InvalidWindowError):
        service.query("d", "x", ts(2), ts(1), 1)


def test_no_data_windows_never_flag(service):
    service.import_data("d", [{"ts": ts(0), "x": 1.0}, {"ts": ts(4), "x": 1.0}], batch_id="b")
    q = service.query("d", "x", ts(0), ts(8), 1)
    log_query("sparse", q)
    statuses = [w.status for w in q.windows]
    # 空窗口全部 no_data；有数据但基线不足的窗口 insufficient_data，均非异常
    assert A.ANOMALY not in statuses
    assert statuses.count(A.NO_DATA) >= 6
    for w in q.windows:
        if w.status is A.NO_DATA:
            assert w.aggregate is None
            assert "no_data" in w.evidence.reason


def test_missing_values_are_skipped(service):
    pts = [
        {"ts": ts(0), "x": 2.0},
        {"ts": ts(0) + 10, "x": None},
        {"ts": ts(0) + 20, "x": 4.0},
    ]  # 含 null -> x 推断为可空列
    service.import_data("d", pts, batch_id="b")
    q = service.query("d", "x", ts(0), ts(1), 1)
    w = q.windows[0]
    # None 被跳过：count=2, mean=3
    assert w.count == 2 and w.aggregate == 3.0


def test_zscore_anomaly_with_evidence_and_reproducibility(service):
    # 8 个有波动的基线窗口（均值约 10，有非零标准差），第 9 个窗口突增
    vals = [10.0, 11.0, 9.0, 10.5, 9.5, 10.0, 11.0, 9.0]
    pts = [{"ts": ts(i), "x": vals[i]} for i in range(8)]
    pts.append({"ts": ts(8), "x": 500.0})
    service.import_data("d", pts, batch_id="b")
    q1 = service.query("d", "x", ts(0), ts(9), 1)
    q2 = service.query("d", "x", ts(0), ts(9), 1)
    log_query("spike", q1)
    assert q1 == q2  # 可复现
    last = q1.windows[-1]
    assert last.status is A.ANOMALY
    assert last.evidence.zscore is not None
    assert abs(last.evidence.zscore) > last.evidence.threshold
    assert last.evidence.baseline_mean is not None
    assert "z-score" in last.evidence.reason
    # 前 8 个窗口不应被误报（基线不足或正常）
    assert all(w.status is not A.ANOMALY for w in q1.windows[:-1])


def test_constant_baseline_deviation(service):
    pts = [{"ts": ts(i), "x": 5.0} for i in range(5)]
    service.import_data("d", pts, batch_id="b")
    service.import_data("d", [{"ts": ts(5), "x": 5.0}, {"ts": ts(6), "x": 9.0}],
                        batch_id="b2", append=True)
    q = service.query("d", "x", ts(0), ts(7), 1)
    assert q.windows[6].status is A.ANOMALY
    assert q.windows[6].evidence.zscore is None  # 除零保护
    assert q.windows[5].status is A.NORMAL


def test_versions_remain_reproducible_after_append(service):
    service.import_data("d", [{"ts": ts(i), "x": 1.0} for i in range(4)], batch_id="b1")
    service.import_data("d", [{"ts": ts(i), "x": 100.0} for i in range(4, 8)],
                        batch_id="b2", append=True)
    log_snapshot(service, "d", 1)
    log_snapshot(service, "d", 2)
    qv1 = service.query("d", "x", ts(0), ts(4), 1, version=1)
    qv1_again = service.query("d", "x", ts(0), ts(4), 1, version=1)
    assert qv1 == qv1_again
    assert qv1.version == 1
    assert all(w.aggregate == 1.0 for w in qv1.windows)
    latest = service.query("d", "x", ts(4), ts(8), 1)
    assert latest.version == 2
    assert all(w.aggregate == 100.0 for w in latest.windows)
