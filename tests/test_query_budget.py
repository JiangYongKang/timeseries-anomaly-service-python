"""查询预算与默认超时：超限明确失败、与参数错误可区分、状态无污染。"""

import pytest

from anomaly_service.config import ServiceConfig
from anomaly_service.errors import (
    InvalidWindowError,
    QueryBudgetExceededError,
    TimeoutError_,
)
from anomaly_service.service import AnomalyService

from .conftest import log, log_query, ts


def _seed(svc, n=10, dataset="d", grouped=False):
    pts = [
        {"ts": ts(i), "x": float(i), **({"g": "ab"[i % 2]} if grouped else {})}
        for i in range(n)
    ]
    log.info("[seed] dataset=%s points=%s grouped=%s", dataset, n, grouped)
    svc.import_data(dataset, pts, batch_id="seed", timeout=30.0)


def test_window_budget_exceeded_fails_fast():
    svc = AnomalyService(ServiceConfig(max_query_windows=5))
    _seed(svc, 10)
    with pytest.raises(QueryBudgetExceededError) as ei:
        svc.query("d", "x", ts(0), ts(10), 1)
    log.info("[budget] code=%s message=%s", ei.value.code, ei.value)
    assert ei.value.code == "query_budget_exceeded"
    # 与参数写错（invalid_window / 422 族）明确区分
    assert not isinstance(ei.value, InvalidWindowError)


def test_grouped_windows_multiply_against_budget():
    # 10 槽位 × 2 分组 = 20 窗口 > 15：分组后预算成倍收紧
    svc = AnomalyService(ServiceConfig(max_query_windows=15))
    _seed(svc, 10, grouped=True)
    with pytest.raises(QueryBudgetExceededError) as ei:
        svc.query_grouped("d", "x", "g", ts(0), ts(10), 1)
    log.info("[grouped-budget] message=%s", ei.value)
    assert "20" in str(ei.value)
    # 同样的区间不分组则 10 窗口，在预算内
    r = svc.query("d", "x", ts(0), ts(10), 1)
    log_query("ungrouped-within-budget", r)
    assert len(r.windows) == 10


def test_points_budget_exceeded():
    svc = AnomalyService(ServiceConfig(max_query_points=5))
    _seed(svc, 10)
    with pytest.raises(QueryBudgetExceededError) as ei:
        svc.query("d", "x", ts(0), ts(10), 1)
    assert ei.value.code == "query_budget_exceeded"
    # 缩小区间后落在预算内，同一服务立即可用（预算失败无副作用）
    r = svc.query("d", "x", ts(0), ts(3), 1)
    log_query("narrowed-within-budget", r)
    assert len(r.windows) == 3


def test_default_timeout_applies_when_not_specified():
    # 未显式指定 timeout 时，默认截止时间真实生效
    svc = AnomalyService(ServiceConfig(default_timeout_seconds=0.0))
    svc.import_data(
        "d", [{"ts": ts(i), "x": 1.0} for i in range(5)],
        batch_id="seed", timeout=30.0,
    )
    with pytest.raises(TimeoutError_) as ei:
        svc.query("d", "x", ts(0), ts(5), 1)
    assert ei.value.code == "timeout"
    # 显式指定超时的请求不受影响
    r = svc.query("d", "x", ts(0), ts(5), 1, timeout=30.0)
    assert len(r.windows) == 5


def test_budget_and_timeout_failures_leave_no_state_pollution():
    # 预算失败：默认超时足够宽松，确保命中的是预算而非超时
    svc = AnomalyService(ServiceConfig(max_query_windows=3))
    _seed(svc, 10)
    with pytest.raises(QueryBudgetExceededError):
        svc.query("d", "x", ts(0), ts(10), 1)
    # 数据完好、版本未变、后续正常请求可用
    assert svc.list_versions("d") == [1]
    r = svc.query("d", "x", ts(0), ts(3), 1)
    log_query("after-budget-failure", r)
    assert len(r.windows) == 3
    # 追加也不受影响
    svc.import_data(
        "d", [{"ts": ts(10), "x": 1.0}], batch_id="b2", append=True
    )
    assert svc.list_versions("d") == [1, 2]

    # 默认超时失败：同样无任何中间状态
    svc2 = AnomalyService(ServiceConfig(default_timeout_seconds=0.0))
    svc2.import_data(
        "d", [{"ts": ts(i), "x": 1.0} for i in range(5)],
        batch_id="seed", timeout=30.0,
    )
    with pytest.raises(TimeoutError_):
        svc2.query("d", "x", ts(0), ts(5), 1)
    with pytest.raises(TimeoutError_):
        svc2.query_grouped("d", "x", "x", ts(0), ts(5), 1)
    assert svc2.list_versions("d") == [1]
    r2 = svc2.query("d", "x", ts(0), ts(5), 1, timeout=30.0)
    log_query("after-timeout-failure", r2)
    assert len(r2.windows) == 5
