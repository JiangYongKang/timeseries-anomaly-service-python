"""查询资源预算与默认超时：超限明确失败、与参数错误可区分、状态无污染。"""


import pytest

from anomaly_service.config import ServiceConfig
from anomaly_service.errors import (
    QueryBudgetError,
    ResourceLimitError,
    TimeoutError_,
    ValidationError,
)
from anomaly_service.service import AnomalyService

from .conftest import log, ts


def _seed(service, n=10, groups=2, dataset="d"):
    pts = []
    for i in range(n):
        for g in range(groups):
            pts.append({"ts": ts(i) + g, "x": float(i), "dev": f"g{g}"})
    service.import_data(dataset, pts, batch_id="seed")
    return pts


def test_window_budget_counts_groups_times_slots():
    cfg = ServiceConfig(max_query_windows=12)
    svc = AnomalyService(cfg)
    _seed(svc, n=10, groups=2)
    # 10 槽位 × 2 组 = 20 窗口 > 12
    with pytest.raises(QueryBudgetError) as ei:
        svc.query("d", "x", ts(0), ts(10), 1, group_by="dev")
    log.info("[window-budget] code=%s message=%s", ei.value.code, ei.value)
    assert ei.value.code == "query_budget_exceeded"
    # 与参数写错（422 族）明确区分：预算错误不是 ValidationError
    assert not isinstance(ei.value, ValidationError)
    assert isinstance(ei.value, ResourceLimitError)
    # 不分组时 10 窗口 <= 12，同一区间可以成功
    result = svc.query("d", "x", ts(0), ts(10), 1)
    assert len(result.windows) == 10


def test_window_budget_also_bounds_ungrouped_queries():
    cfg = ServiceConfig(max_query_windows=5)
    svc = AnomalyService(cfg)
    _seed(svc, n=10, groups=1)
    with pytest.raises(QueryBudgetError):
        svc.query("d", "x", ts(0), ts(10), 1)
    # 缩小区间到预算内即可成功
    assert len(svc.query("d", "x", ts(0), ts(5), 1).windows) == 5


def test_points_budget_counts_in_range_points():
    cfg = ServiceConfig(max_query_points=10)
    svc = AnomalyService(cfg)
    _seed(svc, n=10, groups=2)  # 区间内 20 点
    with pytest.raises(QueryBudgetError) as ei:
        svc.query("d", "x", ts(0), ts(10), 1, group_by="dev")
    log.info("[points-budget] code=%s message=%s", ei.value.code, ei.value)
    assert ei.value.code == "query_budget_exceeded"
    # 半开区间 [0,5s) 内 10 点，刚好不超限
    result = svc.query("d", "x", ts(0), ts(5), 1, group_by="dev")
    assert sum(w.count for g in result.groups for w in g.windows) == 10


def test_budget_failure_leaves_no_state_and_followups_work():
    cfg = ServiceConfig(max_query_windows=4)
    svc = AnomalyService(cfg)
    _seed(svc, n=10, groups=2)
    with pytest.raises(QueryBudgetError):
        svc.query("d", "x", ts(0), ts(10), 1, group_by="dev")
    # 版本与数据未被查询失败影响
    assert svc.list_versions("d") == [1]
    assert len(svc._store.latest_version("d").points) == 20
    # 后续请求（含导入与正常查询）不受影响
    svc.import_data("d", [{"ts": ts(20), "x": 1.0, "dev": "g0"}],
                    batch_id="b2", append=True)
    assert svc.list_versions("d") == [1, 2]
    assert len(svc.query("d", "x", ts(0), ts(4), 1).windows) == 4


def test_default_timeout_applies_when_not_specified():
    cfg = ServiceConfig(default_timeout_seconds=0.0)
    svc = AnomalyService(cfg)
    # 导入：未显式给 timeout，默认截止时间真实生效
    with pytest.raises(TimeoutError_) as ei:
        svc.import_data("d", [{"ts": ts(0), "x": 1.0}])
    assert ei.value.code == "timeout"
    # 显式 timeout 覆盖默认值
    r = svc.import_data("d", [{"ts": ts(0), "x": 1.0}], timeout=30.0)
    assert r.version == 1
    # 查询：默认超时同样生效
    svc2 = AnomalyService(ServiceConfig(default_timeout_seconds=0.0))
    svc2.import_data("d", [{"ts": ts(i), "x": 1.0} for i in range(4)],
                     timeout=30.0)
    with pytest.raises(TimeoutError_):
        svc2.query("d", "x", ts(0), ts(4), 1)
    # 显式超时后正常返回
    assert len(svc2.query("d", "x", ts(0), ts(4), 1, timeout=30.0).windows) == 4


def test_budget_error_distinguishable_from_param_errors(service):
    _seed(service, n=6, groups=1)
    # 参数写错：invalid_window（ValidationError 族）
    with pytest.raises(ValidationError):
        service.query("d", "x", ts(5), ts(0), 1)
    # 预算超限：QueryBudgetError，code 不同、异常族不同
    cfg = ServiceConfig(max_query_windows=2)
    svc = AnomalyService(cfg)
    _seed(svc, n=6, groups=1)
    with pytest.raises(QueryBudgetError) as ei:
        svc.query("d", "x", ts(0), ts(6), 1)
    assert ei.value.code == "query_budget_exceeded"
