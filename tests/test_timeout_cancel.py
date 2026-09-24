"""超时 / 取消 / 资源上限：明确失败、无半写入、无重复任务、不影响后续请求。"""

import threading

import pytest

from anomaly_service.config import ServiceConfig
from anomaly_service.errors import (
    CancelledError_,
    DatasetNotFoundError,
    ResourceLimitError,
    TimeoutError_,
)
from anomaly_service.service import AnomalyService

from .conftest import ts


def _big_batch(n: int):
    return [{"ts": ts(i), "x": float(i)} for i in range(n)]


def test_timeout_during_validation_fails_cleanly():
    svc = AnomalyService(ServiceConfig(max_points_per_batch=200_000))
    with pytest.raises(TimeoutError_) as ei:
        svc.import_data("d", _big_batch(50_000), timeout=0.0)
    assert ei.value.code == "timeout"
    # 未留下任何数据集 / 版本 / 批次
    with pytest.raises(DatasetNotFoundError):
        svc.list_versions("d")
    # 后续正常请求不受影响
    r = svc.import_data("d", _big_batch(3), batch_id="ok")
    assert r.version == 1


def test_cancelled_import_leaves_no_state():
    svc = AnomalyService()
    cancel = threading.Event()
    cancel.set()
    with pytest.raises(CancelledError_) as ei:
        svc.import_data("d", _big_batch(10), cancel_event=cancel)
    assert ei.value.code == "cancelled"
    with pytest.raises(DatasetNotFoundError):
        svc.list_versions("d")


def test_cancel_after_first_version_does_not_corrupt_latest():
    svc = AnomalyService()
    svc.import_data("d", _big_batch(4), batch_id="b1")
    cancel = threading.Event()
    cancel.set()
    extra = [{"ts": ts(10 + i), "x": float(i)} for i in range(2)]
    with pytest.raises(CancelledError_):
        svc.import_data(
            "d", extra, batch_id="b2", append=True, cancel_event=cancel
        )
    # latest 仍为 v1，批次 b2 未注册
    assert svc.list_versions("d") == [1]
    assert len(svc._store.latest_version("d").points) == 4
    # 取消信号移除后可正常用相同批次号提交
    r = svc.import_data("d", extra, batch_id="b2", append=True)
    assert r.version == 2


def test_cancelled_query_leaves_no_side_effect(service):
    service.import_data("d", _big_batch(20), batch_id="b")
    cancel = threading.Event()
    cancel.set()
    with pytest.raises(CancelledError_):
        service.query("d", "x", ts(0), ts(20), 1, cancel_event=cancel)
    # 数据完好，正常查询可用
    q = service.query("d", "x", ts(0), ts(20), 1)
    assert len(q.windows) == 20


def test_query_timeout_is_distinguishable(service):
    service.import_data("d", _big_batch(20), batch_id="b")
    with pytest.raises(TimeoutError_):
        service.query("d", "x", ts(0), ts(20), 1, timeout=0.0)


def test_batch_resource_limit_rejected(service):
    cfg = ServiceConfig(max_points_per_batch=5)
    svc = AnomalyService(cfg)
    with pytest.raises(ResourceLimitError) as ei:
        svc.import_data("d", _big_batch(6))
    assert ei.value.code == "resource_limit_exceeded"
    with pytest.raises(DatasetNotFoundError):
        svc.list_versions("d")


def test_dataset_capacity_limit_rejected_without_partial_write():
    cfg = ServiceConfig(
        max_points_per_batch=100, max_points_per_dataset=10
    )
    svc = AnomalyService(cfg)
    svc.import_data("d", _big_batch(7), batch_id="b1")
    extra = [{"ts": ts(100 + i), "x": float(i)} for i in range(4)]
    with pytest.raises(ResourceLimitError):
        svc.import_data("d", extra, batch_id="b2", append=True)
    assert svc.list_versions("d") == [1]
    assert len(svc._store.latest_version("d").points) == 7
