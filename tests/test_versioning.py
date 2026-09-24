"""版本切换：查询固定到具体不可变快照，新版本发布后旧结论不变。"""

import pytest

from anomaly_service.errors import DuplicatePointError, VersionNotFoundError

from .conftest import log_query, log_snapshot, ts


def test_pinned_version_unchanged_after_new_versions(service):
    service.import_data("d", [{"ts": ts(i), "x": 1.0} for i in range(3)], batch_id="b1")
    service.import_data("d", [{"ts": ts(i), "x": 9.0} for i in range(3, 6)],
                        batch_id="b2", append=True)
    service.import_data("d", [{"ts": ts(i), "x": -5.0} for i in range(6, 9)],
                        batch_id="b3", append=True)

    log_snapshot(service, "d", 1)
    log_snapshot(service, "d", 3)
    assert service.list_versions("d") == [1, 2, 3]

    q_v1_a = service.query("d", "x", ts(0), ts(3), 1, version=1)
    q_latest = service.query("d", "x", ts(0), ts(9), 1)
    assert q_latest.version == 3
    q_v1_b = service.query("d", "x", ts(0), ts(3), 1, version=1)
    assert q_v1_a == q_v1_b
    assert q_v1_a.version == 1
    log_query("v1", q_v1_a)
    assert all(w.aggregate == 1.0 for w in q_v1_a.windows)

    # v2 与 v3 对同一窗口（其包含的数据不同）结论不同，且各自可复现
    q2 = service.query("d", "x", ts(3), ts(6), 1, version=2)
    q3 = service.query("d", "x", ts(6), ts(9), 1, version=3)
    assert all(w.aggregate == 9.0 for w in q2.windows)
    assert all(w.aggregate == -5.0 for w in q3.windows)


def test_unknown_version_raises_distinguishable_error(service):
    service.import_data("d", [{"ts": ts(0), "x": 1.0}], batch_id="b")
    try:
        service.query("d", "x", ts(0), ts(1), 1, version=42)
    except VersionNotFoundError as exc:
        assert exc.code == "version_not_found"
    else:
        raise AssertionError


def test_latest_pointer_switches_atomically(service):
    r1 = service.import_data("d", [{"ts": ts(0), "x": 1.0}], batch_id="b1")
    r2 = service.import_data("d", [{"ts": ts(1), "x": 2.0}], batch_id="b2", append=True)
    assert r1.version == 1 and r2.version == 2
    # latest 指针切换：旧快照对象内容不被改写
    v1 = service._store.get_version("d", 1)
    v2 = service._store.get_version("d", 2)
    assert len(v1.points) == 1 and len(v2.points) == 2
    assert service._store.latest_version("d").version == 2
    assert v1.points[0].ts == ts(0)


def test_failed_append_leaves_no_new_version(service):
    service.import_data("d", [{"ts": ts(i), "x": 1.0} for i in range(3)], batch_id="b1")
    # 触发重复时间点失败
    with pytest.raises(DuplicatePointError):
        service.import_data("d", [{"ts": ts(1), "x": 2.0}], batch_id="bad", append=True)
    assert service.list_versions("d") == [1]
    log_snapshot(service, "d", 1)
    # 失败批次号未注册，可用相同号重新提交合法数据
    r = service.import_data("d", [{"ts": ts(5), "x": 2.0}], batch_id="bad", append=True)
    assert r.version == 2
