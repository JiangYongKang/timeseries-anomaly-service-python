"""并发追加下的结构演进：版本自洽、过期请求明确拒绝、拒绝后可安全重试。"""

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from anomaly_service.errors import StaleSchemaError

from .conftest import log, log_snapshot, ts


def _assert_self_consistent(service, dataset):
    """不变量：版本声明的字段与实际数据完全对应。"""
    v = service._store.latest_version(dataset)
    names = set(v.schema_.field_names())
    for p in v.points:
        assert set(p.values) <= names, f"数据点含 schema 未声明字段: {p.values}"
    log.info(
        "[self-consistent] dataset=%s version=v%s schema=%s points=%s",
        dataset, v.version, sorted(names), len(v.points),
    )
    return v


def test_stale_schema_append_rejected_and_retryable(service):
    service.import_data("d", [{"ts": ts(0), "x": 1.0}], batch_id="seed")
    # 另一任务先带新列演进结构
    service.import_data(
        "d", [{"ts": ts(1), "x": 2.0, "region": "cn"}], batch_id="a", append=True
    )
    log_snapshot(service, "d", 2)
    # 基于过时结构（不知 region）的追加被明确拒绝，且原因可区分
    with pytest.raises(StaleSchemaError) as ei:
        service.import_data(
            "d", [{"ts": ts(2), "x": 3.0}], batch_id="b", append=True
        )
    log.info("[stale] code=%s message=%s", ei.value.code, ei.value)
    assert ei.value.code == "stale_schema"
    # 拒绝无污染：版本不变、批次未注册
    assert service.list_versions("d") == [1, 2]
    # 刷新结构后同批次号重试成功
    r = service.import_data(
        "d",
        [{"ts": ts(2), "x": 3.0, "region": "us"}],
        batch_id="b",
        append=True,
    )
    assert r.version == 3
    v = _assert_self_consistent(service, "d")
    assert set(v.schema_.field_names()) == {"x", "region"}


def test_concurrent_appends_with_new_columns_stay_self_consistent(service):
    """多人同时追加、各自携带不同的新可选维度：任意交错后版本必须自洽。"""
    service.import_data("d", [{"ts": ts(0), "x": 1.0}], batch_id="seed")
    barrier = threading.Barrier(4)
    outcomes: dict[str, str] = {}
    lock = threading.Lock()

    def worker(i):
        # 每个追加者各带一个新维度；时间戳错开避免重复点
        new_col = f"dim{i}"
        point = {"ts": ts(1 + i), "x": float(i), new_col: f"v{i}"}
        barrier.wait()
        try:
            service.import_data("d", [point], batch_id=f"w{i}", append=True)
            outcome = "ok"
        except StaleSchemaError:
            outcome = "stale"
        with lock:
            outcomes[new_col] = outcome

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(worker, range(4)))

    log.info("[concurrent-evolve] outcomes=%s", outcomes)
    assert set(outcomes.values()) <= {"ok", "stale"}
    # 至少一个成功；最终版本自洽（声明字段 ⊇ 所有数据点的键）
    assert "ok" in outcomes.values()
    v = _assert_self_consistent(service, "d")
    # 被拒绝者刷新结构（带上当前全部列）后重试都能成功；
    # 每次重试都可能再次遇到并发演进，因此按最新 schema 重建载荷循环重试
    for i in range(4):
        new_col = f"dim{i}"
        if outcomes[new_col] != "stale":
            continue
        for attempt in range(5):
            cur = service._store.latest_version("d")
            point = {"ts": ts(1 + i), "x": float(i), new_col: f"v{i}"}
            for f in cur.schema_.fields:
                if f.name not in point and f.name != "x":
                    point[f.name] = f"v{i}" if f.type.value == "string" else 0.0
            try:
                r = service.import_data(
                    "d", [point], batch_id=f"w{i}", append=True
                )
                log.info("[retry] %s attempt=%s -> v%s", new_col, attempt, r.version)
                break
            except StaleSchemaError:
                continue
        else:
            raise AssertionError(f"{new_col} 重试多次仍被拒绝")
    v = _assert_self_consistent(service, "d")
    # 所有维度最终都进入 schema，没有任何点的字段被悄悄丢掉
    assert {f"dim{i}" for i in range(4)} <= set(v.schema_.field_names())
    assert len(v.points) == 5


def test_stale_rejection_does_not_block_other_requests(service):
    service.import_data("d", [{"ts": ts(0), "x": 1.0}], batch_id="seed")
    service.import_data(
        "d", [{"ts": ts(1), "x": 2.0, "region": "cn"}], batch_id="a", append=True
    )
    with pytest.raises(StaleSchemaError):
        service.import_data(
            "d", [{"ts": ts(2), "x": 3.0}], batch_id="stale", append=True
        )
    # 其他基于当前结构的请求照常成功
    r = service.import_data(
        "d",
        [{"ts": ts(3), "x": 4.0, "region": "jp"}],
        batch_id="c",
        append=True,
    )
    assert r.version == 3
    # 查询也正常
    q = service.query("d", "x", ts(0), ts(4), 1)
    assert q.version == 3
    _assert_self_consistent(service, "d")
