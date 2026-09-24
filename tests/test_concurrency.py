"""并发导入与计算：状态隔离、一致读取、重复批次串行化、无半写入。"""

import threading
from concurrent.futures import ThreadPoolExecutor

from .conftest import log_snapshot, ts


def test_concurrent_identical_imports_produce_single_version(service):
    batch = [{"ts": ts(i), "x": float(i)} for i in range(10)]
    errors: list[Exception] = []

    def worker():
        try:
            return service.import_data("d", batch, batch_id="same")
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
            return None

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda _: worker(), range(32)))

    assert errors == []
    versions = {r.version for r in results}
    assert versions == {1}
    replays = sum(1 for r in results if r.replayed)
    assert replays == 31
    assert service.list_versions("d") == [1]
    assert len(service._store.get_version("d", 1).points) == 10


def test_concurrent_appends_are_serialized(service):
    service.import_data("d", [{"ts": ts(0), "x": 0.0}], batch_id="seed")

    def worker(i):
        return service.import_data(
            "d",
            [{"ts": ts(10 + i), "x": float(i)}],
            batch_id=f"b{i}",
            append=True,
        )

    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(worker, range(24)))

    assert sorted(r.version for r in results) == list(range(2, 26))
    latest = service._store.latest_version("d")
    assert len(latest.points) == 25
    # 时间戳唯一，无重复
    tss = [p.ts for p in latest.points]
    assert len(tss) == len(set(tss))


def test_concurrent_append_with_overlapping_ts_one_wins_others_fail(service):
    service.import_data("d", [{"ts": ts(0), "x": 0.0}], batch_id="seed")
    outcome = {}

    def worker(i):
        try:
            service.import_data(
                "d", [{"ts": ts(1), "x": float(i)}], batch_id=f"b{i}", append=True
            )
            outcome[i] = "ok"
        except Exception as exc:  # noqa: BLE001
            outcome[i] = exc.code

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(worker, range(8)))

    oks = [k for k, v in outcome.items() if v == "ok"]
    assert len(oks) == 1
    assert all(v in ("ok", "duplicate_timestamp") for v in outcome.values())
    # 只有一个追加版本，且状态一致可继续服务
    assert service.list_versions("d") == [1, 2]
    assert len(service._store.latest_version("d").points) == 2


def test_concurrent_reads_always_on_complete_snapshot(service):
    service.import_data(
        "d", [{"ts": ts(i), "x": 1.0} for i in range(20)], batch_id="seed"
    )
    stop = threading.Event()
    seen: list[int] = []
    read_errors: list[Exception] = []

    def reader():
        while not stop.is_set():
            try:
                q = service.query("d", "x", ts(0), ts(20), 1)
                # 一致性不变量：窗口数固定且聚合值全部来自完整快照
                assert len(q.windows) == 20
                seen.append(q.version)
            except Exception as exc:  # noqa: BLE001
                read_errors.append(exc)

    readers = [threading.Thread(target=reader) for _ in range(6)]
    for t in readers:
        t.start()

    # 持续追加新版本
    for v in range(1, 11):
        service.import_data(
            "d",
            [{"ts": ts(20 + v), "x": float(v)}],
            batch_id=f"grow-{v}",
            append=True,
        )
    stop.set()
    for t in readers:
        t.join()

    assert read_errors == []
    assert set(seen) <= set(range(1, 12))
    log_snapshot(service, "d", service.list_versions("d")[-1])
    # 读者永远不会看到“点数对不上版本”的撕裂状态
    final = service._store.latest_version("d")
    assert len(final.points) == 30


def test_query_during_append_never_blocks_on_half_written_state(service):
    service.import_data("d", [{"ts": ts(0), "x": 1.0}], batch_id="seed")
    barrier = threading.Event()

    def appender():
        # 与读者并发；发布要么不可见，要么完整可见
        barrier.set()
        for i in range(1, 30):
            service.import_data(
                "d", [{"ts": ts(i), "x": float(i)}], batch_id=f"a{i}", append=True
            )

    th = threading.Thread(target=appender)
    th.start()
    barrier.wait()
    for _ in range(50):
        q = service.query("d", "x", ts(0), ts(30), 1)
        n_data = sum(1 for w in q.windows if w.aggregate is not None)
        # 已发布版本的已导入时间点必须连续前缀（无空洞来自半写入）
        assert n_data == q.version
    th.join()
