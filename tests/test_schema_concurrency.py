"""多方并发追加下的结构演进：版本自洽、过期请求明确拒绝、拒绝后可重试。"""

from concurrent.futures import ThreadPoolExecutor

import pytest

from anomaly_service.errors import SchemaEvolutionError, StaleSchemaError

from .conftest import log, log_import, log_snapshot, ts


def test_concurrent_appends_with_new_optional_dimensions_stay_consistent(service):
    """两个任务各带一个新的可选维度并发追加：最终版本字段与数据完全对应。"""
    service.import_data("d", [{"ts": ts(0), "x": 1.0}], batch_id="seed")

    def worker_a():
        return service.import_data(
            "d",
            [{"ts": ts(1), "x": 2.0, "dev": "a1"}],
            batch_id="wa",
            append=True,
        )

    def worker_b():
        return service.import_data(
            "d",
            [{"ts": ts(2), "x": 3.0, "region": "cn"}],
            batch_id="wb",
            append=True,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        ra = pool.submit(worker_a)
        rb = pool.submit(worker_b)
        results = [ra.result(), rb.result()]

    for r in results:
        log_import("concurrent-evolve", [], r)
    # 两个追加都成功（可空列允许省略），版本各一
    assert sorted(r.version for r in results) == [2, 3]
    latest = service._store.latest_version("d")
    log_snapshot(service, "d", latest.version)
    # 最终版本自洽：dev 与 region 都在声明的 schema 中
    names = latest.schema_.field_names()
    assert set(names) == {"x", "dev", "region"}
    assert latest.schema_.get("dev").nullable
    assert latest.schema_.get("region").nullable
    # 每个已发布版本：数据点里出现的键必须被该版本 schema 声明
    for v in service.list_versions("d"):
        snap = service._store.get_version("d", v)
        declared = set(snap.schema_.field_names())
        for p in snap.points:
            assert set(p.values) <= declared, (
                f"v{v} 存在未声明字段: {set(p.values) - declared}"
            )
    # 缺列的点读取为 None，不丢字段也不误报
    r = service.query("d", "x", ts(0), ts(3), 1, group_by="dev")
    assert any(g.group_value == "__missing__" for g in r.groups)


def test_interleaved_evolution_any_order_same_final_schema(service):
    """两种到达顺序产生的最终 schema 等价（字段集合与可空性一致）。"""
    finals = []
    for order in ("ab", "ba"):
        dataset = f"d-{order}"
        service.import_data(dataset, [{"ts": ts(0), "x": 1.0}], batch_id="seed")
        seq = {
            "a": [{"ts": ts(1), "x": 2.0, "dev": "a1"}],
            "b": [{"ts": ts(2), "x": 3.0, "region": "cn"}],
        }
        for key in order:
            service.import_data(dataset, seq[key], batch_id=f"w{key}", append=True)
        latest = service._store.latest_version(dataset)
        finals.append(
            {f.name: (f.type.value, f.nullable) for f in latest.schema_.fields}
        )
        log_snapshot(service, dataset, latest.version)
    assert finals[0] == finals[1]


def test_stale_base_version_rejected_and_retry_succeeds(service):
    service.import_data("d", [{"ts": ts(0), "x": 1.0}], batch_id="seed")
    service.import_data(
        "d", [{"ts": ts(1), "x": 2.0}], batch_id="b2", append=True
    )
    # 基于 v1 的过期追加：清楚且可区分的拒绝
    with pytest.raises(StaleSchemaError) as ei:
        service.import_data(
            "d",
            [{"ts": ts(2), "x": 3.0}],
            batch_id="b3",
            append=True,
            base_version=1,
        )
    log.info("[stale] code=%s message=%s", ei.value.code, ei.value)
    assert ei.value.code == "stale_base_version"
    # 拒绝不产生任何状态：版本未变、批次未登记
    assert service.list_versions("d") == [1, 2]
    # 基于最新版本重试同一批次号即可成功
    r = service.import_data(
        "d",
        [{"ts": ts(2), "x": 3.0}],
        batch_id="b3",
        append=True,
        base_version=2,
    )
    assert r.version == 3
    # 其他不带 base_version 的请求完全不受影响
    r2 = service.import_data(
        "d", [{"ts": ts(3), "x": 4.0}], batch_id="b4", append=True
    )
    assert r2.version == 4


def test_concurrent_stale_and_fresh_appends_loser_rejected(service):
    """并发下只有一个基于同一 base_version 的追加能赢，输家明确失败。"""
    service.import_data("d", [{"ts": ts(0), "x": 1.0}], batch_id="seed")
    outcome = {}

    def worker(i):
        try:
            service.import_data(
                "d",
                [{"ts": ts(1 + i), "x": float(i)}],
                batch_id=f"c{i}",
                append=True,
                base_version=1,
            )
            outcome[i] = "ok"
        except StaleSchemaError:
            outcome[i] = "stale"

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(worker, range(8)))

    log.info("[optimistic] outcome=%s", outcome)
    assert list(outcome.values()).count("ok") == 1
    assert list(outcome.values()).count("stale") == 7
    # 赢家发布 v2；输家重试（基于 v2）后全部成功且版本连续
    assert service.list_versions("d") == [1, 2]
    for i in range(8):
        if outcome[i] == "stale":
            service.import_data(
                "d",
                [{"ts": ts(1 + i), "x": float(i)}],
                batch_id=f"c{i}",
                append=True,
            )
    assert service.list_versions("d") == list(range(1, 10))
    latest = service._store.latest_version("d")
    assert len(latest.points) == 9


def test_missing_required_field_still_rejected_under_lock(service):
    """锁内合并仍拒绝省略必填列的追加，且不污染状态。"""
    service.import_data(
        "d", [{"ts": ts(0), "x": 1.0, "y": 2.0}], batch_id="seed"
    )
    with pytest.raises(SchemaEvolutionError) as ei:
        service.import_data(
            "d", [{"ts": ts(1), "x": 3.0}], batch_id="bad", append=True
        )
    assert ei.value.code == "incompatible_schema"
    assert service.list_versions("d") == [1]
    # 拒绝后正常追加（省略可空列之外、包含全部必填列）仍可进行
    service.import_data(
        "d",
        [{"ts": ts(1), "x": 3.0, "y": 4.0, "tag": "t"}],
        batch_id="good",
        append=True,
    )
    latest = service._store.latest_version("d")
    assert set(latest.schema_.field_names()) == {"x", "y", "tag"}
