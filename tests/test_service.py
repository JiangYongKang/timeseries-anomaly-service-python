"""服务编排：幂等导入、追加、版本切换一致读、资源边界、超时/取消/并发槽。"""
from __future__ import annotations

import asyncio
import logging

import pytest

from app.errors import (
    CancelledError_,
    DuplicateTimestampError,
    InvalidParameterError,
    InvalidTimestampError,
    ResourceLimitExceededError,
    TimeoutError_,
    VersionConflictError,
    VersionNotFoundError,
)
from app.service import AnalysisService
from tests.conftest import T0, import_body, log_report, log_snapshot

LOG = logging.getLogger("tests.service")


def _analyze(version=None, **over):
    body = {"dataset": "d", "metric": "temp", "window": "1m",
            "z_threshold": 3.0, "min_baseline": 3}
    if version:
        body["version_id"] = version
    body.update(over)
    return body


async def _import(service, bid, values, **kw):
    resp = await service.import_batch(import_body("d", bid, values), **kw)
    log_snapshot(f"import {bid}", resp)
    return resp


# ------------------------- 幂等与追加 ------------------------- #
async def test_reimport_same_batch_no_duplicate(service):
    r1 = await _import(service, "b1", [10, 10, 10, 10])
    r2 = await _import(service, "b1", [10, 10, 10, 10])
    assert r1["created"] is True
    assert r2["created"] is False and r2["duplicated"] is True
    assert r1["version"]["version_id"] == r2["version"]["version_id"]
    assert len(service.store.list_versions("d")) == 1


async def test_reimport_even_with_different_payload_returns_first(service):
    r1 = await _import(service, "b1", [1, 2])
    r2 = await _import(service, "b1", [9, 9, 9])
    assert r2["version"]["point_count"] == 2
    assert r2["version"]["version_id"] == r1["version"]["version_id"]


async def test_append_creates_child_version(service):
    r1 = await _import(service, "b1", [10, 20])           # T0, T0+1m
    body = import_body("d", "b2", [30], start=T0 + 2 * 60_000)
    body["base_version_id"] = r1["version"]["version_id"]
    r2 = await service.append_batch(body)
    assert r2["created"] is True
    assert r2["version"]["parent_id"] == r1["version"]["version_id"]
    assert r2["version"]["point_count"] == 3


async def test_append_overlapping_ts_rejected_and_clean(service):
    r1 = await _import(service, "b1", [10, 20])
    body = import_body("d", "b2", [30], start=T0)  # 与首点时间重叠
    body["base_version_id"] = r1["version"]["version_id"]
    with pytest.raises(DuplicateTimestampError) as ei:
        await service.append_batch(body)
    assert ei.value.error_code.value == "DUPLICATE_TIMESTAMP"
    assert len(service.store.list_versions("d")) == 1
    assert service.store.find_by_batch("d", "b2") is None


async def test_schema_change_via_append_rejected(service):
    await _import(service, "b1", [10])
    body = import_body("d", "b2", [{"temp": 1.0, "x": 2.0}],
                       schema={"temp": "float", "x": "float"})
    with pytest.raises(VersionConflictError) as ei:
        await service.append_batch(body)
    assert "schema" in ei.value.message


# ------------------------- 非法参数 ------------------------- #
async def test_invalid_payloads_are_distinguishable(service):
    good = [{"ts": T0, "values": {"temp": 1.0}}]

    with pytest.raises(InvalidParameterError):
        await service.import_batch({"batch_id": "b", "schema": {"temp": "float"},
                                    "points": good})  # 缺 dataset

    bad = [{"ts": "not-a-time", "values": {"temp": 1.0}}]
    with pytest.raises(InvalidTimestampError):
        await service.import_batch({"dataset": "d", "batch_id": "b",
                                    "schema": {"temp": "float"}, "points": bad})
    with pytest.raises(VersionNotFoundError):
        service.get_version_info("d")  # 失败后无半写入


async def test_analyze_rejects_bad_params(service):
    await _import(service, "b1", [10, 10, 10, 10])
    with pytest.raises(InvalidParameterError):
        await service.analyze(_analyze(window="nope"))
    with pytest.raises(InvalidParameterError):
        await service.analyze(_analyze(z_threshold=0))
    with pytest.raises(InvalidParameterError):
        await service.analyze(_analyze(metric="nope"))
    with pytest.raises(InvalidParameterError):
        await service.analyze(_analyze(start=T0))  # 只给 start 不给 end


# ------------------------- 版本切换 / 一致读 ------------------------- #
async def test_analyze_pins_snapshot_across_change(service):
    """追加新版本后：按旧 id 仍得旧结论；默认查询切到新版本。"""
    r1 = await _import(service, "b1", [10, 10, 10, 10, 100])
    rep1 = await service.analyze(_analyze(version=r1["version"]["version_id"]))
    log_report("v1", rep1)
    assert rep1.version_id == r1["version"]["version_id"]
    assert rep1.anomalies[-1].is_anomaly is True

    append_body = import_body("d", "b2", [50, 50, 50, 50, 50],
                              start=T0 + 5 * 60000)
    r2 = await service.append_batch(append_body)
    log_snapshot("append b2", r2)

    rep_old = await service.analyze(_analyze(version=r1["version"]["version_id"]))
    assert rep_old.version_id == rep1.version_id
    old_sig = [(a.window_start, a.value) for a in rep_old.aggregates]
    assert old_sig == [(a.window_start, a.value) for a in rep1.aggregates]
    old_anom = [(x.window_start, x.is_anomaly, x.reason) for x in rep_old.anomalies]
    assert old_anom == [(x.window_start, x.is_anomaly, x.reason)
                        for x in rep1.anomalies]
    log_report("old snapshot re-read", rep_old)

    rep_latest = await service.analyze(_analyze())
    assert rep_latest.version_id != rep1.version_id
    assert len(rep_latest.aggregates) == 10
    log_report("latest snapshot", rep_latest)


async def test_reproducible_conclusions(service):
    await _import(service, "b1", [10, 10, 10, 10, 100, 10])
    r1 = await service.analyze(_analyze())
    r2 = await service.analyze(_analyze())
    agg_key = lambda r: [(x.window_start, x.value, x.count) for x in r.aggregates]
    anom_key = lambda r: [(x.window_start, x.is_anomaly, x.reason)
                          for x in r.anomalies]
    assert agg_key(r1) == agg_key(r2)
    assert anom_key(r1) == anom_key(r2)
    log_report("reproducibility", r1)


# ------------------------- 并发 ------------------------- #
async def test_concurrent_duplicate_imports_single_version(service):
    async def one(_):
        return await service.import_batch(import_body("d", "same", [1, 2, 3]))
    results = await asyncio.gather(*(one(i) for i in range(20)))
    assert len({r["version"]["version_id"] for r in results}) == 1
    assert sum(1 for r in results if r["created"]) == 1


async def test_concurrent_reads_isolated_from_writes(service):
    await _import(service, "b1", [10, 10, 10, 10, 100])
    v1_id = service.store.latest_version("d").version_id

    async def reader():
        rep = await service.analyze(_analyze(version=v1_id))
        return [(a.window_start, a.value, a.count) for a in rep.aggregates]

    async def writer():
        await asyncio.sleep(0)
        body = import_body("d", "b2", [1, 2, 3, 4, 5], start=T0 + 5 * 60000)
        return await service.append_batch(body)

    before = await asyncio.gather(*(reader() for _ in range(10)))
    await writer()
    after = await asyncio.gather(*(reader() for _ in range(10)))
    assert all(len(r) == 5 for r in before + after)
    assert all(r == before[0] for r in before + after)


# ------------------------- 资源 / 超时 / 取消 ------------------------- #
async def test_batch_size_limit_rejected_cleanly():
    svc = AnalysisService(max_points_per_batch=3)
    with pytest.raises(ResourceLimitExceededError) as ei:
        await svc.import_batch(import_body("d", "b", [1, 2, 3, 4]))
    assert ei.value.details["limit"] == 3
    with pytest.raises(VersionNotFoundError):
        svc.get_version_info("d")


async def test_concurrency_slots_overflow_rejected_immediately():
    """唯一槽位被长任务占住时，第二个请求立即收到 RESOURCE_LIMIT_EXCEEDED
    （不排队、无悬挂任务）；持槽任务释放后可正常服务后续请求。"""
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocker():
        entered.set()
        await release.wait()

    svc = AnalysisService(max_concurrency=1, import_blocker=blocker)
    holder = asyncio.create_task(
        svc.import_batch(import_body("d", "held", [1, 2])))
    await entered.wait()
    try:
        with pytest.raises(ResourceLimitExceededError) as ei:
            await svc.import_batch(import_body("d2", "waiter", [1]),
                                   timeout_s=5.0)
        assert ei.value.error_code.value == "RESOURCE_LIMIT_EXCEEDED"
        # 被拒绝的请求未排队、未写入、不影响状态
        with pytest.raises(VersionNotFoundError):
            svc.get_version_info("d2")
        # 持槽任务仍在进行（未被拒绝路径干扰）
        assert not holder.done()
    finally:
        release.set()
        await holder
    assert svc.store.latest_version("d").batch_id == "held"
    assert len(svc.store.list_versions("d")) == 1

    # 槽位已释放，后续请求正常受理（无槽位泄漏）
    follow = await svc.import_batch(import_body("d3", "next", [3]))
    assert follow["created"] is True


async def test_import_timeout_is_distinguishable_and_clean():
    async def blocker():
        await asyncio.sleep(1)

    svc = AnalysisService(import_blocker=blocker)
    with pytest.raises(TimeoutError_) as ei:
        await svc.import_batch(import_body("d", "slow", [1, 2]), timeout_s=0.05)
    assert ei.value.error_code.value == "TIMEOUT"
    with pytest.raises(VersionNotFoundError):
        svc.get_version_info("d")


async def test_import_cancelled_is_distinguishable_and_clean():
    async def blocker():
        await asyncio.Event().wait()  # 永挂

    svc = AnalysisService(import_blocker=blocker)
    task = asyncio.create_task(
        svc.import_batch(import_body("d", "cancelme", [1, 2])))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(CancelledError_) as ei:
        await task
    assert ei.value.error_code.value == "CANCELLED"
    with pytest.raises(VersionNotFoundError):
        svc.get_version_info("d")


async def test_retry_after_cancel_succeeds_with_same_batch_id():
    """取消后用同一 batch_id 重试：最终恰好一个版本，数据完整。"""
    release = asyncio.Event()
    started = asyncio.Event()

    async def blocker_once():
        started.set()
        await release.wait()

    svc = AnalysisService(import_blocker=blocker_once)
    task = asyncio.create_task(
        svc.import_batch(import_body("d", "b1", [1, 2, 3])))
    await started.wait()
    task.cancel()
    with pytest.raises(CancelledError_):
        await task

    release.set()
    svc._import_blocker = None
    resp = await svc.import_batch(import_body("d", "b1", [1, 2, 3]))
    assert resp["created"] is True
    assert resp["version"]["point_count"] == 3
    assert len(svc.store.list_versions("d")) == 1


async def test_analyze_missing_version_is_not_timeout():
    svc = AnalysisService()
    with pytest.raises(VersionNotFoundError):
        await svc.analyze(_analyze())


# ------------------------- 乱序 / 顺序无关的端到端可复现 ------------------------- #
async def test_out_of_order_import_yields_identical_report(service):
    """同一份内容以乱序和有序分别导入两个数据集，分析结论逐窗口一致。"""
    values = [10, 12, 9, 11, 10, 95, 10, 11]
    ts_order = [0, 7, 3, 1, 6, 4, 2, 5]  # 乱序下标
    ordered = import_body("ord", "b", values)
    shuffled_points = [ordered["points"][i] for i in ts_order]
    shuffled = {"dataset": "shuf", "batch_id": "b",
                "schema": ordered["schema"], "points": shuffled_points}

    await service.import_batch(ordered)
    await service.import_batch(shuffled)

    body = {"metric": "temp", "window": "1m",
            "z_threshold": 3.0, "min_baseline": 2}
    r_order = await service.analyze({**body, "dataset": "ord"})
    r_shuf = await service.analyze({**body, "dataset": "shuf"})

    def sig(r):
        return [
            (a.window_start, a.value, a.count, a.missing)
            for a in r.aggregates
        ], [
            (x.window_start, x.is_anomaly, x.reason,
             None if x.evidence is None else (
                 round(x.evidence.baseline_mean, 12),
                 round(x.evidence.baseline_std, 12),
                 x.evidence.baseline_n, round(x.evidence.zscore, 12)))
            for x in r.anomalies
        ]

    a1, n1 = sig(r_order)
    a2, n2 = sig(r_shuf)
    assert a1 == a2
    assert n1 == n2
    # 且点在存储内按时间升序
    vo = service.store.latest_version("ord")
    vs = service.store.latest_version("shuf")
    assert [p.timestamp for p in vo.points] == sorted(p.timestamp for p in vo.points)
    assert [p.timestamp for p in vs.points] == [p.timestamp for p in vo.points]


async def test_empty_range_windows_never_flag(service):
    """分析范围全部为空窗口：不产生任何异常，值均为 null。"""
    await _import(service, "b1", [10, 10, 10])
    rep = await service.analyze(_analyze(
        start=T0 + 3 * 60000, end=T0 + 6 * 60000))
    assert len(rep.aggregates) == 3
    assert all(a.value is None and a.count == 0 for a in rep.aggregates)
    assert all(not x.is_anomaly and x.reason.startswith("NO_DATA")
               for x in rep.anomalies)
