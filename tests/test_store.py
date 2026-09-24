"""存储层：重复导入无副作用、乱序排序、追加、版本快照与结构演进。"""
from __future__ import annotations

import pytest

from app.errors import (
    DuplicateTimestampError,
    ServiceError,
    VersionConflictError,
    VersionNotFoundError,
)
from app.models import DataPoint, Schema


def dp(ts, v=1.0, metric="temp"):
    return DataPoint(ts, {metric: v})


def test_first_import_sorts_out_of_order_points(store):
    s = Schema.parse({"temp": "float"})
    v, created = store.import_batch(
        dataset="d", batch_id="b1", schema=s, points=[dp(2000), dp(1000)])
    assert created is True
    assert [p.timestamp for p in v.points] == [1000, 2000]
    assert v.parent_id is None


def test_duplicate_import_is_idempotent_and_side_effect_free(store):
    s = Schema.parse({"temp": "float"})
    v1, _c1 = store.import_batch(dataset="d", batch_id="b1", schema=s,
                                points=[dp(1000), dp(2000)])
    # 即使重复提交的内容不同，也必须返回首个版本，且不产生重复数据
    v2, c2 = store.import_batch(dataset="d", batch_id="b1", schema=s,
                                points=[dp(9999, 99.0)])
    assert c2 is False
    assert v2 is v1
    assert len(store.list_versions("d")) == 1
    assert [p.timestamp for p in v2.points] == [1000, 2000]
    assert store.find_by_batch("d", "b1") is v1


def test_append_merges_and_links_parent(store):
    s = Schema.parse({"temp": "float"})
    v1, _ = store.import_batch(dataset="d", batch_id="b1", schema=s, points=[dp(2000)])
    v2, created = store.import_batch(dataset="d", batch_id="b2", schema=s,
                                     points=[dp(1000), dp(3000)],
                                     base_version_id=v1.version_id)
    assert created is True
    assert v2.parent_id == v1.version_id
    assert [p.timestamp for p in v2.points] == [1000, 2000, 3000]
    # 旧快照不可变
    assert len(v1.points) == 1
    assert store.latest_version("d") is v2


def test_append_duplicate_ts_rejected_without_half_write(store):
    s = Schema.parse({"temp": "float"})
    v1, _ = store.import_batch(dataset="d", batch_id="b1", schema=s, points=[dp(2000)])
    with pytest.raises(DuplicateTimestampError) as ei:
        store.import_batch(dataset="d", batch_id="b2", schema=s,
                           points=[dp(2000, 9.0)],
                           base_version_id=v1.version_id)
    assert ei.value.details["value"] == 2000
    # 失败后状态未被污染：仍是 1 个版本，batch b2 未登记
    assert len(store.list_versions("d")) == 1
    assert store.latest_version("d") is v1
    assert store.find_by_batch("d", "b2") is None


def test_append_on_stale_version_conflicts(store):
    s = Schema.parse({"temp": "float"})
    v1, _ = store.import_batch(dataset="d", batch_id="b1", schema=s, points=[dp(1000)])
    v2, _ = store.import_batch(dataset="d", batch_id="b2", schema=s, points=[dp(2000)],
                               base_version_id=v1.version_id)
    with pytest.raises(VersionConflictError):
        store.import_batch(dataset="d", batch_id="b3", schema=s, points=[dp(3000)],
                           base_version_id=v1.version_id)
    assert store.latest_version("d") is v2


def test_schema_evolution_keeps_both_versions_reproducible(store):
    """结构演进：独立新批次导入（新 schema），新旧版本各自可复现。"""
    s1 = Schema.parse({"temp": "float"})
    v1, _ = store.import_batch(dataset="d", batch_id="b1", schema=s1, points=[dp(1000, 1.0)])
    # 新 schema 增加字段：以新批次导入（不传 base_version_id 即新批次，
    # 但同一 dataset 会被当作向最新版本追加 -> schema 不同必须被拒绝）
    s2 = Schema.parse({"temp": "float", "humidity": "float"})
    with pytest.raises(VersionConflictError) as ei:
        store.import_batch(dataset="d", batch_id="b2", schema=s2,
                           points=[DataPoint(2000, {"temp": 2.0, "humidity": 0.5})])
    assert "schema" in ei.value.message
    # 旧版本原样可复现
    assert store.latest_version("d") is v1
    assert v1.schema.fields == s1.fields

    # 独立数据集承载演进后的结构；旧版本在其数据集上永久可查
    v1b, _ = store.import_batch(dataset="d_v2", batch_id="b1", schema=s2,
                                points=[DataPoint(1000, {"temp": 1.0, "humidity": 0.1})])
    assert set(v1b.schema.fields) == {"temp", "humidity"}
    assert store.get_version("d", v1.version_id).schema.fields == s1.fields


def test_missing_dataset_and_version(store):
    with pytest.raises(VersionNotFoundError):
        store.latest_version("nope")
    s = Schema.parse({"temp": "float"})
    _v, _ = store.import_batch(dataset="d", batch_id="b", schema=s, points=[dp(1)])
    with pytest.raises(VersionNotFoundError):
        store.get_version("d", "v-does-not-exist")


def test_concurrent_imports_distinct_batches_serialized(store):
    """并发导入不同批次：全部成功且最终包含全部点、无重复。"""
    import threading

    s = Schema.parse({"temp": "float"})
    errors = []

    def worker(k):
        try:
            store.import_batch(
                dataset="d", batch_id=f"b{k}", schema=s,
                points=[dp(k * 1000, float(k))])
        except ServiceError as e:  # pragma: no cover - 仅用于记录失败
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(k,)) for k in range(1, 21)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    latest = store.latest_version("d")
    assert len(latest.points) == 20
    assert len({p.timestamp for p in latest.points}) == 20


def test_concurrent_duplicate_import_single_version(store):
    """同 batch_id 并发重复导入：只创建一个版本。"""
    import threading

    s = Schema.parse({"temp": "float"})
    results = []
    lock = threading.Lock()

    def worker():
        v, created = store.import_batch(
            dataset="d", batch_id="same", schema=s, points=[dp(1000)])
        with lock:
            results.append((v.version_id, created))

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len({vid for vid, _ in results}) == 1
    assert sum(1 for _, created in results if created) == 1
