"""重复导入：同一批次不产生重复数据/副作用，失败原因可区分。"""

from anomaly_service.errors import (
    DuplicateBatchError,
    DuplicatePointError,
    InvalidTimestampError,
    MissingFieldError,
    SchemaEvolutionError,
)

from .conftest import log_import, log_snapshot, ts

BATCH = [{"ts": ts(i), "x": float(i)} for i in range(5)]


def test_same_batch_id_is_idempotent(service):
    r1 = service.import_data("d", BATCH, batch_id="b1")
    log_import("first", BATCH, r1)
    assert r1.version == 1 and not r1.replayed

    r2 = service.import_data("d", BATCH, batch_id="b1")
    log_import("replay", BATCH, r2)
    assert r2.replayed is True and r2.version == 1
    assert service.list_versions("d") == [1]
    log_snapshot(service, "d", 1)
    assert len(service._store.get_version("d", 1).points) == 5


def test_content_hash_idempotency_without_batch_id(service):
    r1 = service.import_data("c", BATCH)
    r2 = service.import_data("c", BATCH)
    assert r1.replayed is False and r2.replayed is True
    assert r1.version == r2.version == 1


def test_same_batch_id_different_payload_rejected(service):
    service.import_data("d", BATCH, batch_id="b1")
    try:
        service.import_data("d", [{"ts": ts(99), "x": 1.0}], batch_id="b1")
    except DuplicateBatchError as exc:
        assert exc.code == "duplicate_batch_conflict"
    else:
        raise AssertionError("应拒绝同批次号不同载荷")
    assert service.list_versions("d") == [1]


def test_recreate_without_append_flag_rejected(service):
    service.import_data("d", BATCH, batch_id="b1")
    try:
        service.import_data("d", BATCH, batch_id="other")
    except DuplicateBatchError as exc:
        assert exc.code == "dataset_exists"
    else:
        raise AssertionError


def test_append_overlapping_timestamps_rejected(service):
    service.import_data("d", BATCH, batch_id="b1")
    overlap = [{"ts": ts(2), "x": 2.0}, {"ts": ts(10), "x": 10.0}]
    try:
        service.import_data("d", overlap, batch_id="b2", append=True)
    except DuplicatePointError as exc:
        assert exc.code == "duplicate_timestamp"
    else:
        raise AssertionError
    # 状态未污染：仍只有 v1、5 个点
    assert service.list_versions("d") == [1]
    assert len(service._store.get_version("d", 1).points) == 5


def test_invalid_payloads_have_distinguishable_codes(service):
    try:
        service.import_data("m", [{"ts": ts(0), "x": 1.0}, {"ts": ts(1)}])
    except MissingFieldError as exc:
        assert exc.code == "missing_field"
    else:
        raise AssertionError

    try:
        service.import_data("t", [{"ts": "not-a-time", "x": 1.0}])
    except InvalidTimestampError as exc:
        assert exc.code == "invalid_timestamp"
    else:
        raise AssertionError

    service.import_data("s", [{"ts": ts(0), "x": 1}], batch_id="s1")
    try:
        service.import_data(
            "s", [{"ts": ts(1), "x": "str"}], batch_id="s2", append=True
        )
    except SchemaEvolutionError as exc:
        assert exc.code == "incompatible_schema"
    else:
        raise AssertionError


def test_int_float_widening_and_new_nullable_column(service):
    service.import_data("d", [{"ts": ts(0), "x": 1}], batch_id="b1")
    # int -> float 拓宽允许；新增可空列 y
    r = service.import_data(
        "d",
        [{"ts": ts(1), "x": 2.5, "y": "tag"}],
        batch_id="b2",
        append=True,
    )
    assert r.version == 2
    log_snapshot(service, "d", 1)
    log_snapshot(service, "d", 2)
    # 旧版本 schema 不含 y；新版本含且历史点缺 y 读取为 None
    assert service._store.get_version("d", 1).schema_.get("y") is None
    assert service._store.get_version("d", 2).schema_.get("y") is not None
    old_points = service._store.get_version("d", 1).points
    assert "y" not in old_points[0].values
