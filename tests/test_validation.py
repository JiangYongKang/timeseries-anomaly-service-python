"""字段缺失/类型不匹配/非法时间/重复时间点：错误原因必须可区分。"""
from __future__ import annotations

import pytest

from app.errors import (
    DuplicateTimestampError,
    EmptyBatchError,
    FieldMissingError,
    InvalidParameterError,
    InvalidTimestampError,
    TypeMismatchError,
)
from app.models import Schema
from app.validation import validate_batch, validate_schema

SCHEMA = {"temp": "float", "cnt": "int"}
T = 1577836800000


def _row(ts=T, temp=1.0, cnt=2, *, drop=(), values_extra=None):
    values = {"temp": temp, "cnt": cnt}
    for k in drop:
        values.pop(k, None)
    if values_extra:
        values.update(values_extra)
    row = {"ts": ts, "values": values}
    if "ts" in drop:
        row.pop("ts")
    if "values" in drop:
        row.pop("values")
    return row


def test_schema_parse_rejects_bad_type():
    with pytest.raises(InvalidParameterError):
        validate_schema({"x": "bool"})
    with pytest.raises(InvalidParameterError):
        validate_schema({})


def test_empty_batch_rejected():
    s = Schema.parse(SCHEMA)
    with pytest.raises(EmptyBatchError) as ei:
        validate_batch([], s)
    assert ei.value.error_code.value == "EMPTY_BATCH"


def test_missing_ts_field_distinguishable():
    s = Schema.parse(SCHEMA)
    with pytest.raises(FieldMissingError) as ei:
        validate_batch([_row(drop=("ts",))], s)
    assert ei.value.error_code.value == "SCHEMA_FIELD_MISSING"
    assert ei.value.details["field"] == "ts"
    assert ei.value.details["index"] == 0


def test_missing_metric_field_distinguishable():
    s = Schema.parse(SCHEMA)
    with pytest.raises(FieldMissingError) as ei:
        validate_batch([_row(drop=("cnt",))], s)
    assert ei.value.details["field"] == "cnt"


def test_bad_timestamp_type():
    s = Schema.parse(SCHEMA)
    with pytest.raises(InvalidTimestampError) as ei:
        validate_batch([_row(ts="2020-01-01")], s)
    assert ei.value.error_code.value == "INVALID_TIMESTAMP"


def test_timestamp_out_of_range():
    s = Schema.parse(SCHEMA)
    with pytest.raises(InvalidTimestampError):
        validate_batch([_row(ts=123)], s)


def test_bool_is_not_a_number():
    s = Schema.parse(SCHEMA)
    with pytest.raises(TypeMismatchError):
        validate_batch([_row(temp=True)], s)


def test_type_mismatch_string_value():
    s = Schema.parse(SCHEMA)
    with pytest.raises(TypeMismatchError) as ei:
        validate_batch([_row(temp="hot")], s)
    assert ei.value.error_code.value == "SCHEMA_TYPE_MISMATCH"
    assert ei.value.details["field"] == "temp"


def test_int_field_rejects_fraction():
    s = Schema.parse(SCHEMA)
    with pytest.raises(TypeMismatchError) as ei:
        validate_batch([_row(cnt=2.5)], s)
    assert ei.value.details["field"] == "cnt"


def test_nan_inf_rejected():
    s = Schema.parse({"temp": "float"})
    for bad in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(TypeMismatchError):
            validate_batch([{"ts": T, "values": {"temp": bad}}], s)


def test_duplicate_timestamp_within_batch():
    s = Schema.parse(SCHEMA)
    with pytest.raises(DuplicateTimestampError) as ei:
        validate_batch([_row(ts=T), _row(ts=T, temp=2.0)], s)
    assert ei.value.error_code.value == "DUPLICATE_TIMESTAMP"
    assert ei.value.details["value"] == T


def test_validation_is_all_or_nothing():
    """第二行非法时，第一行也不得留下任何东西（纯函数，直接断言抛错即可）。"""
    s = Schema.parse(SCHEMA)
    with pytest.raises((FieldMissingError, TypeMismatchError,
                        InvalidTimestampError, DuplicateTimestampError)):
        validate_batch([_row(), _row(ts="bad")], s)


def test_valid_batch_normalizes_to_float_and_accepts_unordered():
    s = Schema.parse(SCHEMA)
    pts = validate_batch([_row(ts=T + 60000), _row(ts=T)], s)
    assert [p.timestamp for p in pts] == [T + 60000, T]  # 不排序，排序归存储层
    assert pts[0].values["cnt"] == 2.0
