"""HTTP 接口：状态码映射、端到端导入/查询、错误分类在响应体中可区分。"""
from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.service import AnalysisService
from tests.conftest import T0, make_points

LOG = logging.getLogger("tests.api")


@pytest.fixture
def client():
    service = AnalysisService()
    return TestClient(create_app(service)), service


def _import(client_, dataset, bid, values, **extra):
    body = {"batch_id": bid, "schema": {"temp": "float"},
            "points": make_points(values), **extra}
    return client_.post(f"/datasets/{dataset}/imports", json=body)


def test_health(client):
    c, _ = client
    assert c.get("/health").json() == {"status": "ok"}


def test_import_then_analyze_end_to_end(client):
    c, _service = client
    r = _import(c, "d", "b1", [10, 10, 10, 10, 100])
    assert r.status_code == 200, r.text
    vid = r.json()["version"]["version_id"]
    LOG.info("API import snapshot=%s", r.json()["version"])

    r = c.post(f"/datasets/{'d'}/analyze",
               json={"metric": "temp", "window": "1m",
                     "z_threshold": 3.0, "min_baseline": 3})
    assert r.status_code == 200, r.text
    rep = r.json()
    assert rep["version_id"] == vid
    assert len(rep["aggregates"]) == 5
    anomalies = [a for a in rep["anomalies"] if a["is_anomaly"]]
    assert len(anomalies) == 1
    spike = anomalies[0]
    assert spike["evidence"]["baseline_mean"] == 10.0
    assert spike["evidence"]["observed"] == 100.0
    LOG.info("API anomaly window=%s reason=%s evidence=%s",
             spike["window_start"], spike["reason"], spike["evidence"])


def test_duplicate_import_returns_200_and_flag(client):
    c, _ = client
    r1 = _import(c, "d", "b1", [1, 2])
    r2 = _import(c, "d", "b1", [1, 2])
    assert r1.status_code == r2.status_code == 200
    assert r1.json()["created"] is True
    assert r2.json()["created"] is False
    assert r2.json()["duplicated"] is True


def test_error_codes_map_to_distinguishable_http_responses(client):
    c, _ = client
    # 字段缺失 -> 422 SCHEMA_FIELD_MISSING
    r = c.post("/datasets/d/imports", json={
        "batch_id": "b", "schema": {"temp": "float"},
        "points": [{"ts": T0, "values": {}}]})
    assert r.status_code == 422
    assert r.json()["error_code"] == "SCHEMA_FIELD_MISSING"

    # 类型不匹配 -> 422 SCHEMA_TYPE_MISMATCH
    r = c.post("/datasets/d/imports", json={
        "batch_id": "b", "schema": {"temp": "float"},
        "points": [{"ts": T0, "values": {"temp": "x"}}]})
    assert r.json()["error_code"] == "SCHEMA_TYPE_MISMATCH"

    # 非法时间 -> 422 INVALID_TIMESTAMP
    r = c.post("/datasets/d/imports", json={
        "batch_id": "b", "schema": {"temp": "float"},
        "points": [{"ts": -1, "values": {"temp": 1.0}}]})
    assert r.status_code == 422
    assert r.json()["error_code"] == "INVALID_TIMESTAMP"

    # 非法参数 -> 400 INVALID_PARAMETER
    r = c.post("/datasets/d/imports", json={
        "schema": {"temp": "float"},
        "points": [{"ts": T0, "values": {"temp": 1.0}}]})
    assert r.status_code == 400
    assert r.json()["error_code"] == "INVALID_PARAMETER"

    # 版本不存在 -> 404
    r = c.get("/datasets/nope/versions/latest")
    assert r.status_code == 404
    assert r.json()["error_code"] == "VERSION_NOT_FOUND"


def test_duplicate_timestamp_conflict_is_409(client):
    c, _ = client
    r = c.post("/datasets/d/imports", json={
        "batch_id": "b", "schema": {"temp": "float"},
        "points": [{"ts": T0, "values": {"temp": 1.0}},
                   {"ts": T0, "values": {"temp": 2.0}}]})
    assert r.status_code == 409
    assert r.json()["error_code"] == "DUPLICATE_TIMESTAMP"


def test_version_switch_over_http_is_consistent(client):
    c, _ = client
    vid1 = _import(c, "d", "b1", [10, 10, 10, 10, 100]).json()["version"]["version_id"]
    c.post("/datasets/d/appends", json={
        "batch_id": "b2", "schema": {"temp": "float"},
        "base_version_id": vid1,
        "points": make_points([50, 50, 50, 50, 50], start=T0 + 5 * 60000)})

    old = c.post("/datasets/d/analyze",
                 json={"metric": "temp", "window": "1m",
                       "version_id": vid1}).json()
    latest = c.post("/datasets/d/analyze",
                    json={"metric": "temp", "window": "1m"}).json()
    assert old["version_id"] == vid1
    assert len(old["aggregates"]) == 5
    assert len(latest["aggregates"]) == 10

    info = c.get(f"/datasets/d/versions/{vid1}").json()
    assert info["version_id"] == vid1 and info["point_count"] == 5
