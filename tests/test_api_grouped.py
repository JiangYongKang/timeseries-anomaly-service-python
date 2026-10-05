"""HTTP 层：分组查询端点、stale_schema / query_budget_exceeded 状态码映射。"""

import pytest
from fastapi.testclient import TestClient

from anomaly_service.api import create_app
from anomaly_service.config import ServiceConfig
from anomaly_service.service import AnomalyService

from .conftest import log, ts


@pytest.fixture
def client():
    return TestClient(create_app(AnomalyService()))


def _seed(client):
    pts = []
    for i in range(5):
        pts.append({"ts": ts(i), "x": 10.0, "dev": "a"})
        pts.append({"ts": ts(i) + 1, "x": 20.0, "dev": "b"})
    pts.append({"ts": ts(5), "x": 99.0, "dev": "a"})
    r = client.post("/datasets/d/import", json={"points": pts, "batch_id": "seed"})
    assert r.status_code == 200, r.json()


def test_grouped_query_endpoint(client):
    _seed(client)
    r = client.post("/query/grouped", json={
        "dataset": "d", "metric_field": "x", "group_field": "dev",
        "start_ts": 0, "end_ts": ts(6), "window_seconds": 1})
    assert r.status_code == 200, r.json()
    body = r.json()
    log.info("[http-grouped] groups=%s", [g["group_key"] for g in body["groups"]])
    by_key = {g["group_key"]: g for g in body["groups"]}
    assert by_key["a"]["windows"][-1]["status"] == "anomaly"
    assert by_key["b"]["windows"][-1]["status"] == "no_data"
    # 同参数再查一次，响应逐字节相等（可复现）
    r2 = client.post("/query/grouped", json={
        "dataset": "d", "metric_field": "x", "group_field": "dev",
        "start_ts": 0, "end_ts": ts(6), "window_seconds": 1})
    assert r2.json() == body


def test_stale_schema_maps_to_409(client):
    client.post("/datasets/d/import", json={
        "points": [{"ts": 0, "x": 1.0}], "batch_id": "b1"})
    client.post("/datasets/d/import", json={
        "points": [{"ts": ts(1), "x": 2.0, "region": "cn"}],
        "batch_id": "b2", "append": True})
    r = client.post("/datasets/d/import", json={
        "points": [{"ts": ts(2), "x": 3.0}], "batch_id": "b3", "append": True})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "stale_schema"
    log.info("[http-stale] body=%s", r.json())


def test_query_budget_maps_to_413():
    svc = AnomalyService(ServiceConfig(max_query_windows=3))
    client = TestClient(create_app(svc))
    client.post("/datasets/d/import", json={
        "points": [{"ts": ts(i), "x": 1.0} for i in range(10)],
        "batch_id": "b1"})
    r = client.post("/query", json={
        "dataset": "d", "metric_field": "x",
        "start_ts": 0, "end_ts": ts(10), "window_seconds": 1})
    assert r.status_code == 413
    assert r.json()["error"]["code"] == "query_budget_exceeded"
    log.info("[http-budget] body=%s", r.json())
    # 参数写错仍是 422，与预算超限（413）明确区分
    r2 = client.post("/query", json={
        "dataset": "d", "metric_field": "x",
        "start_ts": ts(10), "end_ts": 0, "window_seconds": 1})
    assert r2.status_code == 422
