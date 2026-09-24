"""HTTP 层：错误码可区分、幂等重放、查询结论可复现。"""

from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from anomaly_service.api import create_app
from anomaly_service.service import AnomalyService

NS = 1_000_000_000


@pytest.fixture
def client():
    return TestClient(create_app(AnomalyService()))


def test_error_codes_are_distinguishable_over_http(client):
    # missing_field（批次内某点缺列）
    client.post("/datasets/d/import", json={
        "points": [{"ts": 0, "x": 1.0}], "batch_id": "b1"})
    r1 = client.post("/datasets/d/import", json={
        "points": [{"ts": NS, "x": 1.0}, {"ts": 2 * NS}], "append": True})
    assert r1.status_code == 422 and r1.json()["error"]["code"] == "missing_field"

    # type mismatch（schema 演进不兼容 -> 409）
    r2 = client.post("/datasets/d/import", json={
        "points": [{"ts": 5 * NS, "x": "bad"}], "append": True})
    assert r2.status_code == 409 and r2.json()["error"]["code"] == "incompatible_schema"

    # invalid timestamp
    r3 = client.post("/datasets/f/import", json={"points": [{"ts": "xx", "x": 1.0}]})
    assert r3.status_code == 422 and r3.json()["error"]["code"] == "invalid_timestamp"

    # dataset not found
    r4 = client.post("/query", json={
        "dataset": "nope", "metric_field": "x",
        "start_ts": 0, "end_ts": NS, "window_seconds": 1})
    assert r4.status_code == 404 and r4.json()["error"]["code"] == "dataset_not_found"

    # duplicate batch conflict
    r5 = client.post("/datasets/d/import", json={
        "points": [{"ts": 0, "x": 1.0}, {"ts": NS, "x": 2.0}], "batch_id": "b1"})
    assert r5.status_code == 409 and r5.json()["error"]["code"] == "duplicate_batch_conflict"


def test_http_idempotent_replay_and_query_evidence(client):
    pts = [{"ts": i * NS, "x": float(i)} for i in range(6)]
    a = client.post("/datasets/d/import", json={"points": pts, "batch_id": "b"})
    b = client.post("/datasets/d/import", json={"points": pts, "batch_id": "b"})
    assert a.json()["version"] == 1 and b.json()["replayed"] is True

    body = {"dataset": "d", "metric_field": "x",
            "start_ts": 0, "end_ts": 6 * NS, "window_seconds": 1}
    q1 = client.post("/query", json=body).json()
    q2 = client.post("/query", json=body).json()
    assert q1 == q2
    assert q1["version"] == 1
    for w in q1["windows"]:
        assert w["evidence"] is not None and w["evidence"]["reason"]


def test_concurrent_http_imports_single_version(client):
    pts = [{"ts": i * NS, "x": 1.0} for i in range(8)]

    def call(_):
        r = client.post("/datasets/d/import", json={"points": pts, "batch_id": "k"})
        return r.json()

    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(call, range(20)))
    assert {r["version"] for r in results} == {1}
    assert sum(1 for r in results if r["replayed"]) == 19
    assert client.get("/datasets/d/versions").json()["versions"] == [1]


def test_query_after_append_sees_new_version(client):
    client.post("/datasets/d/import", json={
        "points": [{"ts": 0, "x": 1.0}], "batch_id": "b1"})
    q_before = client.post("/query", json={
        "dataset": "d", "metric_field": "x",
        "start_ts": 0, "end_ts": 2 * NS, "window_seconds": 1}).json()
    assert q_before["version"] == 1

    client.post("/datasets/d/import", json={
        "points": [{"ts": NS, "x": 2.0}], "batch_id": "b2", "append": True})
    q_after = client.post("/query", json={
        "dataset": "d", "metric_field": "x",
        "start_ts": 0, "end_ts": 2 * NS, "window_seconds": 1}).json()
    assert q_after["version"] == 2
    assert q_after["windows"][1]["aggregate"] == 2.0
    # 固定旧版本查询保持旧结论
    q_old = client.post("/query", json={
        "dataset": "d", "metric_field": "x",
        "start_ts": 0, "end_ts": 2 * NS, "window_seconds": 1, "version": 1}).json()
    assert q_old == q_before
