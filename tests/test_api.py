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


def test_http_grouped_query_and_missing_group(client):
    client.post("/datasets/d/import", json={
        "points": [{"ts": i * NS, "x": 10.0} for i in range(4)],
        "batch_id": "b1"})
    pts = []
    for i in range(4, 9):
        pts.append({"ts": i * NS, "x": 10.0, "dev": "a"})
        pts.append({"ts": i * NS + 1, "x": 10.0, "dev": "b"})
    pts.append({"ts": 8 * NS + 2, "x": 99.0, "dev": "a"})
    client.post("/datasets/d/import", json={
        "points": pts, "batch_id": "b2", "append": True})
    r = client.post("/query", json={
        "dataset": "d", "metric_field": "x",
        "start_ts": 0, "end_ts": 9 * NS, "window_seconds": 1,
        "group_by": "dev"})
    assert r.status_code == 200
    body = r.json()
    assert body["group_field"] == "dev"
    groups = {g["group_value"]: g for g in body["groups"]}
    assert set(groups) == {"a", "b", "__missing__"}
    assert groups["a"]["windows"][-1]["status"] == "anomaly"
    assert groups["b"]["windows"][-1]["status"] != "anomaly"
    # 可复现：同参数再查一次逐字节一致
    assert client.post("/query", json={
        "dataset": "d", "metric_field": "x",
        "start_ts": 0, "end_ts": 9 * NS, "window_seconds": 1,
        "group_by": "dev"}).json() == body


def test_http_stale_base_version_and_budget_codes(client):
    client.post("/datasets/d/import", json={
        "points": [{"ts": 0, "x": 1.0}], "batch_id": "b1"})
    client.post("/datasets/d/import", json={
        "points": [{"ts": NS, "x": 2.0}], "batch_id": "b2", "append": True})
    # 过期 base_version -> 409 stale_base_version
    r = client.post("/datasets/d/import", json={
        "points": [{"ts": 2 * NS, "x": 3.0}], "batch_id": "b3",
        "append": True, "base_version": 1})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "stale_base_version"
    # 基于最新版本重试成功
    r2 = client.post("/datasets/d/import", json={
        "points": [{"ts": 2 * NS, "x": 3.0}], "batch_id": "b3",
        "append": True, "base_version": 2})
    assert r2.status_code == 200 and r2.json()["version"] == 3


def test_http_query_budget_exceeded_is_413():
    from anomaly_service.config import ServiceConfig

    svc = AnomalyService(ServiceConfig(max_query_windows=3))
    c = TestClient(create_app(svc))
    c.post("/datasets/d/import", json={
        "points": [{"ts": i * NS, "x": 1.0} for i in range(6)],
        "batch_id": "b1"})
    r = c.post("/query", json={
        "dataset": "d", "metric_field": "x",
        "start_ts": 0, "end_ts": 6 * NS, "window_seconds": 1})
    assert r.status_code == 413
    assert r.json()["error"]["code"] == "query_budget_exceeded"
    # 预算错误与参数错误（422）状态码不同
    r2 = c.post("/query", json={
        "dataset": "d", "metric_field": "x",
        "start_ts": 6 * NS, "end_ts": 0, "window_seconds": 1})
    assert r2.status_code == 422
