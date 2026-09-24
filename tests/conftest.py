"""测试公共工具：关键输入、版本快照、判定依据统一打印到单测日志。"""

from __future__ import annotations

import logging

import pytest

from anomaly_service.service import AnomalyService

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("anomaly-tests")

NS = 1_000_000_000


def ts(second: int) -> int:
    return second * NS


@pytest.fixture
def service() -> AnomalyService:
    return AnomalyService()


def log_import(title: str, raw_points, result) -> None:
    log.info(
        "[%s] input_points=%s -> version=v%s appended=%s count=%s replayed=%s",
        title,
        raw_points,
        result.version,
        result.appended,
        result.point_count,
        result.replayed,
    )


def log_snapshot(service: AnomalyService, dataset: str, version: int) -> None:
    snap = service._store.get_version(dataset, version)
    log.info(
        "[snapshot] %s v%s schema=%s points=%s",
        dataset,
        version,
        snap.schema_.field_names(),
        [(p.ts, p.values) for p in snap.points],
    )


def log_query(title: str, result) -> None:
    log.info(
        "[%s] version=v%s field=%s window=%ss range=(%s,%s)",
        title,
        result.version,
        result.metric_field,
        result.window_seconds,
        result.start_ts,
        result.end_ts,
    )
    for w in result.windows:
        ev = w.evidence
        log.info(
            "  window=[%s,%s) count=%s agg=%s status=%s z=%s reason=%s",
            w.window_start,
            w.window_end,
            w.count,
            w.aggregate,
            w.status.value,
            None if ev is None else ev.zscore,
            None if ev is None else ev.reason,
        )
