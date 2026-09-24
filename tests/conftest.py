"""共享测试夹具与辅助：单测日志打印关键输入、版本快照与判定依据。"""
from __future__ import annotations

import logging
import sys

import pytest

from app.service import AnalysisService
from app.store import VersionStore

# 让关键业务日志在单测中可见
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOG = logging.getLogger("tests")

T0 = 1577836800000  # 2020-01-01T00:00:00Z


def make_points(values, *, metric="temp", start=T0, step=60000):
    """构造 [{ts, values}] 行数据，values 可为数值或 {字段: 值}。"""
    out = []
    for i, v in enumerate(values):
        values_obj = v if isinstance(v, dict) else {metric: float(v)}
        out.append({"ts": start + i * step, "values": values_obj})
    return out


def import_body(dataset, batch_id, values, *, schema=None, metric="temp",
                start=T0, step=60000):
    return {
        "dataset": dataset,
        "batch_id": batch_id,
        "schema": schema or {metric: "float"},
        "points": make_points(values, metric=metric, start=start, step=step),
    }


def log_snapshot(tag, resp):
    """打印导入响应中的版本快照。"""
    LOG.info("[%s] version snapshot=%s", tag, resp.get("version"))


def log_report(tag, report):
    """打印分析报告关键信息与异常判定依据。"""
    anomalies = []
    for r in report.anomalies:
        anomalies.append({
            "window_start": r.window_start,
            "is_anomaly": r.is_anomaly,
            "reason": r.reason,
            "evidence": None if r.evidence is None else {
                "baseline_mean": r.evidence.baseline_mean,
                "baseline_std": r.evidence.baseline_std,
                "baseline_n": r.evidence.baseline_n,
                "threshold": r.evidence.threshold,
                "observed": r.evidence.observed,
                "zscore": r.evidence.zscore,
            },
        })
    LOG.info("[%s] report version=%s params=%s windows=%d anomalies=%d detail=%s",
             tag, report.version_id, report.params, len(report.aggregates),
             sum(1 for a in report.anomalies if a.is_anomaly), anomalies)


@pytest.fixture
def store():
    return VersionStore()


@pytest.fixture
def service():
    return AnalysisService()
