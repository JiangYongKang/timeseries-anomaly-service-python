"""FastAPI HTTP 适配层。

错误分类映射：
- 409 duplicate_batch_conflict / duplicate_timestamp / dataset_exists
- 404 dataset_not_found / version_not_found
- 422 validation_*（missing_field/type_mismatch/invalid_timestamp/...）
- 413 resource_limit_exceeded
- 408 timeout / 499 cancelled
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import (
    AnomalyServiceError,
    CancelledError_,
    DatasetNotFoundError,
    ResourceLimitError,
    TimeoutError_,
    ValidationError,
    VersionNotFoundError,
)
from .models import GroupedQueryResult
from .service import AnomalyService

_NOT_FOUND = (DatasetNotFoundError, VersionNotFoundError)
_CONFLICT_CODES = {
    "duplicate_batch_conflict",
    "duplicate_timestamp",
    "dataset_exists",
    "incompatible_schema",
    "stale_base_version",
}


class ImportRequest(BaseModel):
    points: list[dict]
    batch_id: str | None = None
    append: bool = False
    base_version: int | None = Field(default=None, ge=0)
    timeout: float | None = Field(default=None, gt=0)


class QueryRequest(BaseModel):
    dataset: str
    metric_field: str
    start_ts: int
    end_ts: int
    window_seconds: int = Field(gt=0)
    version: int | None = None
    baseline_aggregate: str = "mean"
    group_by: str | None = None
    timeout: float | None = Field(default=None, gt=0)


def _status_for(exc: AnomalyServiceError) -> int:
    if isinstance(exc, TimeoutError_):
        return 408
    if isinstance(exc, CancelledError_):
        return 499
    if isinstance(exc, ResourceLimitError):
        return 413
    if isinstance(exc, _NOT_FOUND):
        return 404
    if isinstance(exc, ValidationError):
        return 409 if exc.code in _CONFLICT_CODES else 422
    return 500


def create_app(service: AnomalyService | None = None) -> FastAPI:
    service = service or AnomalyService()
    app = FastAPI(title="timeseries-anomaly-service")

    @app.exception_handler(AnomalyServiceError)
    async def handle_business_error(_request, exc: AnomalyServiceError):
        return JSONResponse(
            status_code=_status_for(exc),
            content={"error": {"code": exc.code, "message": str(exc)}},
        )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/datasets/{dataset}/import")
    def import_data(dataset: str, req: ImportRequest):
        result = service.import_data(
            dataset,
            req.points,
            batch_id=req.batch_id,
            append=req.append,
            base_version=req.base_version,
            timeout=req.timeout,
        )
        return result.__dict__

    @app.post("/query")
    def query(req: QueryRequest):
        result = service.query(
            req.dataset,
            req.metric_field,
            req.start_ts,
            req.end_ts,
            req.window_seconds,
            version=req.version,
            baseline_aggregate=req.baseline_aggregate,
            group_by=req.group_by,
            timeout=req.timeout,
        )
        return _serialize_query(result)

    @app.get("/datasets/{dataset}/versions")
    def versions(dataset: str):
        return {"dataset": dataset, "versions": service.list_versions(dataset)}

    return app


def _serialize_window(w) -> dict:
    return {
        "window_start": w.window_start,
        "window_end": w.window_end,
        "count": w.count,
        "aggregate": w.aggregate,
        "status": w.status.value,
        "evidence": None
        if w.evidence is None
        else {
            "metric": w.evidence.metric,
            "window_value": w.evidence.window_value,
            "baseline_mean": w.evidence.baseline_mean,
            "baseline_std": w.evidence.baseline_std,
            "zscore": w.evidence.zscore,
            "threshold": w.evidence.threshold,
            "reason": w.evidence.reason,
        },
    }


def _serialize_query(result) -> dict:
    if isinstance(result, GroupedQueryResult):
        return {
            "dataset": result.dataset,
            "version": result.version,
            "metric_field": result.metric_field,
            "group_field": result.group_field,
            "window_seconds": result.window_seconds,
            "start_ts": result.start_ts,
            "end_ts": result.end_ts,
            "groups": [
                {
                    "group_value": g.group_value,
                    "windows": [_serialize_window(w) for w in g.windows],
                }
                for g in result.groups
            ],
        }
    return {
        "dataset": result.dataset,
        "version": result.version,
        "metric_field": result.metric_field,
        "window_seconds": result.window_seconds,
        "start_ts": result.start_ts,
        "end_ts": result.end_ts,
        "windows": [_serialize_window(w) for w in result.windows],
    }
