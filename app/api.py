"""HTTP 接口与错误到 HTTP 状态码的映射。"""
from __future__ import annotations

from dataclasses import asdict

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.errors import ErrorCode, ServiceError
from app.models import AnalysisReport
from app.service import AnalysisService

_STATUS_MAP: dict[ErrorCode, int] = {
    ErrorCode.SCHEMA_FIELD_MISSING: 422,
    ErrorCode.SCHEMA_TYPE_MISMATCH: 422,
    ErrorCode.INVALID_TIMESTAMP: 422,
    ErrorCode.DUPLICATE_TIMESTAMP: 409,
    ErrorCode.EMPTY_BATCH: 422,
    ErrorCode.INVALID_PARAMETER: 400,
    ErrorCode.VERSION_NOT_FOUND: 404,
    ErrorCode.NO_DATA_IN_RANGE: 404,
    ErrorCode.VERSION_CONFLICT: 409,
    ErrorCode.RESOURCE_LIMIT_EXCEEDED: 413,
    ErrorCode.TIMEOUT: 504,
    ErrorCode.CANCELLED: 499,
    ErrorCode.INTERNAL_ERROR: 500,
}


def error_to_status(err: ServiceError) -> int:
    return _STATUS_MAP.get(err.error_code, 500)


def _report_to_dict(r: AnalysisReport) -> dict:
    d = asdict(r)
    # Enum -> 字符串，保证响应 JSON 稳定可读
    d["method"] = r.method.value
    for a in d["aggregates"]:
        a["method"] = r.method.value
    return d


def create_app(service: AnalysisService | None = None) -> FastAPI:
    svc = service or AnalysisService()
    app = FastAPI(title="timeseries-anomaly-service", version="0.1.0")

    @app.exception_handler(ServiceError)
    async def _handle(_request, exc: ServiceError):
        return JSONResponse(
            status_code=error_to_status(exc),
            content={
                "error_code": exc.error_code.value,
                "message": exc.message,
                "details": exc.details,
            },
        )

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    @app.post("/datasets/{dataset}/imports")
    async def import_batch(dataset: str, body: dict):
        body = dict(body or {})
        body["dataset"] = dataset
        return await svc.import_batch(body)

    @app.post("/datasets/{dataset}/appends")
    async def append_batch(dataset: str, body: dict):
        body = dict(body or {})
        body["dataset"] = dataset
        return await svc.append_batch(body)

    @app.post("/datasets/{dataset}/analyze")
    async def analyze(dataset: str, body: dict):
        body = dict(body or {})
        body["dataset"] = dataset
        report = await svc.analyze(body)
        return _report_to_dict(report)

    @app.get("/datasets/{dataset}/versions/latest")
    async def latest_version(dataset: str):
        return svc.get_version_info(dataset)

    @app.get("/datasets/{dataset}/versions/{version_id}")
    async def get_version(dataset: str, version_id: str):
        return svc.get_version_info(dataset, version_id)

    return app
