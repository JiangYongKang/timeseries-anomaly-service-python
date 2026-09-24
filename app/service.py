"""服务编排层：请求解析、资源边界、并发隔离、超时/取消与一致读取。

并发与资源语义
--------------
- 每个请求必须先获得全局并发槽（asyncio.Semaphore）；槽满立即拒绝
  （RESOURCE_LIMIT_EXCEEDED），不排队、不留任务。
- 超时：使用 ``asyncio.timeout``。超时或客户端取消时明确失败为
  TIMEOUT / CANCELLED。写操作在锁内原子提交且按 batch_id 幂等：
  调用方拿不到结果即可用同一 batch_id 安全重试，不会产生半写入或重复版本。
- 计算任务在线程池执行；任务在获得槽位之前被取消时，未触碰任何状态。
- 所有读取都基于一次解析得到的**不可变版本快照**，计算期间数据即使发生变化，
  当前结论仍绑定原版本（报告内含 version_id 与全部参数），可复现可解释。
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from app.anomaly import detect
from app.errors import (
    CancelledError_,
    InvalidParameterError,
    ResourceLimitExceededError,
    TimeoutError_,
)
from app.models import AggMethod, AnalysisReport
from app.store import VersionStore
from app.validation import validate_batch, validate_schema
from app.windows import build_aggregates, default_range, parse_interval, parse_window

logger = logging.getLogger("anomaly_service")


def _version_digest(v) -> dict:
    """版本快照摘要，用于日志与可解释响应。"""
    tr = v.time_range
    return {
        "version_id": v.version_id,
        "parent_id": v.parent_id,
        "batch_id": v.batch_id,
        "point_count": len(v.points),
        "time_range": None if tr is None else {"start": tr[0], "end": tr[1]},
        "schema": {k: f.value for k, f in v.schema.fields.items()},
    }


class AnalysisService:
    def __init__(
        self,
        store: VersionStore | None = None,
        *,
        max_points_per_batch: int = 1_000_000,
        max_points_per_query: int = 5_000_000,
        max_concurrency: int = 16,
        default_timeout_s: float = 10.0,
        import_blocker: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.store = store or VersionStore()
        self.max_points_per_batch = max_points_per_batch
        self.max_points_per_query = max_points_per_query
        self.max_concurrency = max_concurrency
        self._slots = asyncio.Semaphore(max_concurrency)
        self.default_timeout_s = default_timeout_s
        # 测试/扩展钩子：在提交前阻塞（默认 None）
        self._import_blocker = import_blocker

    async def _take_slot(self) -> None:
        """非阻塞获取并发槽；槽满立即失败（不排队、不留等待者）。

        Python 3.14 的 ``Semaphore`` 移除了 ``acquire_nowait``。这里先看
        ``locked()``：为真即槽满，立刻抛 RESOURCE_LIMIT_EXCEEDED；
        否则直接 ``await acquire()``。由于 ``locked()`` 与 ``acquire()`` 之间
        没有任何 await 点（同一事件循环线程内不会被其他协程插入），许可不会被
        抢走，因此该 acquire 立即完成、不会挂起。
        """
        if self._slots.locked():
            raise ResourceLimitExceededError(
                "服务并发槽已满，请稍后重试",
                details={"max_concurrency": self.max_concurrency},
            )
        await self._slots.acquire()

    # ------------------------------------------------------------------ #
    # 公共 API
    # ------------------------------------------------------------------ #
    async def import_batch(self, request: dict, *, timeout_s: float | None = None) -> dict:
        """导入一个新数据集批次（幂等）。"""
        return await self._do_import(request, append=False, timeout_s=timeout_s)

    async def append_batch(self, request: dict, *, timeout_s: float | None = None) -> dict:
        """向数据集最新版本追加批次（幂等）。"""
        return await self._do_import(request, append=True, timeout_s=timeout_s)

    async def analyze(self, request: dict, *, timeout_s: float | None = None) -> AnalysisReport:
        """在某个不可变版本快照上执行窗口聚合 + 异常检测。"""
        timeout_s = self._resolve_timeout(timeout_s)

        # 1) 参数解析（轻量，在获取槽位前完成，非法参数快速失败）
        parsed = self._parse_analyze_request(request)
        dataset = parsed["dataset"]
        version_id = parsed["version_id"]
        snapshot = (
            self.store.get_version(dataset, version_id)
            if version_id
            else self.store.latest_version(dataset)
        )
        logger.info("analyze 解析完成 dataset=%s snapshot=%s params=%s",
                    dataset, _version_digest(snapshot),
                    {k: v for k, v in parsed.items() if k not in ("dataset", "version_id")})

        # 2) 资源边界 + 一致快照读取 + 并发槽 + 超时/取消
        if len(snapshot.points) > self.max_points_per_query:
            raise ResourceLimitExceededError(
                f"查询扫描点数 {len(snapshot.points)} 超过上限 "
                f"{self.max_points_per_query}",
                details={"points": len(snapshot.points),
                         "limit": self.max_points_per_query},
            )

        await self._take_slot()
        try:
            try:
                async with asyncio.timeout(timeout_s):
                    report = await asyncio.to_thread(self._compute, snapshot, parsed)
            except TimeoutError:
                raise TimeoutError_(
                    f"分析超过 {timeout_s}s 被终止",
                    details={"timeout_s": timeout_s,
                             "version_id": snapshot.version_id},
                ) from None
            except asyncio.CancelledError:
                raise CancelledError_("分析任务被取消") from None
            return report
        finally:
            self._slots.release()

    def get_version_info(self, dataset: str, version_id: str | None = None) -> dict:
        v = (
            self.store.get_version(dataset, version_id)
            if version_id
            else self.store.latest_version(dataset)
        )
        return _version_digest(v)

    # ------------------------------------------------------------------ #
    # 导入
    # ------------------------------------------------------------------ #
    async def _do_import(self, request: dict, *, append: bool,
                         timeout_s: float | None) -> dict:
        timeout_s = self._resolve_timeout(timeout_s)

        # 1) 解析与校验：全有或全无，非法时不动任何状态
        parsed = self._parse_import_request(request)
        dataset = parsed["dataset"]
        batch_id = parsed["batch_id"]
        schema = parsed["schema"]
        points = parsed["points"]
        base_version_id = parsed.get("base_version_id")

        # 2) 幂等快速路径：已存在的 batch 直接返回，不占资源槽
        existing = self.store.find_by_batch(dataset, batch_id)
        if existing is not None:
            logger.info("导入幂等命中 dataset=%s batch_id=%s snapshot=%s",
                        dataset, batch_id, _version_digest(existing))
            return {"duplicated": True, "created": False,
                    "version": _version_digest(existing)}

        # 3) 资源边界
        if len(points) > self.max_points_per_batch:
            raise ResourceLimitExceededError(
                f"批次点数 {len(points)} 超过上限 {self.max_points_per_batch}",
                details={"points": len(points),
                         "limit": self.max_points_per_batch},
            )

        # 4) 并发槽：非阻塞获取，槽满立即失败（不排队、无悬挂任务）。
        #    槽位获取在 timeout 之外先完成；持槽工作受 timeout/取消保护，
        #    finally 保证任何退出路径都释放槽位，不影响后续请求。
        await self._take_slot()
        try:
            try:
                async with asyncio.timeout(timeout_s):
                    if self._import_blocker is not None:
                        await self._import_blocker()
                    # 校验与原子提交在线程内完成；store 锁保证全有或全无
                    version, created = await asyncio.to_thread(
                        self.store.import_batch,
                        dataset=dataset, batch_id=batch_id, schema=schema,
                        points=points, base_version_id=base_version_id,
                    )
            except TimeoutError:
                logger.warning("导入超时 dataset=%s batch_id=%s（原子提交，可安全重试）",
                               dataset, batch_id)
                raise TimeoutError_(
                    f"导入超过 {timeout_s}s 被终止；提交为原子操作，"
                    "可用相同 batch_id 安全重试",
                    details={"timeout_s": timeout_s, "dataset": dataset,
                             "batch_id": batch_id},
                ) from None
            except asyncio.CancelledError:
                logger.warning("导入被取消 dataset=%s batch_id=%s（原子提交，可安全重试）",
                               dataset, batch_id)
                raise CancelledError_(
                    "导入任务被取消；提交为原子操作，可用相同 batch_id 安全重试"
                ) from None
        finally:
            self._slots.release()

        logger.info("导入完成 dataset=%s append=%s snapshot=%s",
                    dataset, append, _version_digest(version))
        return {"duplicated": not created, "created": created,
                "version": _version_digest(version)}

    # ------------------------------------------------------------------ #
    # 解析
    # ------------------------------------------------------------------ #
    def _parse_import_request(self, request: dict) -> dict:
        if not isinstance(request, dict):
            raise InvalidParameterError("请求体必须是 JSON 对象")
        dataset = request.get("dataset")
        if not isinstance(dataset, str) or not dataset:
            raise InvalidParameterError("dataset 必须是非空字符串",
                                        details={"field": "dataset"})
        batch_id = request.get("batch_id")
        if not isinstance(batch_id, str) or not batch_id:
            raise InvalidParameterError("batch_id 必须是非空字符串（幂等键）",
                                        details={"field": "batch_id"})
        schema = validate_schema(request.get("schema"))
        points = validate_batch(request.get("points"), schema)

        parsed = {"dataset": dataset, "batch_id": batch_id,
                  "schema": schema, "points": points}

        if "base_version_id" in request and request["base_version_id"] is not None:
            bv = request["base_version_id"]
            if not isinstance(bv, str):
                raise InvalidParameterError("base_version_id 必须是字符串",
                                            details={"field": "base_version_id"})
            parsed["base_version_id"] = bv
        return parsed

    def _parse_analyze_request(self, request: dict) -> dict:
        if not isinstance(request, dict):
            raise InvalidParameterError("请求体必须是 JSON 对象")
        dataset = request.get("dataset")
        if not isinstance(dataset, str) or not dataset:
            raise InvalidParameterError("dataset 必须是非空字符串",
                                        details={"field": "dataset"})
        metric = request.get("metric")
        if not isinstance(metric, str) or not metric:
            raise InvalidParameterError("metric 必须是非空字符串",
                                        details={"field": "metric"})

        method_raw = request.get("method", "mean")
        try:
            method = AggMethod(method_raw)
        except ValueError:
            raise InvalidParameterError(
                f"不支持的聚合方法 {method_raw!r}",
                details={"method": method_raw,
                         "supported": [m.value for m in AggMethod]})

        window_ms = parse_window(request.get("window", "5m"))
        interval_ms = parse_interval(request.get("interval"))

        z = request.get("z_threshold", 3.0)
        if isinstance(z, bool) or not isinstance(z, (int, float)) or z <= 0:
            raise InvalidParameterError("z_threshold 必须是正数",
                                        details={"z_threshold": z})
        min_baseline = request.get("min_baseline", 3)
        if (isinstance(min_baseline, bool) or not isinstance(min_baseline, int)
                or min_baseline < 1):
            raise InvalidParameterError("min_baseline 必须是 >=1 的整数",
                                        details={"min_baseline": min_baseline})

        parsed: dict = {
            "dataset": dataset,
            "version_id": request.get("version_id"),
            "metric": metric,
            "method": method,
            "window": request.get("window", "5m"),
            "window_ms": window_ms,
            "interval": request.get("interval"),
            "interval_ms": interval_ms,
            "z_threshold": float(z),
            "min_baseline": min_baseline,
        }
        # start/end 允许缺省，缺省在拿到快照后按数据范围推导
        start = request.get("start")
        end = request.get("end")
        if (start is None) ^ (end is None):
            raise InvalidParameterError("start 与 end 必须同时提供或同时省略")
        if start is not None and (
            isinstance(start, bool) or isinstance(end, bool)
            or not isinstance(start, int) or not isinstance(end, int)
        ):
            raise InvalidParameterError("start/end 必须是 epoch 毫秒整数")
        parsed["start"] = start
        parsed["end"] = end
        return parsed

    def _resolve_timeout(self, timeout_s: float | None) -> float:
        if timeout_s is None:
            return self.default_timeout_s
        if isinstance(timeout_s, bool) or not isinstance(timeout_s, (int, float)):
            raise InvalidParameterError("timeout_s 必须是正数秒",
                                        details={"timeout_s": timeout_s})
        if timeout_s <= 0:
            raise InvalidParameterError("timeout_s 必须是正数秒",
                                        details={"timeout_s": timeout_s})
        return float(timeout_s)

    # ------------------------------------------------------------------ #
    # 计算（线程池内执行，纯 CPU，基于不可变快照）
    # ------------------------------------------------------------------ #
    def _compute(self, snapshot, parsed: dict) -> AnalysisReport:
        window_ms = parsed["window_ms"]
        start = parsed["start"]
        end = parsed["end"]
        if start is None:
            start, end = default_range(snapshot, window_ms)

        aggs = build_aggregates(
            snapshot, parsed["metric"], parsed["method"], window_ms,
            start, end, interval_ms=parsed["interval_ms"],
        )
        anomalies = detect(
            snapshot, parsed["metric"], aggs,
            z_threshold=parsed["z_threshold"],
            min_baseline=parsed["min_baseline"],
        )
        report = AnalysisReport(
            version_id=snapshot.version_id,
            metric=parsed["metric"],
            method=parsed["method"],
            window=parsed["window"],
            start=start,
            end=end,
            params={
                "method": parsed["method"].value,
                "window": parsed["window"],
                "interval": parsed["interval"],
                "z_threshold": parsed["z_threshold"],
                "min_baseline": parsed["min_baseline"],
            },
            aggregates=tuple(aggs),
            anomalies=tuple(anomalies),
        )
        anom_windows = [
            {"window_start": r.window_start, "reason": r.reason,
             **({"zscore": round(r.evidence.zscore, 6),
                 "baseline_mean": r.evidence.baseline_mean,
                 "baseline_std": r.evidence.baseline_std,
                 "baseline_n": r.evidence.baseline_n}
                if r.evidence else {})}
            for r in anomalies if r.is_anomaly
        ]
        logger.info(
            "分析完成 version=%s metric=%s windows=%d anomalies=%d evidence=%s",
            snapshot.version_id, parsed["metric"], len(aggs),
            len(anom_windows), anom_windows,
        )
        return report
