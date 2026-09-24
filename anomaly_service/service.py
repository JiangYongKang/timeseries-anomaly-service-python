"""应用门面：串联校验、存储、窗口与检测。

并发语义：
- 导入/追加在 store 内按数据集串行化；查询完全基于某个不可变版本快照，
  无锁读取。导入发布后新查询必然看到新版本，已开始的查询基于旧快照完成；
- 超时 / 取消：在批次校验与发布前检查点轮询。store 的写入是“全部校验
  通过后一次性发布”，因此失败不会留下半写入状态或批次记录。

schema 演进：追加批次可带来新增的可空列；历史点不被改写，缺失新列在
读取时按 None 处理，因此旧版本与新版本都可独立复现。
"""

from __future__ import annotations

import hashlib
import json
import threading
import time

from .config import ServiceConfig
from .detector import BaselineAnomalyDetector
from .errors import (
    CancelledError_,
    DatasetNotFoundError,
    DuplicateBatchError,
    FieldNotFoundError,
    TimeoutError_,
    ValidationError,
)
from .models import (
    FieldType,
    ImportResult,
    QueryResult,
    Schema,
    WindowResult,
)
from .store import VersionStore
from .validation import DataValidator
from .windows import aggregate, enumerate_window_slots, window_windows


class _Deadline:
    __slots__ = ("cancel_event", "deadline")

    def __init__(self, timeout: float | None, cancel_event: threading.Event | None):
        self.deadline = time.monotonic() + timeout if timeout is not None else None
        self.cancel_event = cancel_event

    def check(self) -> None:
        if self.cancel_event is not None and self.cancel_event.is_set():
            raise CancelledError_("操作已被取消")
        if self.deadline is not None and time.monotonic() >= self.deadline:
            raise TimeoutError_("操作超过截止时间")


def canonical_payload_hash(raw_points: list[dict[str, object]]) -> str:
    blob = json.dumps(
        raw_points, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


class AnomalyService:
    def __init__(self, config: ServiceConfig | None = None) -> None:
        self.config = config or ServiceConfig()
        self._validator = DataValidator(max_points=self.config.max_points_per_batch)
        self._store = VersionStore(
            max_points_per_dataset=self.config.max_points_per_dataset
        )

    # -- 导入 -------------------------------------------------------------

    def import_data(
        self,
        dataset: str,
        raw_points: list[dict[str, object]],
        *,
        batch_id: str | None = None,
        append: bool = False,
        timeout: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> ImportResult:
        if not isinstance(dataset, str) or not dataset:
            raise ValidationError("dataset 必须是非空字符串")
        deadline = _Deadline(timeout, cancel_event)
        deadline.check()

        payload_hash = canonical_payload_hash(raw_points)
        replay = self._store.get_replay(dataset, batch_id, payload_hash)
        if replay is not None:
            return replay

        new_points, parsed_schema = self._validator.validate_points(
            raw_points, None
        )
        deadline.check()

        try:
            parent = self._store.latest_version(dataset)
        except DatasetNotFoundError:
            if append:
                raise
            final_schema: Schema = parsed_schema
            do_append = False
        else:
            if not append:
                raise DuplicateBatchError(
                    f"数据集 {dataset!r} 已存在版本；请使用 append=true 追加",
                    code="dataset_exists",
                )
            final_schema = self._validator.check_compatible_append(
                parent.schema_, parsed_schema
            )
            do_append = True

        return self._store.import_batch(
            dataset,
            new_points,
            final_schema,
            batch_id,
            payload_hash,
            append=do_append,
            before_publish=lambda *_: deadline.check(),
        )

    # -- 查询 -------------------------------------------------------------

    def query(
        self,
        dataset: str,
        metric_field: str,
        start_ts: int,
        end_ts: int,
        window_seconds: int,
        *,
        version: int | None = None,
        baseline_aggregate: str = "mean",
        timeout: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> QueryResult:
        if not isinstance(metric_field, str) or not metric_field:
            raise ValidationError("metric_field 必须是非空字符串")
        if baseline_aggregate not in ("mean", "sum", "min", "max", "count"):
            raise ValidationError(f"不支持的聚合方法: {baseline_aggregate!r}")
        deadline = _Deadline(timeout, cancel_event)
        deadline.check()

        snapshot = (
            self._store.get_version(dataset, version)
            if version is not None
            else self._store.latest_version(dataset)
        )
        spec = snapshot.schema_.get(metric_field)
        if spec is None or spec.type not in (FieldType.FLOAT, FieldType.INT):
            raise FieldNotFoundError(
                f"版本 v{snapshot.version} 中不存在可聚合数值字段 {metric_field!r}"
            )

        detector = BaselineAnomalyDetector(
            min_baseline=self.config.min_baseline_windows,
            threshold=self.config.anomaly_zscore_threshold,
            aggregate_method=baseline_aggregate,
        )
        buckets = window_windows(
            snapshot.points, metric_field, start_ts, end_ts, window_seconds
        )
        by_start = {ws: vals for ws, _, vals in buckets}
        slots = enumerate_window_slots(start_ts, end_ts, window_seconds)

        results: list[WindowResult] = []
        baseline_series: list[float] = []
        for idx, (ws, we) in enumerate(slots):
            if idx % 64 == 0:
                deadline.check()
            values = by_start.get(ws, [])
            value = aggregate(values, baseline_aggregate)
            status, evidence = detector.judge(value, baseline_series)
            if value is not None:
                baseline_series.append(value)
            results.append(
                WindowResult(
                    window_start=ws,
                    window_end=we,
                    count=len(values),
                    aggregate=value,
                    status=status,
                    evidence=evidence,
                )
            )
        return QueryResult(
            dataset=dataset,
            version=snapshot.version,
            metric_field=metric_field,
            window_seconds=window_seconds,
            start_ts=start_ts,
            end_ts=end_ts,
            windows=tuple(results),
        )

    def list_versions(self, dataset: str) -> list[int]:
        return self._store.list_versions(dataset)
