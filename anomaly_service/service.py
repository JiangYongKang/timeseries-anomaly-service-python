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
    QueryBudgetError,
    TimeoutError_,
    ValidationError,
)
from .models import (
    MISSING_GROUP,
    FieldType,
    GroupedQueryResult,
    GroupResult,
    ImportResult,
    QueryResult,
    WindowResult,
)
from .store import VersionStore
from .validation import DataValidator
from .windows import (
    MISSING_KEY,
    aggregate,
    bucket_by_group,
    enumerate_window_slots,
)


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

    def _effective_timeout(self, timeout: float | None) -> float:
        """未显式指定超时时，默认截止时间真实生效。"""
        return timeout if timeout is not None else self.config.default_timeout_seconds

    def import_data(
        self,
        dataset: str,
        raw_points: list[dict[str, object]],
        *,
        batch_id: str | None = None,
        append: bool = False,
        base_version: int | None = None,
        timeout: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> ImportResult:
        if not isinstance(dataset, str) or not dataset:
            raise ValidationError("dataset 必须是非空字符串")
        if base_version is not None and (
            not isinstance(base_version, int)
            or isinstance(base_version, bool)
            or base_version < 0
        ):
            raise ValidationError("base_version 必须是非负整数")
        deadline = _Deadline(self._effective_timeout(timeout), cancel_event)
        deadline.check()

        payload_hash = canonical_payload_hash(raw_points)
        replay = self._store.get_replay(dataset, batch_id, payload_hash)
        if replay is not None:
            return replay

        new_points, parsed_schema = self._validator.validate_points(
            raw_points, None
        )
        deadline.check()

        # 快速预检（最终在 store 锁内仍会复查，保证并发正确性）
        try:
            self._store.latest_version(dataset)
        except DatasetNotFoundError:
            if append:
                raise
            do_append = False
        else:
            if not append:
                raise DuplicateBatchError(
                    f"数据集 {dataset!r} 已存在版本；请使用 append=true 追加",
                    code="dataset_exists",
                )
            do_append = True

        return self._store.import_batch(
            dataset,
            new_points,
            parsed_schema,
            batch_id,
            payload_hash,
            append=do_append,
            merge_schema=self._validator.check_compatible_append,
            base_version=base_version,
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
        group_by: str | None = None,
        timeout: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> QueryResult | GroupedQueryResult:
        if not isinstance(metric_field, str) or not metric_field:
            raise ValidationError("metric_field 必须是非空字符串")
        if baseline_aggregate not in ("mean", "sum", "min", "max", "count"):
            raise ValidationError(f"不支持的聚合方法: {baseline_aggregate!r}")
        if group_by is not None and (not isinstance(group_by, str) or not group_by):
            raise ValidationError("group_by 必须是非空字符串")
        deadline = _Deadline(self._effective_timeout(timeout), cancel_event)
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
        if group_by is not None:
            group_spec = snapshot.schema_.get(group_by)
            if group_spec is None:
                raise FieldNotFoundError(
                    f"版本 v{snapshot.version} 中不存在分组字段 {group_by!r}"
                )
            if group_spec.type is FieldType.FLOAT:
                raise ValidationError(
                    f"分组字段 {group_by!r} 为 float，不是可枚举的分类维度",
                    code="invalid_group_field",
                )

        slots = enumerate_window_slots(start_ts, end_ts, window_seconds)
        # 单趟扫描：分组落桶 + 统计区间内参与计算的点数
        buckets, in_range_points = bucket_by_group(
            snapshot.points, metric_field, group_by, start_ts, end_ts, window_seconds
        )
        deadline.check()

        # -- 资源预算：先裁决再计算，超限明确失败且无任何副作用 --
        if in_range_points > self.config.max_query_points:
            raise QueryBudgetError(
                f"查询涉及 {in_range_points} 个数据点，超过单次查询上限 "
                f"{self.config.max_query_points}"
            )
        group_keys = sorted(
            buckets, key=lambda g: (g is MISSING_KEY, str(g))
        )
        n_groups = len(group_keys) if group_by is not None else 1
        total_windows = len(slots) * n_groups
        if total_windows > self.config.max_query_windows:
            raise QueryBudgetError(
                f"查询将产生 {total_windows} 个窗口（{len(slots)} 槽位 × "
                f"{n_groups} 组），超过单次查询上限 {self.config.max_query_windows}"
            )

        detector = BaselineAnomalyDetector(
            min_baseline=self.config.min_baseline_windows,
            threshold=self.config.anomaly_zscore_threshold,
            aggregate_method=baseline_aggregate,
        )

        if group_by is None:
            windows = self._judge_series(
                detector, slots, buckets.get(None, {}), baseline_aggregate, deadline
            )
            return QueryResult(
                dataset=dataset,
                version=snapshot.version,
                metric_field=metric_field,
                window_seconds=window_seconds,
                start_ts=start_ts,
                end_ts=end_ts,
                windows=windows,
            )

        groups: list[GroupResult] = []
        for idx, key in enumerate(group_keys):
            if idx % 16 == 0:
                deadline.check()
            label = MISSING_GROUP if key is MISSING_KEY else key
            groups.append(
                GroupResult(
                    group_value=label,
                    windows=self._judge_series(
                        detector, slots, buckets[key], baseline_aggregate, deadline
                    ),
                )
            )
        return GroupedQueryResult(
            dataset=dataset,
            version=snapshot.version,
            metric_field=metric_field,
            group_field=group_by,
            window_seconds=window_seconds,
            start_ts=start_ts,
            end_ts=end_ts,
            groups=tuple(groups),
        )

    @staticmethod
    def _judge_series(
        detector: BaselineAnomalyDetector,
        slots: list[tuple[int, int]],
        by_start: dict[int, list[float]],
        baseline_aggregate: str,
        deadline: _Deadline,
    ) -> tuple[WindowResult, ...]:
        """对单条序列逐窗口聚合并判定；无数据窗口判 no_data，绝不误报。"""
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
        return tuple(results)

    def list_versions(self, dataset: str) -> list[int]:
        return self._store.list_versions(dataset)
