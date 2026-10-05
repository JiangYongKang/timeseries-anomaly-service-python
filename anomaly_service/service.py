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
    FieldNotFoundError,
    QueryBudgetExceededError,
    StaleSchemaError,
    TimeoutError_,
    ValidationError,
)
from .models import (
    MISSING_GROUP_KEY,
    FieldType,
    GroupedQueryResult,
    GroupResult,
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


def _group_sort_key(key: object) -> tuple:
    """分组键的稳定全序：缺失桶固定在最前，其余按 (类型名, 值) 排序。

    保证同版本 + 同参数 + 同区间的分组输出顺序完全可复现，
    与数据点到达顺序无关。
    """
    if key == MISSING_GROUP_KEY:
        return (0, "", "")
    return (1, type(key).__name__, str(key))


class AnomalyService:
    def __init__(self, config: ServiceConfig | None = None) -> None:
        self.config = config or ServiceConfig()
        self._validator = DataValidator(max_points=self.config.max_points_per_batch)
        self._store = VersionStore(
            max_points_per_dataset=self.config.max_points_per_dataset
        )

    # -- 导入 -------------------------------------------------------------

    def _make_deadline(
        self, timeout: float | None, cancel_event: threading.Event | None
    ) -> _Deadline:
        """未显式指定超时时，套用配置中的默认截止时间（真实生效）。"""
        effective = timeout if timeout is not None else (
            self.config.default_timeout_seconds
        )
        return _Deadline(effective, cancel_event)

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
        deadline = self._make_deadline(timeout, cancel_event)
        deadline.check()

        payload_hash = canonical_payload_hash(raw_points)
        replay = self._store.get_replay(dataset, batch_id, payload_hash)
        if replay is not None:
            return replay

        new_points, parsed_schema = self._validator.validate_points(
            raw_points, None
        )
        deadline.check()

        if append:
            # 确认数据集存在（快速失败路径）；结构合并与发布在 store 锁内
            # 针对真实父版本完成，并发演进不会互相覆盖。
            self._store.latest_version(dataset)

        return self._store.import_batch(
            dataset,
            new_points,
            parsed_schema,
            batch_id,
            payload_hash,
            append=append,
            merge_schema=self._merge_append_schema if append else None,
            before_publish=lambda *_: deadline.check(),
        )

    def _merge_append_schema(self, parent_schema: Schema, parsed: Schema) -> Schema:
        """追加时的结构合并（在数据集锁内针对真实父版本调用）。

        - 父版本声明了请求方不知道的列 → 该请求基于过时结构发出，
          以 ``stale_schema`` 明确拒绝（可刷新结构后安全重试）；
        - 否则按既有规则演进：新增列并入为可空列，int→float 拓宽，
          其余不兼容变更抛 ``incompatible_schema``。
        """
        unknown = [f.name for f in parent_schema.fields if parsed.get(f.name) is None]
        if unknown:
            raise StaleSchemaError(
                f"追加请求基于过时的结构：当前版本已包含字段 {unknown}，"
                f"请刷新 schema 后重试"
            )
        return self._validator.check_compatible_append(parent_schema, parsed)

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
        deadline = self._make_deadline(timeout, cancel_event)
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
        slots = enumerate_window_slots(start_ts, end_ts, window_seconds)
        self._check_query_budget(snapshot.points, start_ts, end_ts, n_slots=len(slots), n_groups=1)
        results = self._window_results(
            snapshot.points,
            metric_field,
            start_ts,
            end_ts,
            window_seconds,
            detector,
            deadline,
            slots,
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

    # -- 分组查询 -----------------------------------------------------------

    def query_grouped(
        self,
        dataset: str,
        metric_field: str,
        group_field: str,
        start_ts: int,
        end_ts: int,
        window_seconds: int,
        *,
        version: int | None = None,
        baseline_aggregate: str = "mean",
        timeout: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> GroupedQueryResult:
        """按分类字段把同一版本拆成多条序列，各自聚合、各自维护基线。

        语义：
        - 分组字段必须是 schema 中的 string/int/bool 分类列；
        - 分组值为 None（含历史点缺该列）的点统一归入 MISSING_GROUP_KEY，
          绝不静默丢弃；
        - 每条序列独立按窗口聚合、独立维护基线；某条序列或某个窗口无数据
          时判 no_data，绝不误报；
        - 分组按确定顺序输出，同版本 + 同参数 + 同区间结果完全可复现。
        """
        if not isinstance(metric_field, str) or not metric_field:
            raise ValidationError("metric_field 必须是非空字符串")
        if not isinstance(group_field, str) or not group_field:
            raise ValidationError("group_field 必须是非空字符串")
        if baseline_aggregate not in ("mean", "sum", "min", "max", "count"):
            raise ValidationError(f"不支持的聚合方法: {baseline_aggregate!r}")
        deadline = self._make_deadline(timeout, cancel_event)
        deadline.check()

        snapshot = (
            self._store.get_version(dataset, version)
            if version is not None
            else self._store.latest_version(dataset)
        )
        metric_spec = snapshot.schema_.get(metric_field)
        if metric_spec is None or metric_spec.type not in (
            FieldType.FLOAT,
            FieldType.INT,
        ):
            raise FieldNotFoundError(
                f"版本 v{snapshot.version} 中不存在可聚合数值字段 {metric_field!r}"
            )
        group_spec = snapshot.schema_.get(group_field)
        if group_spec is None or group_spec.type not in (
            FieldType.STRING,
            FieldType.INT,
            FieldType.BOOL,
        ):
            raise ValidationError(
                f"版本 v{snapshot.version} 中不存在可作为分组维度的分类字段 "
                f"{group_field!r}（支持 string/int/bool）",
                code="invalid_group_field",
            )

        # 单趟扫描：按分组键把区间内数据点拆成互不干扰的序列
        buckets: dict[object, list] = {}
        for p in snapshot.points:
            if p.ts < start_ts or p.ts >= end_ts:
                continue
            key = p.values.get(group_field)
            if key is None:
                key = MISSING_GROUP_KEY
            buckets.setdefault(key, []).append(p)

        # 预算在逐组计算前一次性检查：窗口总数 = 槽位数 × 分组数
        slots = enumerate_window_slots(start_ts, end_ts, window_seconds)
        self._check_query_budget(
            snapshot.points, start_ts, end_ts,
            n_slots=len(slots), n_groups=len(buckets),
        )

        detector = BaselineAnomalyDetector(
            min_baseline=self.config.min_baseline_windows,
            threshold=self.config.anomaly_zscore_threshold,
            aggregate_method=baseline_aggregate,
        )
        groups: list[GroupResult] = []
        for key in sorted(buckets, key=_group_sort_key):
            deadline.check()
            results = self._window_results(
                tuple(buckets[key]),
                metric_field,
                start_ts,
                end_ts,
                window_seconds,
                detector,
                deadline,
                slots,
            )
            groups.append(GroupResult(group_key=key, windows=tuple(results)))
        return GroupedQueryResult(
            dataset=dataset,
            version=snapshot.version,
            metric_field=metric_field,
            group_field=group_field,
            window_seconds=window_seconds,
            start_ts=start_ts,
            end_ts=end_ts,
            groups=tuple(groups),
        )

    # -- 内部 ---------------------------------------------------------------

    def _check_query_budget(
        self,
        points,
        start_ts: int,
        end_ts: int,
        *,
        n_slots: int,
        n_groups: int,
    ) -> None:
        """单次查询资源预算：窗口总数与参与计算的数据量双上限。

        超限抛 QueryBudgetExceededError（413），与参数错误（422）明确区分；
        检查发生在任何重计算之前，超限请求被快速拒绝，不会拖住服务。
        """
        total_windows = n_slots * max(n_groups, 1)
        if total_windows > self.config.max_query_windows:
            raise QueryBudgetExceededError(
                f"查询涉及窗口总数 {total_windows}"
                f"（{n_slots} 槽位 × {max(n_groups, 1)} 分组），"
                f"超过预算 {self.config.max_query_windows}"
            )
        in_range = 0
        for p in points:
            if start_ts <= p.ts < end_ts:
                in_range += 1
        if in_range > self.config.max_query_points:
            raise QueryBudgetExceededError(
                f"查询涉及数据点 {in_range} 个，"
                f"超过预算 {self.config.max_query_points}"
            )

    @staticmethod
    def _window_results(
        points,
        metric_field: str,
        start_ts: int,
        end_ts: int,
        window_seconds: int,
        detector: BaselineAnomalyDetector,
        deadline: _Deadline,
        slots: list[tuple[int, int]],
    ) -> list[WindowResult]:
        """对单条序列（同一分组）枚举完整窗口并逐窗判定。"""
        buckets = window_windows(
            points, metric_field, start_ts, end_ts, window_seconds
        )
        by_start = {ws: vals for ws, _, vals in buckets}

        results: list[WindowResult] = []
        baseline_series: list[float] = []
        for idx, (ws, we) in enumerate(slots):
            if idx % 64 == 0:
                deadline.check()
            values = by_start.get(ws, [])
            value = aggregate(values, detector.aggregate_method)
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
        return results

    def list_versions(self, dataset: str) -> list[int]:
        return self._store.list_versions(dataset)
