"""版本化存储：不可变版本快照 + 原子指针切换。

并发模型：
- 每个数据集一把可重入锁，导入/追加完全串行化，因此并发重复批次必然
  命中幂等记录，不可能产生两个版本或重复副作用；
- 读取不加锁：版本对象一旦发布即不可变，``current`` 指针的一次赋值
  即为原子的版本切换，读者要么看到完整旧版本，要么看到完整新版本。
"""

from __future__ import annotations

import threading

from .errors import (
    DatasetNotFoundError,
    DuplicateBatchError,
    DuplicatePointError,
    ResourceLimitError,
    StaleSchemaError,
    VersionNotFoundError,
)
from .models import DataPoint, DatasetVersion, ImportResult, Schema


class _DatasetState:
    __slots__ = ("batches", "current", "lock", "point_total", "versions")

    def __init__(self) -> None:
        self.versions: dict[int, DatasetVersion] = {}
        self.current: int | None = None
        self.batches: dict[str, tuple[str, ImportResult]] = {}
        self.point_total: int = 0
        self.lock = threading.RLock()


class VersionStore:
    def __init__(self, max_points_per_dataset: int = 1_000_000) -> None:
        self.max_points_per_dataset = max_points_per_dataset
        self._datasets: dict[str, _DatasetState] = {}
        self._catalog_lock = threading.Lock()

    def _state(self, dataset: str, *, create: bool = False) -> _DatasetState:
        state = self._datasets.get(dataset)
        if state is None:
            if not create:
                raise DatasetNotFoundError(f"数据集不存在: {dataset}")
            with self._catalog_lock:
                state = self._datasets.get(dataset)
                if state is None:
                    state = _DatasetState()
                    self._datasets[dataset] = state
        return state

    def get_replay(
        self, dataset: str, batch_id: str | None, payload_hash: str
    ) -> ImportResult | None:
        """命中已导入批次则返回重放结果；载荷冲突抛错；未命中返回 None。"""
        try:
            state = self._state(dataset)
        except DatasetNotFoundError:
            return None
        key = batch_id if batch_id is not None else f"content:{payload_hash}"
        with state.lock:
            existing = state.batches.get(key)
            if existing is None:
                return None
            stored_hash, result = existing
            if stored_hash != payload_hash:
                raise DuplicateBatchError(
                    f"批次 {key!r} 已导入但载荷不同，拒绝重复导入"
                )
            return ImportResult(
                dataset=result.dataset,
                version=result.version,
                appended=result.appended,
                point_count=result.point_count,
                batch_id=result.batch_id,
                replayed=True,
            )

    def import_batch(
        self,
        dataset: str,
        points: list[DataPoint],
        parsed_schema: Schema,
        batch_id: str | None,
        payload_hash: str,
        append: bool,
        *,
        merge_schema=None,
        base_version: int | None = None,
        before_publish=None,
    ) -> ImportResult:
        """在数据集锁内完成全部校验后一次性发布新版本。

        - ``merge_schema(parent_schema, batch_schema)``：追加时在**持锁状态
          下**基于真实的父版本 schema 计算合并结构，保证并发演进下发布版本
          的声明字段与实际数据完全对应；抛异常则中止且无任何写入；
        - ``base_version``：可选的乐观并发令牌，声明请求基于的版本号
          （数据集尚不存在时为 0）；与锁内实际最新版本不一致则拒绝；
        - ``before_publish(state, parent)``：发布前的取消/超时检查钩子。
        """
        state = self._state(dataset, create=True)
        with state.lock:
            key = batch_id if batch_id is not None else f"content:{payload_hash}"

            existing = state.batches.get(key)
            if existing is not None:
                stored_hash, result = existing
                if stored_hash != payload_hash:
                    raise DuplicateBatchError(
                        f"批次 {key!r} 已导入但载荷不同，拒绝重复导入"
                    )
                return ImportResult(
                    dataset=result.dataset,
                    version=result.version,
                    appended=result.appended,
                    point_count=result.point_count,
                    batch_id=result.batch_id,
                    replayed=True,
                )

            parent: DatasetVersion | None = (
                state.versions[state.current] if state.current is not None else None
            )
            if parent is None and append:
                raise DatasetNotFoundError(f"数据集 {dataset!r} 不存在，无法追加")
            if parent is not None and not append:
                raise DuplicateBatchError(
                    f"数据集 {dataset!r} 已存在版本；请使用 append=true 追加",
                    code="dataset_exists",
                )

            actual_base = parent.version if parent is not None else 0
            if base_version is not None and base_version != actual_base:
                raise StaleSchemaError(
                    f"追加基于版本 v{base_version}，但当前最新为 "
                    f"v{actual_base}；请基于最新结构重试"
                )

            # 结构合并在锁内基于真实父版本进行，杜绝并发下字段被悄悄丢掉
            if parent is None:
                final_schema = parsed_schema
            elif merge_schema is not None:
                final_schema = merge_schema(parent.schema_, parsed_schema)
            else:
                final_schema = parsed_schema

            if parent is not None:
                overlap = {p.ts for p in parent.points}.intersection(
                    p.ts for p in points
                )
                if overlap:
                    raise DuplicatePointError(
                        f"追加数据与版本 v{parent.version} 存在重复时间点: "
                        f"{sorted(overlap)[:5]}"
                    )

            new_total = state.point_total + len(points)
            if new_total > self.max_points_per_dataset:
                raise ResourceLimitError(
                    f"数据集总点数将达到 {new_total}，超过上限 "
                    f"{self.max_points_per_dataset}"
                )

            if before_publish is not None:
                before_publish(state, parent)

            version_no = (state.current or 0) + 1
            merged_points = (
                tuple(parent.points) + tuple(points)
                if parent is not None
                else tuple(points)
            )
            snapshot = DatasetVersion(
                dataset=dataset,
                version=version_no,
                schema_=final_schema,
                points=merged_points,
                parent_version=parent.version if parent is not None else None,
                batch_id=key,
            )
            result = ImportResult(
                dataset=dataset,
                version=version_no,
                appended=parent is not None,
                point_count=len(merged_points),
                batch_id=key,
                replayed=False,
            )
            # 全部校验完成后才发布：任何异常都不会留下半写入状态。
            state.versions[version_no] = snapshot
            state.batches[key] = (payload_hash, result)
            state.point_total = new_total
            state.current = version_no
            return result

    def get_version(self, dataset: str, version: int) -> DatasetVersion:
        state = self._state(dataset)
        snapshot = state.versions.get(version)
        if snapshot is None:
            raise VersionNotFoundError(f"数据集 {dataset!r} 不存在版本 v{version}")
        return snapshot

    def latest_version(self, dataset: str) -> DatasetVersion:
        state = self._state(dataset)
        if state.current is None:
            raise DatasetNotFoundError(f"数据集不存在: {dataset}")
        return state.versions[state.current]

    def list_versions(self, dataset: str) -> list[int]:
        state = self._state(dataset)
        return sorted(state.versions)
