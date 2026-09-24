"""版本化存储：幂等导入、追加、不可变快照与一致读取。

并发模型
--------
- 一把 ``threading.RLock`` 串行化所有写入（导入/追加）。
- ``DatasetVersion`` 是不可变对象（frozen dataclass + tuple），
  读路径直接返回快照引用，无需持锁即可获得一致读：
  读发生在哪个版本上，就永远看到那个版本的完整内容。
- 写入是全有或全无：合并/排序/校验失败时不发布新版本。

幂等
----
按 (dataset, batch_id) 去重：同一批次重复提交，直接返回第一次创建的版本，
不产生重复数据、不分配新版本号、无任何重复副作用。
"""
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field

from app.errors import (
    DuplicateTimestampError,
    VersionConflictError,
    VersionNotFoundError,
)
from app.models import DataPoint, DatasetVersion, Schema


@dataclass
class _Dataset:
    name: str
    versions: list[DatasetVersion] = field(default_factory=list)
    batch_index: dict[str, str] = field(default_factory=dict)  # batch_id -> version_id


class VersionStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._datasets: dict[str, _Dataset] = {}

    # ------------------------------------------------------------------ #
    # 写
    # ------------------------------------------------------------------ #
    def import_batch(
        self,
        *,
        dataset: str,
        batch_id: str,
        schema: Schema,
        points: list[DataPoint],
        base_version_id: str | None = None,
    ) -> tuple[DatasetVersion, bool]:
        """导入新批次；``base_version_id`` 非空时为追加。

        返回 ``(版本快照, 是否本次新建)``。相同 (dataset, batch_id) 的重复调用
        返回首次创建的版本与 ``False``，不产生新数据/新版本/其他副作用。
        追加要求 schema 与父版本一致（结构演进请以新批次独立导入，
        新旧版本各自保留各自 schema 与数据，均可复现）。
        """
        with self._lock:
            ds = self._datasets.get(dataset)

            # 幂等命中：必须返回与首次完全相同的快照
            if ds is not None and batch_id in ds.batch_index:
                vid = ds.batch_index[batch_id]
                return self._find(ds, vid), False

            if ds is None:
                if base_version_id is not None:
                    raise VersionNotFoundError(
                        f"数据集 {dataset!r} 不存在，无法追加",
                        details={"dataset": dataset, "base_version_id": base_version_id},
                    )
                ds = _Dataset(name=dataset)
                self._datasets[dataset] = ds
                parent: DatasetVersion | None = None
            else:
                parent = (
                    ds.versions[-1]
                    if base_version_id is None
                    else self._find(ds, base_version_id)
                )
                if parent is not ds.versions[-1]:
                    # 只允许在最新版本上追加，避免分叉与并发竞争
                    raise VersionConflictError(
                        "只能在最新版本上追加",
                        details={
                            "base_version_id": base_version_id,
                            "latest_version_id": ds.versions[-1].version_id,
                        },
                    )
                if schema.fields != parent.schema.fields:
                    raise VersionConflictError(
                        "追加批次的 schema 必须与父版本一致；"
                        "结构演进请使用独立的新批次导入",
                        details={
                            "parent_schema": {
                                k: v.value for k, v in parent.schema.fields.items()
                            },
                            "batch_schema": {
                                k: v.value for k, v in schema.fields.items()
                            },
                        },
                    )

            merged = self._merge(parent, points)
            version = DatasetVersion(
                version_id=self._new_version_id(ds),
                schema=schema if parent is None else parent.schema,
                points=tuple(merged),
                parent_id=None if parent is None else parent.version_id,
                batch_id=batch_id,
                created_at=time.time(),
            )
            # 发布：两步引用赋值在锁内完成，读路径要么看到旧列表要么看到完整新版本
            ds.versions.append(version)
            ds.batch_index[batch_id] = version.version_id
            return version, True

    @staticmethod
    def _merge(
        parent: DatasetVersion | None, points: list[DataPoint]
    ) -> list[DataPoint]:
        """合并父版本数据与新批次；时间点重叠即拒绝，保证时间点唯一。"""
        new_by_ts: dict[int, DataPoint] = {}
        for p in points:  # 批次内重复已在 validation 拦截，此处双保险
            if p.timestamp in new_by_ts:
                raise DuplicateTimestampError(
                    "批次内存在重复时间戳", details={"value": p.timestamp}
                )
            new_by_ts[p.timestamp] = p

        if parent is not None:
            parent_ts = {p.timestamp for p in parent.points}
            overlap = parent_ts.intersection(new_by_ts)
            if overlap:
                ts = min(overlap)
                raise DuplicateTimestampError(
                    "追加数据与父版本存在重复时间点",
                    details={"value": ts, "count": len(overlap)},
                )
            combined = list(parent.points) + points
        else:
            combined = list(points)
        combined.sort(key=lambda p: p.timestamp)
        return combined

    @staticmethod
    def _new_version_id(ds: _Dataset) -> str:
        return f"v{len(ds.versions) + 1}-{uuid.uuid4().hex[:8]}"

    @staticmethod
    def _find(ds: _Dataset, version_id: str) -> DatasetVersion:
        for v in ds.versions:
            if v.version_id == version_id:
                return v
        raise VersionNotFoundError(
            "版本不存在",
            details={"dataset": ds.name, "version_id": version_id},
        )

    # ------------------------------------------------------------------ #
    # 读（返回不可变快照，无需持锁）
    # ------------------------------------------------------------------ #
    def get_version(self, dataset: str, version_id: str) -> DatasetVersion:
        ds = self._datasets.get(dataset)
        if ds is None:
            raise VersionNotFoundError(
                "数据集不存在", details={"dataset": dataset, "version_id": version_id}
            )
        return self._find(ds, version_id)

    def latest_version(self, dataset: str) -> DatasetVersion:
        ds = self._datasets.get(dataset)
        if ds is None or not ds.versions:
            raise VersionNotFoundError(
                "数据集不存在或尚无版本", details={"dataset": dataset}
            )
        return ds.versions[-1]

    def list_versions(self, dataset: str) -> list[DatasetVersion]:
        ds = self._datasets.get(dataset)
        if ds is None:
            raise VersionNotFoundError("数据集不存在", details={"dataset": dataset})
        return list(ds.versions)

    def find_by_batch(self, dataset: str, batch_id: str) -> DatasetVersion | None:
        ds = self._datasets.get(dataset)
        if ds is None:
            return None
        vid = ds.batch_index.get(batch_id)
        return None if vid is None else self._find(ds, vid)
