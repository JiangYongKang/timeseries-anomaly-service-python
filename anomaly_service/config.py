"""服务配置与资源边界。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ServiceConfig:
    max_points_per_batch: int = 100_000
    max_points_per_dataset: int = 1_000_000
    default_timeout_seconds: float = 5.0
    min_baseline_windows: int = 3
    anomaly_zscore_threshold: float = 3.0
    # 单次查询预算：窗口总数（窗口槽位 × 分组数）与参与计算的数据点数
    max_query_windows: int = 10_000
    max_query_points: int = 500_000
