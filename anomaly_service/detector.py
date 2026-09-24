"""异常检测：基线均值/总体标准差 + z-score。

判定规则（同版本同参数必然同结论）：
1. 窗口无有效数据点（聚合为 None）→ ``no_data``，绝不误报；
2. 基线完整窗口数 < ``min_baseline`` → ``insufficient_data``，不判异常；
3. 基线总体标准差为 0（恒定基线）：窗口值与基线均值不同即 ``anomaly``，
   z-score 记为 None（不可除零）；相同则 ``normal``；
4. 其余情况计算 z = (窗口值 - 基线均值) / 总体标准差，
   ``|z| > threshold`` 判为 ``anomaly``，否则 ``normal``。
"""

from __future__ import annotations

import math

from .models import AnomalyEvidence, AnomalyStatus


class BaselineAnomalyDetector:
    def __init__(
        self,
        min_baseline: int = 3,
        threshold: float = 3.0,
        aggregate_method: str = "mean",
    ) -> None:
        if min_baseline < 1:
            raise ValueError("min_baseline 必须 >= 1")
        if threshold <= 0:
            raise ValueError("threshold 必须为正数")
        self.min_baseline = min_baseline
        self.threshold = float(threshold)
        self.aggregate_method = aggregate_method

    @staticmethod
    def baseline_stats(values: list[float]) -> tuple[float, float]:
        n = len(values)
        mean = sum(values) / n
        variance = sum((v - mean) ** 2 for v in values) / n
        return mean, math.sqrt(variance)

    def judge(
        self,
        window_value: float | None,
        baseline_values: list[float],
    ) -> tuple[AnomalyStatus, AnomalyEvidence | None]:
        if window_value is None:
            return AnomalyStatus.NO_DATA, AnomalyEvidence(
                metric=self.aggregate_method,
                window_value=None,
                baseline_mean=None,
                baseline_std=None,
                zscore=None,
                threshold=self.threshold,
                reason="窗口内无有效数据点，跳过判定（no_data 不误报）",
            )
        if len(baseline_values) < self.min_baseline:
            return AnomalyStatus.INSUFFICIENT_DATA, AnomalyEvidence(
                metric=self.aggregate_method,
                window_value=window_value,
                baseline_mean=None,
                baseline_std=None,
                zscore=None,
                threshold=self.threshold,
                reason=(
                    f"基线完整窗口仅 {len(baseline_values)} 个，"
                    f"少于最小要求 {self.min_baseline} 个，样本不足"
                ),
            )

        mean, std = self.baseline_stats(baseline_values)
        if std == 0.0:
            anomalous = window_value != mean
            reason = (
                f"基线恒定（std=0，mean={mean:.6g}），窗口值 {window_value:.6g} "
                + ("偏离恒定基线" if anomalous else "等于恒定基线")
            )
            status = AnomalyStatus.ANOMALY if anomalous else AnomalyStatus.NORMAL
            return status, AnomalyEvidence(
                metric=self.aggregate_method,
                window_value=window_value,
                baseline_mean=mean,
                baseline_std=0.0,
                zscore=None,
                threshold=self.threshold,
                reason=reason,
            )

        z = (window_value - mean) / std
        anomalous = abs(z) > self.threshold
        status = AnomalyStatus.ANOMALY if anomalous else AnomalyStatus.NORMAL
        reason = (
            f"z-score={z:.6g}，阈值 |z|>{self.threshold:g}；"
            + ("超过阈值，判定异常" if anomalous else "未超过阈值，判定正常")
        )
        return status, AnomalyEvidence(
            metric=self.aggregate_method,
            window_value=window_value,
            baseline_mean=mean,
            baseline_std=std,
            zscore=z,
            threshold=self.threshold,
            reason=reason,
        )
