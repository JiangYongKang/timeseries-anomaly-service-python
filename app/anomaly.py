"""异常判定：严格历史滚动基线 + z-score。

规则（确定性，同版本同参数结论可复现）
--------------------------------------
- 按窗口时间顺序逐个判定；窗口 i 的基线**只**由它之前已完成的非空窗口构成，
  不使用当前窗口，也不使用任何未来窗口（避免数据泄漏；因此乱序到达不影响结论）。
- 当且仅当历史非空窗口数 >= ``min_baseline`` 时才可能判定异常：
  zscore = (observed - mean) / std（std 为总体标准差），
  |zscore| >= z_threshold 判异常。std==0 时：观测等于均值则正常，否则异常。
- 无数据窗口（count==0）：非异常，原因 NO_DATA。
- 基线不足：非异常，原因 INSUFFICIENT_BASELINE。
- 其余非异常窗口原因：NORMAL。
每条判定都给出基线均值/标准差/样本数/阈值/观测值/zscore，保证可解释。
"""
from __future__ import annotations

import math

from app.models import (
    AnomalyEvidence,
    AnomalyResult,
    DatasetVersion,
    WindowAggregate,
)


def _population_std(values: list[float], mean: float) -> float:
    return math.sqrt(sum((x - mean) ** 2 for x in values) / len(values))


def detect(
    version: DatasetVersion,
    metric: str,
    aggregates: list[WindowAggregate],
    *,
    z_threshold: float,
    min_baseline: int,
) -> list[AnomalyResult]:
    if isinstance(z_threshold, bool) or not isinstance(z_threshold, (int, float)):
        from app.errors import InvalidParameterError
        raise InvalidParameterError("z_threshold 必须是数值")
    if z_threshold <= 0:
        from app.errors import InvalidParameterError
        raise InvalidParameterError("z_threshold 必须为正数",
                                    details={"z_threshold": z_threshold})
    if isinstance(min_baseline, bool) or not isinstance(min_baseline, int):
        from app.errors import InvalidParameterError
        raise InvalidParameterError("min_baseline 必须是正整数")
    if min_baseline < 1:
        from app.errors import InvalidParameterError
        raise InvalidParameterError("min_baseline 必须 >= 1",
                                    details={"min_baseline": min_baseline})

    results: list[AnomalyResult] = []
    history: list[float] = []  # 仅包含已完成的非空窗口值

    for agg in aggregates:
        common = {
            "window_start": agg.window_start,
            "window_end": agg.window_end,
            "metric": metric,
        }

        if agg.count == 0 or agg.value is None:
            results.append(AnomalyResult(
                **common, is_anomaly=False,
                reason="NO_DATA: 窗口内无数据点，按规则不判定异常", evidence=None))
            continue

        observed = agg.value
        if len(history) < min_baseline:
            results.append(AnomalyResult(
                **common, is_anomaly=False,
                reason=(f"INSUFFICIENT_BASELINE: 历史非空窗口 {len(history)} 个，"
                        f"少于 min_baseline={min_baseline}，不判定异常"),
                evidence=None))
            history.append(observed)
            continue

        mean = sum(history) / len(history)
        std = _population_std(history, mean)
        z = 0.0 if std == 0 else (observed - mean) / std
        evidence = AnomalyEvidence(
            baseline_mean=mean,
            baseline_std=std,
            baseline_n=len(history),
            threshold=float(z_threshold),
            observed=observed,
            zscore=z,
            rule=(f"|zscore| >= {z_threshold} 判定异常；"
                  f"基线为该窗口之前 {len(history)} 个非空窗口（总体标准差）"),
        )
        if std == 0:
            is_anom = observed != mean
            why = ("基线标准差为 0，观测值与恒定基线不同" if is_anom
                   else "基线标准差为 0，观测值等于恒定基线")
        else:
            is_anom = abs(z) >= z_threshold
            why = f"|zscore|={abs(z):.6g} {'>=' if is_anom else '<'} {z_threshold}"
        results.append(AnomalyResult(
            **common, is_anomaly=is_anom,
            reason=("ANOMALY: " if is_anom else "NORMAL: ") + why,
            evidence=evidence))
        history.append(observed)

    return results
