# 时序数据版本化分析与异常检测服务

一个可本地运行的 FastAPI 服务，支持时序数据集的**版本化导入/追加/查询**、
**时间窗口聚合**与**可解释的异常检测**。核心目标：在重复导入、乱序数据、
版本切换与并发请求下，结论**一致、可复现、可解释**。

- 运行时：Python 3.14、FastAPI；存储为进程内内存（带版本快照）
- 纯标准库实现统计与并发控制，无外部数据库依赖

---

## 1. 本地运行

```bash
uv sync
uv run uvicorn main:app --reload          # 启动服务，默认 http://127.0.0.1:8000
# 交互式文档：http://127.0.0.1:8000/docs

uv run pytest -s                          # 运行全部单测（-s 查看关键日志）
```

单测日志会打印**关键输入、版本快照与异常判定依据**（由 `logger "anomaly_service"`
与测试辅助 `log_snapshot/log_report` 输出）。

---

## 2. 数据与版本语义

### 2.1 数据点与 Schema

- 时间戳 `ts`：**Unix epoch 毫秒整数（UTC）**，允许范围 `[2000-01-01, 2100-01-01)`。
- 值容器 `values`：`{字段名: 数值}`；字段集合由 `schema` 定义，类型为 `float` 或 `int`。
  - `int` 字段收到非整数（如 `2.5`）会被拒绝；布尔值不是数值；`NaN/Inf` 被拒绝。

```json
{"ts": 1577836800000, "values": {"temp": 23.5}}
```

### 2.2 版本模型（不可变快照）

每次导入/追加都产生一个**不可变版本** `DatasetVersion`：

| 字段 | 含义 |
| --- | --- |
| `version_id` | 版本唯一标识 |
| `schema` | 该版本的结构快照 |
| `points` | 按 `ts` 升序、时间点唯一的数据元组 |
| `parent_id` | 追加场景的父版本；首次导入为 `null` |
| `batch_id` | 导入批次幂等键 |
| `created_at` | 创建时间 |

- **幂等导入**：以 `(dataset, batch_id)` 去重。同一批次重复提交（即使报文内容不同）
  都返回**第一次创建的同一版本**，响应中 `created=false / duplicated=true`，
  不产生重复数据、不分配新版本、无其他副作用。
- **追加**：在数据集**最新版本**上追加；与父版本存在相同时间点即整体拒绝
  （`DUPLICATE_TIMESTAMP`），不产生半写入。在非最新版本上追加返回 `VERSION_CONFLICT`。
- **结构演进**：追加要求 schema 与父版本一致；需要新结构时，以**独立批次/数据集**
  导入，新旧版本各自保留自己的 schema 与数据，**永久可复现**。
- **校验是全有或全无**：批次内任一点非法（字段缺失/类型不符/非法时间/批次内重复时间点）
  整个批次被拒绝，存储状态不变。
- **乱序无关**：导入时按时间戳排序并保证时间点唯一，结论只取决于版本内容，
  与数据到达顺序无关。

---

## 3. 窗口与聚合规则

- 窗口表达式：`"<n><unit>"`，单位 `s / m / h / d`（如 `30s`、`5m`、`1h`、`1d`）。
- **网格对齐**：窗口以 Unix epoch 为起点对齐。`window=5m` 时边界恒为
  `...10:00, 10:05, 10:10...`；`window_start = floor(ts / window) * window`。
- 分析范围为**左闭右开 `[start, end)`**；`start/end` 必须是 epoch 毫秒整数、
  `end > start` 且与窗口网格对齐，否则返回 `INVALID_PARAMETER`（不做隐式截断）。
  省略 `start/end` 时按数据时间范围外扩对齐到窗口网格。
- 聚合方法：`mean / sum / min / max`（默认 `mean`）。
- **空窗口**：`count == 0`、`value == null`。空窗口是稠密结果序列中的一等公民，
  异常检测对其**永不报警**（原因 `NO_DATA`）。
- **缺失值**：可选 `interval`（期望采样间隔）。当 `window` 是 `interval` 整数倍时，
  `missing = window/interval - count`，否则参数校验失败。
- 窗口数量上限 `MAX_WINDOWS_PER_QUERY = 100000`，超限返回
  `RESOURCE_LIMIT_EXCEEDED`。

---

## 4. 异常判定规则

采用**严格历史滚动基线 + z-score**，结果确定性、可复现：

- 按窗口时间顺序判定。窗口 *i* 的基线**只**包含它之前已完成的非空窗口
  （不含当前窗口、不含未来窗口，避免数据泄漏）。
- 当历史非空窗口数 `>= min_baseline` 时：

  ```
  mean = 历史窗口均值
  std  = 历史窗口总体标准差(population)
  z    = (observed - mean) / std
  异常 ⇔ |z| >= z_threshold
  ```

  `std == 0` 时：观测值等于恒定基线为正常，否则为异常。
- 无数据窗口 → 非异常（`NO_DATA`）。
- 历史非空窗口不足 `min_baseline` → 非异常（`INSUFFICIENT_BASELINE`）。
- 每条结论 `AnomalyResult` 都带 `reason` 与 `evidence`：基线均值、标准差、
  基线样本数、阈值、观测值、z-score 与规则描述，保证**可解释**。
- 同一版本 + 同一参数的报告 `AnalysisReport` 逐窗口可复现，报告内嵌
  `version_id` 与全部参数。

---

## 5. 并发、一致读取与资源边界

- **一致读取（快照隔离）**：每次分析在开始时解析出**一个不可变版本快照**，
  计算全程基于该快照。期间即使有新写入，当前报告仍绑定原版本（不会出现新旧混杂）；
  数据变更后的新查询显式或隐式指向新版本，不会返回旧结论。
- **写隔离**：存储用一把锁串行化写入，发布新版本是锁内原子动作；
  读路径读取不可变对象，无需持锁。
- **并发槽**：服务维护容量为 `max_concurrency`（默认 16）的信号量，
  **非阻塞获取，槽满立即返回 `RESOURCE_LIMIT_EXCEEDED`，不排队、不留下等待任务**；
  槽位在 `finally` 中释放，任何退出路径都不会泄漏给后续请求。
- **超时 / 取消**：
  - 每个请求可用 `timeout_s`（默认 10s）限定，超时返回 `TIMEOUT`（HTTP 504）；
    客户端取消返回 `CANCELLED`（HTTP 499），两者错误码可区分。
  - 写操作在锁内**原子提交**且按 `batch_id` 幂等：调用方超时/取消后拿不到结果时，
    用**同一个 `batch_id` 重试**即可，绝不会出现半写入状态或重复版本。
- **资源上限**：单批次点数 `max_points_per_batch`（默认 1,000,000）、
  单查询扫描点数 `max_points_per_query`（默认 5,000,000）、窗口数上限 100,000。
  超限在触碰存储/计算前即明确失败。

> 说明：CPU 计算在线程池执行；“取消/超时”对**尚未提交**的工作保证无副作用，
> 对已提交的写入由“原子提交 + 幂等键”保证可安全重试，因此不会留下重复任务或污染状态。

---

## 6. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/datasets/{dataset}/imports` | 导入新批次（幂等） |
| POST | `/datasets/{dataset}/appends` | 在最新版本上追加（body 可带 `base_version_id`） |
| POST | `/datasets/{dataset}/analyze` | 基于版本快照做聚合 + 异常检测 |
| GET  | `/datasets/{dataset}/versions/latest` | 最新版本快照信息 |
| GET  | `/datasets/{dataset}/versions/{version_id}` | 指定版本快照信息 |
| GET  | `/health` | 健康检查 |

导入请求示例：

```json
{
  "batch_id": "batch-2020-01-01-01",
  "schema": {"temp": "float"},
  "points": [
    {"ts": 1577836800000, "values": {"temp": 10.0}},
    {"ts": 1577836860000, "values": {"temp": 12.0}}
  ]
}
```

分析请求示例：

```json
{
  "metric": "temp",
  "method": "mean",
  "window": "1m",
  "interval": null,
  "z_threshold": 3.0,
  "min_baseline": 3
}
```

响应中 `aggregates` 为逐窗口稠密序列，`anomalies` 为逐窗口结论（含判定依据）。

---

## 7. 错误分类

错误响应统一为：

```json
{"error_code": "<CODE>", "message": "可读原因", "details": {"index": 0, "field": "temp"}}
```

| error_code | HTTP | 触发场景 |
| --- | --- | --- |
| `SCHEMA_FIELD_MISSING` | 422 | 缺 `ts` / `values` / schema 字段 |
| `SCHEMA_TYPE_MISMATCH` | 422 | 值类型不符、`int` 收到小数、`NaN/Inf`、布尔 |
| `INVALID_TIMESTAMP` | 422 | `ts` 非整数毫秒或超出 `[2000,2100)` |
| `DUPLICATE_TIMESTAMP` | 409 | 批次内或与父版本存在重复时间点 |
| `EMPTY_BATCH` | 422 | 空批次 |
| `INVALID_PARAMETER` | 400 | dataset/batch_id/window/range/阈值等非法 |
| `VERSION_NOT_FOUND` | 404 | 数据集或版本不存在 |
| `VERSION_CONFLICT` | 409 | 在非最新版本追加、追加时 schema 不一致 |
| `RESOURCE_LIMIT_EXCEEDED` | 413 | 批次/查询/窗口超限，或并发槽已满 |
| `TIMEOUT` | 504 | 超过 `timeout_s` |
| `CANCELLED` | 499 | 请求被取消 |
| `INTERNAL_ERROR` | 500 | 兜底 |

校验类错误的 `details` 通常带 `index`（数据行号）与 `field`，便于定位与区分。

---

## 8. 测试覆盖

| 文件 | 覆盖内容 |
| --- | --- |
| `tests/test_validation.py` | 字段缺失、类型不匹配、非法时间、重复时间点、空批次、全有或全无 |
| `tests/test_store.py` | 幂等导入无副作用、乱序排序、追加、冲突、快照不可变、结构演进、并发写 |
| `tests/test_windows_anomaly.py` | 网格对齐、乱序/重复点无关、空窗口不误报、缺失计数、z-score 判定与依据、可复现 |
| `tests/test_service.py` | 幂等、追加、版本切换一致读、并发读写隔离、资源上限、槽满拒绝、超时、取消、同 batch 重试 |
| `tests/test_api.py` | HTTP 状态码映射、错误码可区分、端到端导入/追加/版本切换 |

```bash
uv run pytest -s          # 含关键输入 / 版本快照 / 判定依据日志
uv run pytest             # 仅结果
```

## 9. 代码结构

```
app/
  errors.py      # 错误码与异常分类
  models.py      # Schema/DataPoint/版本快照/窗口聚合/异常结论/报告
  validation.py  # 批次与 schema 校验（可区分失败原因）
  store.py       # 版本化存储：幂等、追加、不可变快照、锁
  windows.py     # 窗口表达式/网格对齐/聚合/缺失值
  anomaly.py     # 滚动基线 z-score 判定与证据
  service.py     # 编排：资源边界、并发槽、超时/取消、一致快照读
  api.py         # FastAPI 路由与错误→HTTP 映射
main.py          # create_app() 入口
```
