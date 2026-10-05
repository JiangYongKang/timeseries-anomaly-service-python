# 时序数据版本化分析与异常检测 — 语义说明

## 1. 数据与版本语义

### 1.1 数据点与时间戳
- 每个数据点为一个对象，必含 `ts` 字段与若干指标/标签字段。
- 时间戳单位为**纳秒 epoch 整数**；导入时也接受 ISO-8601 字符串
  （如 `2024-01-01T00:00:00Z`），服务端统一转换为纳秒整数。
- 字段类型在首次导入时推断：`float / int / string / bool`；
  批次中出现过 `null` 的列被标记为 **nullable**。`int` 与 `float`
  混用时推断为 `float`（允许 int→float 拓宽）。

### 1.2 校验规则（失败原因可区分，见第 5 节）
- 缺字段：批次内某点缺少同批其他点拥有的列 → `missing_field`；
- 类型不匹配：值类型与推断/既有 schema 不符 → `type_mismatch`；
- 非法时间戳：缺失、空串、无法解析 → `invalid_timestamp`；
- 未知字段 / 空批次 / 非对象点：`unknown_field` / `empty_batch` / `type_mismatch`；
- 批内重复时间点：同一导入批次中出现相同 `ts` → `duplicate_timestamp`，
  乱序数据本身允许且不影响任何结论；
- 追加批次与当前版本存在时间点交集 → `duplicate_timestamp`。

### 1.3 版本不可变与幂等
- 每次成功导入（创建或追加）产生一个**单调递增的新版本**，版本快照
  （schema + 全部数据点）一经发布即不可变（frozen dataclass）。
- 历史数据点永不被改写。结构演进时新增列在合并 schema 中标记为
  **nullable**；旧点不包含该键，读取缺失即视为 `None`，因此旧版本
  与新版本都可独立、精确复现。
- 允许的结构演进：新增可空列；`int → float` 拓宽；追加批次可**省略
  既有可空列**（读取为 `None`），多方并发追加各自的可选维度因此可以
  任意顺序交错。省略**必填**列、删除列、收窄/其他类型变更 →
  `incompatible_schema`。
- 结构合并在数据集锁内基于**真实父版本**计算：并发演进时，后到的
  追加针对最新 schema 重新合并，发布版本的声明字段与实际数据必然
  完全对应，不会出现字段被悄悄丢掉的版本。
- 乐观并发令牌 `base_version`（可选）：声明本次追加基于的版本号
  （数据集不存在时为 0）。发布前在锁内比对，与当前最新版本不一致 →
  `stale_base_version`（409），请求被明确拒绝且不产生任何状态；
  调用方读取最新版本后可用同一批次号安全重试。
- 幂等键：
  - 显式 `batch_id`：同键 + 同内容哈希 → 返回原结果且 `replayed=true`，
    不产生新版本、不产生重复副作用；同键不同内容 →
    `duplicate_batch_conflict`；
  - 未提供 `batch_id`：以**规范化内容哈希**（键排序、紧凑 JSON）为键，
    完全相同的重复导入天然幂等；
  - 重放判断先于 `append` 标志检查，因此同一批次的重复重试总是安全的。

## 2. 窗口与聚合规则

- 固定窗口按 Unix epoch 对齐：
  `window_start = floor(ts / window_ns) * window_ns`，
  与数据到达顺序无关（乱序、重排产生完全相同结果）。
- 查询区间为半开区间 `[start_ts, end_ts)`；只输出**完整窗口**：
  `window_start >= start_ts` 且 `window_end <= end_ts`。
  不完整窗口（被起止边界截断）不参与任何统计或判定。
- 枚举窗口槽位时**包含无数据窗口**，避免“没有数据就不出现”的歧义。
- 聚合方法：`mean / sum / min / max / count`。
- 缺失值（`None`）跳过但保留窗口：窗口内无有效数值时聚合为 `None`。

## 3. 异常判定规则（基线 z-score）

基线 = 按时间顺序排在当前窗口**之前**、且有有效聚合值的完整窗口序列。

1. 窗口聚合为 `None`（无有效数据）→ `no_data`，**绝不误报**；
2. 基线窗口数 `< min_baseline_windows`（默认 3）→ `insufficient_data`；
3. 基线总体标准差为 0（恒定基线）：窗口值 ≠ 基线均值 → `anomaly`，
   `zscore=null`（除零保护）；相等 → `normal`；
4. 否则 `z = (v - mean) / std`（总体标准差，非样本标准差），
   `|z| > threshold`（默认 3.0）→ `anomaly`，否则 `normal`。

每个窗口都附带 `evidence`：窗口值、基线均值/标准差、z-score、阈值
与人类可读 `reason`，说明判定依据。同版本 + 同参数 + 同区间的
查询结果逐字节相等（可复现）。

## 3.1 分组分析（group_by）

- 查询可指定一个**分类维度字段** `group_by`（`string / int / bool`；
  `float` 不可枚举 → `invalid_group_field`；字段不存在 →
  `field_not_found`），把同一版本的数据拆成多条互不干扰的序列。
- 每条序列独立按窗口聚合、独立维护自己的历史基线并各自给出异常
  结论：某组的尖峰不会污染其他组的基线。
- 数据点缺少分组键或值为 `null`（常见于结构演进前的历史点）时，
  归入显式的 **`__missing__`** 组，有自己的窗口与基线，绝不悄悄
  漏掉。
- 某条序列的某个窗口无数据 → 该窗口 `no_data`，绝不误报；区间内
  没有任何数据点时分组列表为空，同样不产生任何异常结论。
- 分组输出按组键确定性排序（`__missing__` 固定在最后）：同版本 +
  同参数 + 同区间的分组结果逐字节可复现。

## 4. 并发与资源边界

- 每个数据集一把可重入锁：导入/追加在锁内**先完成全部校验**
  （schema、重复点、容量、取消/超时检查），**再一次性发布**版本、
  批次记录与 `current` 指针。任何异常路径都不会留下半写入版本或
  批次任务，后续请求可立即用相同批次号重试。
- 并发相同批次：串行化后只有第一个创建版本，其余全部命中重放。
- 读取无锁：查询固定到某个不可变版本快照（默认发布时的最新版本）。
  数据变更后新查询必然读到新版本；进行中的查询基于旧快照完成，
  不存在撕裂读。
- 资源上限（`ServiceConfig`）：
  - `max_points_per_batch`（默认 100,000）：超限 `resource_limit_exceeded`；
  - `max_points_per_dataset`（默认 1,000,000）：超限拒绝追加且不发布；
  - `max_query_windows`（默认 10,000）：单次查询的窗口总数上限，
    按 **窗口槽位数 × 分组数** 计算（不分组时分组数为 1），超限 →
    `query_budget_exceeded`（413）；
  - `max_query_points`（默认 500,000）：单次查询参与计算的数据点
    （落在查询区间内的点）上限，超限 → `query_budget_exceeded`（413）；
  - 查询预算在**计算前**裁决：单趟扫描完成落桶与计数后立即判定，
    超限明确失败，不会把请求拖住，也不产生任何副作用；
    预算错误（413）与参数写错（422）是不同错误族，可明确区分。
- 超时：`timeout`（秒）未显式指定时，`default_timeout_seconds`
  （默认 5.0）作为**真实生效的默认截止时间**；`cancel_event`
  （threading.Event）支持显式取消。两者在批次校验、分组落桶与
  逐窗口判定等检查点轮询：超时 → `timeout`（408），取消 →
  `cancelled`（499）；均为明确失败。导入因“全部校验通过后才发布”，
  失败不留半写入状态；查询只读快照，失败无任何副作用，后续请求
  不受影响。

## 5. 错误分类

| code | HTTP | 含义 |
| --- | --- | --- |
| `missing_field` | 422 | 数据点缺少必填字段 / 非空列给了 null |
| `type_mismatch` | 422 | 字段值类型不合法 |
| `invalid_timestamp` | 422 | 时间戳缺失或无法解析 |
| `unknown_field` / `empty_batch` | 422 | 未知列 / 空批次 |
| `incompatible_schema` | 409 | 非法结构演进（删列/省略必填列/类型不兼容） |
| `stale_base_version` | 409 | 追加基于已过时的版本结构（乐观并发冲突），可安全重试 |
| `duplicate_timestamp` | 409 | 批内或追加与既有版本时间点重复 |
| `duplicate_batch_conflict` | 409 | 同批次号不同载荷 |
| `dataset_exists` | 409 | 已存在数据集但未带 `append=true` |
| `dataset_not_found` / `version_not_found` | 404 | 数据集 / 版本不存在 |
| `field_not_found` | 422 | 查询字段不存在或非数值不可聚合 |
| `invalid_group_field` | 422 | 分组字段不可枚举（如 float 列） |
| `invalid_window` | 422 | 窗口参数非法（零/负长度、区间倒置、短于一个窗口） |
| `resource_limit_exceeded` | 413 | 超过批次/数据集点数上限 |
| `query_budget_exceeded` | 413 | 单次查询窗口总数 / 参与计算点数超预算 |
| `timeout` | 408 | 超过截止时间 |
| `cancelled` | 499 | 操作被显式取消 |

## 6. HTTP 接口

- `GET /health`：健康检查。
- `POST /datasets/{dataset}/import`：
  body `{points, batch_id?, append?, base_version?, timeout?}` →
  `ImportResult`。
- `POST /query`：
  body `{dataset, metric_field, start_ts, end_ts, window_seconds,
  version?, baseline_aggregate?, group_by?, timeout?}` → 窗口结论与证据；
  带 `group_by` 时响应为 `{..., group_field, groups: [{group_value,
  windows}]}`。
- `GET /datasets/{dataset}/versions`：版本号列表。

## 7. 本地验证

```bash
# 安装依赖（含 pytest / httpx）
uv sync --group dev

# 全量单测（-s 可查看关键输入、版本快照与判定依据日志）
uv run pytest
uv run pytest -s

# 启动服务
uv run uvicorn main:app --reload

# 示例：导入（纳秒时间戳；1 秒 = 1_000_000_000）
curl -s -XPOST localhost:8000/datasets/demo/import \
  -H 'content-type: application/json' \
  -d '{"batch_id":"b1","points":[
    {"ts":0,"x":10.0},{"ts":1000000000,"x":10.2},
    {"ts":2000000000,"x":9.8},{"ts":3000000000,"x":10.1},
    {"ts":4000000000,"x":99.0}]}'

# 查询 5 个 1 秒窗口（最后一个应判 anomaly，并带 z-score 证据）
curl -s -XPOST localhost:8000/query -H 'content-type: application/json' \
  -d '{"dataset":"demo","metric_field":"x",
       "start_ts":0,"end_ts":5000000000,"window_seconds":1}'

# 分组分析：追加带分类维度的可空列后按 dev 拆组
curl -s -XPOST localhost:8000/datasets/demo/import \
  -H 'content-type: application/json' \
  -d '{"batch_id":"b2","append":true,"points":[
    {"ts":5000000000,"x":10.3,"dev":"a"},{"ts":5000000001,"x":10.0,"dev":"b"},
    {"ts":6000000000,"x":10.1,"dev":"a"},{"ts":6000000001,"x":9.9,"dev":"b"},
    {"ts":7000000000,"x":88.0,"dev":"a"},{"ts":7000000001,"x":10.2,"dev":"b"}]}'
curl -s -XPOST localhost:8000/query -H 'content-type: application/json' \
  -d '{"dataset":"demo","metric_field":"x","group_by":"dev",
       "start_ts":0,"end_ts":8000000000,"window_seconds":1}'
# 响应含 dev=a / dev=b / __missing__ 三组，各自独立基线；
# 只有 dev=a 的最后一窗判 anomaly

# 乐观并发：基于过期版本号追加会被 409 stale_base_version 拒绝
curl -s -XPOST localhost:8000/datasets/demo/import \
  -H 'content-type: application/json' \
  -d '{"batch_id":"b3","append":true,"base_version":1,
       "points":[{"ts":8000000000,"x":1.0}]}'
```

查询预算可用 `ServiceConfig(max_query_windows=..., max_query_points=...)`
调整；默认超时用 `ServiceConfig(default_timeout_seconds=...)` 调整。

## 8. 测试覆盖矩阵

| 文件 | 覆盖点 |
| --- | --- |
| `tests/test_import_idempotency.py` | 重复导入幂等、冲突原因、缺字段/类型/时间戳、结构演进 |
| `tests/test_windows_anomaly.py` | 乱序、重复时间点、完整窗口、缺失值、无数据不误报、z-score 证据、可复现 |
| `tests/test_grouped_query.py` | 分组拆分、独立基线、缺分类值显式归属、空窗口/空序列不误报、结论可复现 |
| `tests/test_query_budget.py` | 窗口/点数预算、预算与参数错误可区分、默认超时生效、失败无污染 |
| `tests/test_schema_concurrency.py` | 并发追加结构演进自洽、任意顺序等价、过期 base_version 拒绝与重试 |
| `tests/test_versioning.py` | 版本固定查询、版本切换、指针原子性、失败无新版本 |
| `tests/test_concurrency.py` | 并发重复导入、并发追加串行化、重叠时间点、并发一致快照读 |
| `tests/test_timeout_cancel.py` | 超时、取消、资源上限、无污染与后续请求可用 |
| `tests/test_api.py` | HTTP 错误码区分、幂等重放、并发 HTTP、版本切换后查询、分组/预算/过期版本 |
