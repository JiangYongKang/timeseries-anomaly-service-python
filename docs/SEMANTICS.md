# 时序数据版本化分析与异常检测 — 语义说明

## 1. 数据与版本语义

### 1.1 数据点与时间戳
- 每个数据点为一个对象，必含 `ts` 字段与若干指标/标签字段。
- 时间戳单位为**纳秒 epoch 整数**；导入时也接受 ISO-8601 字符串
  （如 `2024-01-01T00:00:00Z`），服务端统一转换为纳秒整数。
- 字段类型在首次导入时推断：`float / int / string / bool`；
  批次中出现过 `null` 的列被标记为 **nullable**。`int` 与 `float`
  混用时推断为 `float`（允许 int→float 拓宽）。

### 1.2 校验规则（失败原因可区分，见第 6 节）
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
- 允许的结构演进：新增可空列；`int → float` 拓宽。删除既有列、
  收窄/其他类型变更 → `incompatible_schema`。
- 并发演进兼容范围：多方同时追加各自的新可选列时，schema 合并在
  数据集锁内针对真实父版本进行，最终版本必然自洽；基于过时结构
  （缺少当前版本已声明的列）的追加 → `stale_schema`，刷新结构后
  用相同批次号重试即可（详见第 5 节）。
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

## 3. 分组分析（按维度拆序列）

`query_grouped` 在单序列查询的基础上，指定一个**分类字段**（`group_field`），
把同一版本的数据拆成多条互不干扰的序列：

- 分组字段必须是该版本 schema 中的 `string / int / bool` 列；
  数值列或不存在列 → `invalid_group_field`（422）。
- 每条分组序列**独立**按窗口聚合、**独立**维护自己的历史基线并各自给出
  异常结论；某条序列的突增不会污染其他序列的基线。
- 分组值为 `null`（含结构演进前历史点缺该列）的数据点统一归入显式的
  `<missing>` 分组（`MISSING_GROUP_KEY`），**绝不静默丢弃**；该分组同样
  走完整的窗口与基线流程。
- 某条序列整体无数据时不出现在结果中；序列内某个窗口无数据时判
  `no_data`，基线窗口不足判 `insufficient_data`，**绝不误报 anomaly**。
- 输出分组按确定顺序排列（`<missing>` 固定最前，其余按类型名 + 值排序），
  与数据点到达顺序无关；同版本 + 同参数 + 同区间的分组结果逐字节相等
  （可复现）。

## 4. 异常判定规则（基线 z-score）
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

## 5. 并发与资源边界

- 每个数据集一把可重入锁：导入/追加在锁内**先完成全部校验**
  （schema、重复点、容量、取消/超时检查），**再一次性发布**版本、
  批次记录与 `current` 指针。任何异常路径都不会留下半写入版本或
  批次任务，后续请求可立即用相同批次号重试。
- 并发相同批次：串行化后只有第一个创建版本，其余全部命中重放。
- 并发结构演进：追加时的 schema 合并在**数据集锁内针对真实父版本**
  进行。多方同时追加、各自携带新的可选维度时，无论请求以何种顺序
  交错，发布的版本必然自洽（声明字段与实际数据完全对应，没有任何
  点的字段被悄悄丢掉）。若追加请求基于已过时的结构发出（当前版本
  已声明它不知道的列），以 `stale_schema`（409）明确拒绝；拒绝不
  留任何状态，刷新结构后用相同批次号重试即可，其他请求不受影响。
- 读取无锁：查询固定到某个不可变版本快照（默认发布时的最新版本）。
  数据变更后新查询必然读到新版本；进行中的查询基于旧快照完成，
  不存在撕裂读。
- 资源上限（`ServiceConfig`）：
  - `max_points_per_batch`（默认 100,000）：超限 `resource_limit_exceeded`；
  - `max_points_per_dataset`（默认 1,000,000）：超限拒绝追加且不发布；
  - `max_query_windows`（默认 10,000）：单次查询的窗口总数上限，
    分组查询按 **窗口槽位 × 分组数** 计；
  - `max_query_points`（默认 500,000）：单次查询参与计算的数据点上限；
  - 查询预算在任何重计算之前检查，超限即 `query_budget_exceeded`
    （413）快速失败，与参数写错（422）明确区分，不会拖住服务。
- `timeout`（秒）与 `cancel_event`（threading.Event）在批次校验与
  发布前检查点轮询：超时 → `timeout`，取消 → `cancelled`；
  两者均为明确失败，且因“全部通过后才发布”，不产生任何半写入状态。
  未显式指定 `timeout` 时套用 `default_timeout_seconds`（默认 5 秒）
  作为真实生效的默认截止时间。

## 6. 错误分类

| code | HTTP | 含义 |
| --- | --- | --- |
| `missing_field` | 422 | 数据点缺少必填字段 / 非空列给了 null |
| `type_mismatch` | 422 | 字段值类型不合法 |
| `invalid_timestamp` | 422 | 时间戳缺失或无法解析 |
| `unknown_field` / `empty_batch` | 422 | 未知列 / 空批次 |
| `incompatible_schema` | 409 | 非法结构演进（删列/类型不兼容） |
| `stale_schema` | 409 | 追加基于过时结构（并发演进冲突），刷新后可安全重试 |
| `duplicate_timestamp` | 409 | 批内或追加与既有版本时间点重复 |
| `duplicate_batch_conflict` | 409 | 同批次号不同载荷 |
| `dataset_exists` | 409 | 已存在数据集但未带 `append=true` |
| `dataset_not_found` / `version_not_found` | 404 | 数据集 / 版本不存在 |
| `field_not_found` | 422 | 查询字段不存在或非数值不可聚合 |
| `invalid_group_field` | 422 | 分组字段不存在或不是 string/int/bool 分类列 |
| `invalid_window` | 422 | 窗口参数非法（零/负长度、区间倒置、短于一个窗口） |
| `resource_limit_exceeded` | 413 | 超过批次/数据集点数上限 |
| `query_budget_exceeded` | 413 | 单次查询超出窗口总数/数据点预算 |
| `timeout` | 408 | 超过截止时间 |
| `cancelled` | 499 | 操作被显式取消 |

## 7. HTTP 接口

- `GET /health`：健康检查。
- `POST /datasets/{dataset}/import`：
  body `{points, batch_id?, append?, timeout?}` → `ImportResult`。
- `POST /query`：
  body `{dataset, metric_field, start_ts, end_ts, window_seconds,
  version?, baseline_aggregate?, timeout?}` → 窗口结论与证据。
- `POST /query/grouped`：
  body `{dataset, metric_field, group_field, start_ts, end_ts,
  window_seconds, version?, baseline_aggregate?, timeout?}` →
  按分组维度拆分的多序列窗口结论与证据。
- `GET /datasets/{dataset}/versions`：版本号列表。

## 8. 本地验证

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

# 分组分析：按设备维度拆序列（dev 为分类列；缺分类值的点归入 <missing>）
curl -s -XPOST localhost:8000/datasets/demo2/import \
  -H 'content-type: application/json' \
  -d '{"batch_id":"g1","points":[
    {"ts":0,"x":10.0,"dev":"a"},{"ts":1,"x":20.0,"dev":"b"},
    {"ts":1000000000,"x":10.0,"dev":"a"},{"ts":1000000001,"x":20.0,"dev":"b"},
    {"ts":2000000000,"x":10.0,"dev":"a"},{"ts":2000000001,"x":20.0,"dev":"b"},
    {"ts":3000000000,"x":10.0,"dev":"a"},{"ts":3000000001,"x":20.0,"dev":"b"},
    {"ts":4000000000,"x":99.0,"dev":"a"},
    {"ts":4000000001,"x":20.1,"dev":null}]}'
curl -s -XPOST localhost:8000/query/grouped -H 'content-type: application/json' \
  -d '{"dataset":"demo2","metric_field":"x","group_field":"dev",
       "start_ts":0,"end_ts":5000000000,"window_seconds":1}'
```

## 9. 测试覆盖矩阵

| 文件 | 覆盖点 |
| --- | --- |
| `tests/test_import_idempotency.py` | 重复导入幂等、冲突原因、缺字段/类型/时间戳、结构演进 |
| `tests/test_windows_anomaly.py` | 乱序、重复时间点、完整窗口、缺失值、无数据不误报、z-score 证据、可复现 |
| `tests/test_versioning.py` | 版本固定查询、版本切换、指针原子性、失败无新版本 |
| `tests/test_concurrency.py` | 并发重复导入、并发追加串行化、重叠时间点、并发一致快照读 |
| `tests/test_timeout_cancel.py` | 超时、取消、资源上限、无污染与后续请求可用 |
| `tests/test_grouped_query.py` | 分组可复现、独立基线、缺分类值归属、空序列/空窗口不误报、版本固定 |
| `tests/test_query_budget.py` | 窗口/数据点预算、分组预算翻倍、默认超时生效、超限后状态无污染 |
| `tests/test_schema_concurrency.py` | 并发追加结构演进自洽、过时结构明确拒绝、拒绝后重试与他人不受影响 |
| `tests/test_api.py` | HTTP 错误码区分、幂等重放、并发 HTTP、版本切换后查询 |
| `tests/test_api_grouped.py` | 分组查询端点、stale_schema→409、query_budget_exceeded→413 |
