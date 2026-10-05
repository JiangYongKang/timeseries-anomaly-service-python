# 时序数据版本化分析与异常检测服务

基于 FastAPI 的本地可运行服务，支持时序数据集的**版本化导入/追加**、
按时间窗口的**聚合与异常检测**、按分类维度的**分组分析**，并保证
重复导入、乱序数据、版本切换与并发场景下结果一致、可解释、状态无污染。

- 分组分析：指定分类字段把同一版本拆成多条序列，各自聚合、各自维护
  基线；缺分类值的点归入显式 `<missing>` 分组，空序列/空窗口不误报。
- 资源边界：单次查询的窗口总数（槽位 × 分组数）与数据点预算可配置，
  超限即 `query_budget_exceeded`（413）快速失败；未指定超时时套用
  默认截止时间（`default_timeout_seconds`）。
- 并发结构演进：多方同时追加新可选列时版本必然自洽；基于过时结构的
  追加以 `stale_schema`（409）明确拒绝，刷新后可安全重试。
- 完整语义（数据与版本、窗口、分组、异常规则、并发与资源边界、
  错误分类、本地验证步骤）：见 [`docs/SEMANTICS.md`](docs/SEMANTICS.md)
- 运行测试：`uv sync --group dev && uv run pytest`（加 `-s` 查看
  关键输入、版本快照与判定依据日志）
- 启动服务：`uv run uvicorn main:app --reload`

## 快速体验

```python
from anomaly_service import AnomalyService

NS = 1_000_000_000
svc = AnomalyService()
svc.import_data("demo", [{"ts": i * NS, "x": 10.0} for i in range(4)],
                batch_id="b1")
svc.import_data("demo", [{"ts": 4 * NS, "x": 99.0}],
                batch_id="b2", append=True)
result = svc.query("demo", "x", 0, 5 * NS, 1)
for w in result.windows:
    print(w.status.value, w.aggregate, w.evidence.reason if w.evidence else "")

# 分组分析：按设备维度拆序列，各自独立基线
svc.import_data("g", [{"ts": i * NS, "x": 10.0, "dev": "a"} for i in range(4)]
                + [{"ts": i * NS + 1, "x": 20.0, "dev": "b"} for i in range(4)]
                + [{"ts": 4 * NS, "x": 99.0, "dev": "a"}],
                batch_id="g1")
grouped = svc.query_grouped("g", "x", "dev", 0, 5 * NS, 1)
for g in grouped.groups:
    print(g.group_key, [w.status.value for w in g.windows])
```
