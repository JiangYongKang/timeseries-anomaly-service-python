# 时序数据版本化分析与异常检测服务

基于 FastAPI 的本地可运行服务，支持时序数据集的**版本化导入/追加**、
按时间窗口的**聚合与异常检测**，并保证重复导入、乱序数据、版本切换与
并发场景下结果一致、可解释、状态无污染。

- 完整语义（数据与版本、窗口、异常规则、并发与资源边界、错误分类、
  本地验证步骤）：见 [`docs/SEMANTICS.md`](docs/SEMANTICS.md)
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
```
