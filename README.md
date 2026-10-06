# 临床实验室质量控制与结果拦截

只使用Python标准库和SQLite的模块化服务，默认端口`8339`。覆盖检测项目、质控品批次、质控规则、允许范围、仪器校准、连续偏差、趋势、失控、结果拦截、复测、调查、批次切换、历史更正和断网暂存回传。

## 断网暂存回传

仪器断网时质控结果留在机内，回网后通过 `POST /api/instruments/<id>/sync` 批量回传。机内序号每次上电都会重来，因此回传必须带本次会话号 `session_id`；服务端以 `(instrument_id, session_id, sequence_no)` 唯一去重，两个操作员同时提交同一回传也只会入库一次。

- 补录按观测时刻（`run_at`）插回时间线，不按到达顺序。
- 每来一条补录就重算该仪器同批次质控的完整时间线（`recompute`），连续偏差/趋势判定不再被打乱。
- 判定一变，依据变了的已放行批次自动退回拦截（`recall`），审计可查。
- 失败记录单独落库为 `failed`，重试只补未完成部分，已应用记录跳过。

## 模块结构

- `app.py`：参数解析、依赖组装和服务生命周期。
- `src/domain.py`：角色、领域异常和数据对象。
- `src/rules.py`：QC规则计算、状态机、校准与放行约束。
- `src/repository.py`：SQLite持久化、乐观锁、幂等、暂存会话与断点续传。
- `src/service.py`：用例编排、权限校验、时间线重算与批次召回。
- `src/http_api.py`：JSON接口和统一错误响应。
- `src/audit.py`：操作审计。
- `static/index.html`：最小演示页面。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8339
```

## 核心对象

`assay`为检测项目，`qc_lot`为质控品批次，`instrument`为仪器，`qc_run`为质控结果，`result_batch`为患者结果批次。

## 接口

- `GET /health`
- `GET /api/<kind>`，可用`?status=`过滤
- `GET /api/entities/<id>`
- `POST /api/<kind>`
- `POST /api/entities/<id>/actions`
- `POST /api/instruments/<id>/sync`，断网暂存回传，请求体 `{"session_id": "...", "results": [...]}`
- `GET /api/instruments/<id>/sync?session_id=...`，查询某会话各序号的入库进度
- `GET /api/instruments/<id>/sessions`，列出仪器的回传会话
- `POST /api/instruments/<id>/recompute`，按观测时刻重算该仪器全部质控判定并召回受影响的已放行批次
- `GET /api/audit`

身份通过`X-User-Id`和`X-Role`请求头传入。可选`Idempotency-Key`防止重复创建。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

质控规则为可运行的简化模型，包含1-3s、连续偏移和趋势检查，但不替代CLIA、ISO 15189、Westgard完整规则集或实验室信息系统接口。
