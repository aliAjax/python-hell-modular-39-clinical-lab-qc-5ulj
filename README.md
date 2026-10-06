# 临床实验室质量控制与结果拦截

只使用Python标准库和SQLite的模块化服务，默认端口`8339`。覆盖检测项目、质控品批次、质控规则、允许范围、仪器校准、连续偏差、趋势、失控、结果拦截、复测、调查、批次切换和历史更正。

## 模块结构

- `app.py`：参数解析、依赖组装和服务生命周期。
- `src/domain.py`：角色、领域异常和数据对象。
- `src/rules.py`：QC规则计算、状态机、校准与放行约束。
- `src/repository.py`：SQLite持久化、乐观锁、幂等和审计查询。
- `src/service.py`：用例编排、权限校验和版本控制。
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
- `GET /api/audit`

身份通过`X-User-Id`和`X-Role`请求头传入。可选`Idempotency-Key`防止重复创建。

## 仪器断网暂存与回传

仪器断网期间质控结果留在机内，回网后批量回传。机内序号每次上电都会从0重来，因此记录身份为`(仪器, 本次会话号, 机内序号)`，回传必须带`session_id`。

- `POST /api/instruments/<instrument_id>/qc-buffer-uploads`：请求体`{"session_id": "...", "records": [{"seq": 0, "assay_id", "qc_lot_id", "value", "run_at"}, ...]}`，角色为`operator`及以上。
- `GET /api/instruments/<instrument_id>/qc-buffer-uploads`：列出该仪器的回传会话。
- `GET /api/instruments/<instrument_id>/qc-buffer-uploads/<session_id>`：查看会话状态与断点（`status`、`last_seq`、`total`、`last_error`）。

语义：

- 每条记录独立提交并推进`last_seq`检查点；中途失败后用同一会话号和相同载荷重试，已入序号自动跳过，从断点继续。
- 两个操作员同时提交同一台仪器的同一会话不会入库两次：写入在数据库事务内按`(instrument_id, session_id, seq)`去重，重复序号但内容不同返回`409 ConflictError`。
- 补录按观测时刻`run_at`插回时间线（而非到达顺序），连续偏差与趋势判定基于完整时间线计算。
- 每次回传后级联重算该仪器各质控品批次的全部已判定运行；结论或标志变化时覆盖判定、版本递增并写`cascade_evaluate`审计。
- 已放行的`result_batch`，若其依据的质控运行由accepted翻转为rejected，自动退回`intercepted`（审计动作为`intercept`，操作者为`system:cascade`，`data.pullback`记录翻转依据），需按既有流程复测/调查后才能放行后续批次。
- 手工`correct`一条质控结果同样触发上述级联重算。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

质控规则为可运行的简化模型，包含1-3s、连续偏移和趋势检查，但不替代CLIA、ISO 15189、Westgard完整规则集或实验室信息系统接口。
