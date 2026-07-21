# ADR 0004：RiskExecutionService 模块边界

## 状态

已接受（2026-07-22）

## 背景

`risk_grpc.rs` 曾同时承担 gRPC trait、协议映射、最终风控编排、确认/提交、取消、执行订单对账、
Broker 账户对账、恢复和测试，达到 3442 行。逻辑虽有回归测试保护，但修改一个流程时需要理解
多个不相干职责，增加评审和冲突成本。

## 决策

保留 `risk_grpc.rs` 作为稳定公共入口，`main.rs` 的导入和 protobuf 服务注册不变。内部按职责拆分：

- `backend.rs`：执行后端配置、paper 路由和 opt-in 校验。
- `candidate.rs`：候选计划评估与 Stage。
- `confirmation.rs`：人工确认、Final Risk 复核与 Broker 提交。
- `orders.rs`：订单查询与取消。
- `reconciliation.rs`：执行订单、Broker 账户和重启恢复对账。
- `mapping.rs`：protobuf/domain 转换、摘要和只读投影。
- `tests.rs`：跨模块行为回归；通过公共 gRPC trait 验证，不绕过入口。

入口模块只保留共享 authority/workflow 类型、依赖组合、Final Risk 原语和薄 RPC 委托。子模块可访问
父模块私有状态，但不得导出新的交易入口。

## 不变量

- protobuf 契约、服务名、公共 Rust 类型和 `main.rs` 接线不变。
- Confirm 仍必须重跑 Final Risk；LLM 或人工输入不能覆盖 Rust 结论。
- unknown submit/cancel、残余敞口和未完成对账仍保持 sticky fail-closed。
- 拆分前的 24 项 `risk_grpc` 场景及全仓 Rust/跨语言测试必须逐项保持通过。

## 后果

单个生产模块降到约 760 行以内，流程所有权更明确。`repository.py`、`longbridge.rs` 与
`broker_registry.rs` 仍偏大，后续应采用相同的“稳定门面 + 职责模块”方式拆分；本 ADR 不授权
在结构重构中改变其交易语义。
