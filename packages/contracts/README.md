# packages/contracts

跨语言数据契约的唯一权威来源。**契约优先**：先改这里并重新生成，再改实现。

## 布局

```text
proto/        Protobuf 服务/消息定义 (API 边界，Python↔Rust gRPC)
jsonschema/   JSON Schema (持久化与 LLM 边界，draft 2020-12)
fixtures/     跨语言一致性测试用的脱敏样本数据
generated/    生成产物 (git 忽略，由 `make contracts` 重建)
```

## 边界划分（见 CLAUDE.md 第 4 节）

- **API 契约** → Protobuf；前端客户端由 OpenAPI 生成。
- **持久化与 LLM 边界** → JSON Schema / Pydantic。
- 同一业务概念在所有语言使用相同名称与字段。

## 通用字段（common envelope）

所有领域事件/记录复用 `jsonschema/common.json` 中的 `$defs`：

```text
schema_version   契约版本
event_id         全局唯一事件 ID
correlation_id   跨服务关联同一业务流
causation_id     触发本事件的上游 event_id
session_id       交易会话 ID
occurred_at_utc  业务发生时间 (UTC, RFC3339)
received_at_utc  系统接收时间 (UTC, RFC3339)
source           来源系统/供应商
source_sequence  来源侧递增序号或去重键
rule_version     生成时的规则包版本
```

## 关键类型约束

- 时间：一律 UTC、RFC3339（带 `Z`）。ET 仅为派生展示字段。
- 金额/价格/Greeks：字符串表示的定点小数（避免 JSON number 的浮点误差），实现侧转 `numeric`/缩放整数。
- 期权合约主键：`underlying + expiry + strike + right + multiplier`。

## 首批冻结契约 (schema_version 1.0)

MarketSnapshot、OptionSnapshot、Signal、CandidateTradePlan、EventContext、DataHealth、BrokerHealth。
