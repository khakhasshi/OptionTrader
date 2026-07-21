# ADR 0002：Phase 3 半自动执行边界

- 状态：Accepted（首个切片；真实 Broker 适配与恢复仍在进行）
- 日期：2026-07-21

## 背景

Phase 3 要把 Python 产生的候选交易安全地送入 Rust 权威风控，并在人工确认后进入
shadow 或 paper 执行。系统的第一目标仍是阻止错误交易，因此确认、服务重启、重复
请求、账户数据缺失和券商断线都必须 fail closed。

## 决策

1. Python 使用确定性 Protobuf 序列化计算 `plan_hash` 和 `idempotency_key`；Rust 重新
   计算并校验，不信任调用方提供的 hash。
2. Rust 在 Stage 和 Confirm 两处执行相同的权威风险检查。Confirm 会重新读取最新市场、
   事件、账户、规则版本与限额；人工确认不能覆盖第二次结果。
3. 人工确认能力使用绑定 `order_id + plan_hash + TTL` 的随机令牌。令牌只保存在
   Application API 进程内存，不写 PostgreSQL、不返回浏览器、不进日志。任一服务重启
   后无法证明原确认能力时，返回 reconciliation required，禁止重新创建或提交订单。
4. PostgreSQL 在同一事务记录候选计划、初始风险、订单投影与不可变审计；确认意图先
   落库，再跨越 Rust 提交边界。数据库不可用时不调用 Rust。
5. `REPLAY/SHADOW` 永不调用 Broker；`PAPER/MANUAL_CONFIRM` 当前只调用确定性内存
   PaperBroker；`CONTROLLED_AUTO` 和真实提交保持禁用。
6. Broker adapter 请求必须携带一至四条完整期权腿。每腿合约唯一、方向明确、数量与
   组合单位一致；重复 idempotency key 只有在 hash、限价和全部腿完全一致时才返回原单。
7. Longbridge 原生 Rust adapter 与 IBKR 本机 sidecar 服从同一领域语义。跨进程 sidecar
   契约定义在 `broker.proto`，覆盖账户、持仓、订单、成交、提交、撤单和对账；当前仅
   契约落地，不代表真实账户已连接或通过 paper 认证。
8. 风险参数默认 `UNCONFIRMED`，购买力默认 0。只有显式确认限额、指定规则版本、市场与
   Broker 双健康且完成对账时才可能允许新开仓。HTTP 与 gRPC 默认只监听回环地址。
9. Application API 的确认能力是进程内状态，因此当前部署强制单 worker；常见 worker
   环境变量不是整数 1 时启动失败。订单投影携带 Rust 单调 `state_version`，PostgreSQL
   与 React 都拒绝旧版本或同版本冲突，迟到 HTTP 请求再由前端 request generation 丢弃。
10. Rust workflow 原地推进订单，不在可失败步骤前从账本 remove。内部不一致会保留订单
    并进入 `RECONCILE_PENDING`，以便后续人工或 Broker 对账恢复。

## 当前限制

- Rust workflow 和 PaperBroker 仍为进程内状态。重启后保留 PostgreSQL 投影但不自动
  重建执行事实，系统会闭锁并要求对账。
- Broker snapshot 尚未成为账户风险字段的动态来源；首个切片仍由启动配置注入。
- 当前市场快照不能独立证明每条期权腿的 quote age、spread、Greeks 与 chain 完整性；
  在这些证明进入 Rust 前，不得把本切片视为 live Gate。
- 真实 Longbridge/IBKR adapter、订单事件流、持仓管理和保护性退出仍待后续切片。

## 后果

首个切片可以确定性验证候选计划、两阶段风控、人工确认、paper 幂等和审计链，同时不
具备误触实盘的通路。代价是重启或执行事实不一致时需要保持 No Trade，直到后续对账
服务用 Broker 权威快照完成恢复。
