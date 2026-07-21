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
3. 人工确认能力使用绑定 `order_id + plan_hash + TTL` 的随机令牌。令牌以 Fernet
   authenticated ciphertext 写入 PostgreSQL 的短期 capability store，密钥仅由
   `OPTIONTRADER_CONFIRMATION_FERNET_KEY` 注入进程，不写数据库、不返回浏览器、不进日志。
   Confirm 在记录人工意图的同一事务内锁行并一次性 claim；不确定的 gRPC 结果不释放
   claim，必须先向 Rust 对账。
4. PostgreSQL 在同一事务记录候选计划、初始风险、订单投影与不可变审计；确认意图先
   落库，再跨越 Rust 提交边界。数据库不可用时不调用 Rust。
5. `REPLAY/SHADOW` 永不调用 Broker；`PAPER/MANUAL_CONFIRM` 当前只调用确定性内存
   PaperBroker；`CONTROLLED_AUTO` 和真实提交保持禁用。
6. Broker adapter 请求必须携带一至四条完整期权腿。每腿合约唯一、方向明确、数量与
   组合单位一致；重复 idempotency key 只有在 hash、订单方向/类型、提交价和全部腿完全
   一致时才返回原单。
7. Longbridge 原生 Rust adapter 与 IBKR 本机 sidecar 服从同一领域语义。跨进程 sidecar
   契约定义在 `broker.proto`，覆盖账户、持仓、订单、成交、提交、撤单和对账；当前仅
   契约落地，不代表真实账户已连接或通过 paper 认证。
8. 风险参数默认 `UNCONFIRMED`，购买力默认 0。只有显式确认限额、指定规则版本、市场与
   Broker 双健康且完成对账时才可能允许新开仓。HTTP 与 gRPC 默认只监听回环地址。
9. 确认 capability store 由 PostgreSQL 共享，不依赖 API worker 本地内存。订单投影把
   Rust 单调 `state_version` 存为独立列；PostgreSQL 通过行锁与条件版本更新拒绝并发回退，
   React 跨不可用窗口保留 last-known 锚点并拒绝旧版本、成交量回退或同版本冲突。
10. Rust workflow 原地推进订单，不在可失败步骤前从账本 remove。内部不一致会保留订单
    并进入 `RECONCILE_PENDING`，以便后续人工或 Broker 对账恢复。
11. CandidateTradePlan 1.1 为每条腿携带报价时间、bid/ask size、Greeks 和 chain snapshot
    proof。Rust 在 Stage/Confirm 检查报价时效、点差、Greeks、策略白名单和美东开仓窗口；
    市价新开仓固定拒绝。
12. 自适应限价由 Rust 计算并受原计划保护价约束。adapter 只做确定性映射，不得用坏报价
    退化到 touch/market。Longbridge 多腿 fail closed；IBKR 多腿使用 BAG。

## 当前限制

- Rust workflow 和 PaperBroker 仍为进程内状态。重启后保留 PostgreSQL 投影但不自动
  重建执行事实，系统会闭锁并要求对账。
- capability 密文只有持有同一 Fernet 密钥的 API 实例可解密；缺失、错误或轮换不当均
  fail closed。密钥轮换与多密钥解密尚未实现。
- Broker snapshot 尚未成为账户风险字段的动态来源；首个切片仍由启动配置注入。
- Candidate 已携带并由 Rust 校验期权证明，但证明仍来自候选输入；Rust 直连的实时期权报价
  权威源尚未落地，因此不能把该校验视为 live Gate。
- Longbridge/IBKR SDK 映射已开始，订单/成交全量流、sidecar gRPC、自动重启对账、持仓管理
  和保护性退出仍待后续切片。

## 后果

首个切片可以确定性验证候选计划、两阶段风控、人工确认、paper 幂等和审计链，同时不
具备误触实盘的通路。代价是重启或执行事实不一致时需要保持 No Trade，直到后续对账
服务用 Broker 权威快照完成恢复。
