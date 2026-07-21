# ADR 0002：Phase 3 半自动执行边界

- 状态：Accepted（Phase 3 开发基线；真实 Broker 现场认证仍在进行）
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
   该变量是逗号分隔 key ring，首 key 加密、其余 key 仅解密；启动时在单一数据库事务内
   将 legacy-key capability 轮换到首 key，首 key 密文不重写，任一密文损坏则整体回滚。Confirm 在记录人工意图的
   同一事务内锁行并一次性 claim；不确定的 gRPC 结果不释放 claim，必须先向 Rust 对账。
4. PostgreSQL 在同一事务记录候选计划、初始风险、订单投影与不可变审计；确认意图先
   落库，再跨越 Rust 提交边界。数据库不可用时不调用 Rust。
5. `REPLAY/SHADOW` 永不调用 Broker；`PAPER/MANUAL_CONFIRM` 默认调用确定性内存
   PaperBroker。IBKR/Longbridge 外部 paper 路由必须同时满足 `OPTIONTRADER_ENV=paper`、
   全局显式 opt-in、Broker 专属 paper/submission 开关，并与唯一持续对账 Broker 相同；
   任一条件缺失即拒绝启动。`CONTROLLED_AUTO` 与 live 提交在 Phase 3 无条件禁用。
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
11. CandidateTradePlan 1.3 为每条腿携带报价时间、bid/ask size、Greeks、chain snapshot
    proof 与 `provider=THETADATA`，计划级 `market_data_provider` 也固定为 `THETADATA`；二者
    都进入确定性 hash。Python 构造和 Rust Stage/Confirm 分别拒绝其他来源。`position_effect`
    明确区分 OPEN/CLOSE；市价新开仓固定拒绝，市价仅允许单腿、持仓可证明减少的保护性 CLOSE。
12. 自适应限价由 Rust 计算并受原计划保护价约束。adapter 只做确定性映射，不得用坏报价
    退化到 touch/market。IBKR 多腿使用 BAG；Longbridge 按 Rust 计算的每腿价格受控拆腿，
    所有 BUY 腿完整成交后才允许 SELL，partial/unknown 停止并投影残仓或要求对账。
13. ExecutionOrder 1.1 携带完整子单投影。任何部分成交或 active/unknown 子单都必须显式
    形成残余敞口；持久化层禁止成交量回退、子单消失，以及没有 FILLED 或零成交终态证明的
    residual true→false。操作响应复用同一 state_version 但内容冲突时，前端要求对账。
14. ThetaData SDK bridge 为 exact-contract quote/size/Greeks 生成内容寻址的 chain snapshot id。
    Rust 持有可信 registry，在 Stage 与 Confirm 对候选 proof 做逐字段复核。Standard entitlement
    的 first-order 端点没有 Gamma，二阶端点需要 Professional；因此 Gamma 只能从同一 ThetaData
    first-order Delta/IV/underlying/time 确定性推导，时间不同步时拒绝，不允许 Broker 数据回填。
15. PostgreSQL 是 workflow 重启真相。Rust `RestoreWorkflow` 原子校验全部 identity/hash/version：
    未 claim 且未过期的确认能力可恢复；终态保持；其余非终态提升为 ReconcilePending 并增加版本。
    恢复不触发提交，且返回明确的 Broker 对账清单。
16. IBKR sidecar 只绑定 loopback，并在账户、持仓、未结订单、成交四个 snapshot end callback
    全部完成前保持未对账。未知活动订单会使账户对账失败。Longbridge reconcile 同步读取账户、
    持仓、全部活动订单和当日成交；未知活动订单或无法归属的成交 fail closed。
17. 全账户事实对账采用 Rust 签发的两阶段快照凭证：Begin 先把共享 BrokerAuthority 置为
    RECONCILING，再返回经过结构/时效验证的 BrokerSnapshot protobuf 字节、sequence、SHA-256 与
    15 秒 TTL；Python 原子持久化并核对本地订单后 Commit。哈希/序号漂移、过期、数据库失败、
    任一 mismatch 或未解决 workflow 均不得恢复 HEALTHY。该路径只读，不具有提交能力。
18. Longbridge 只读恢复 authority 直接使用官方 Rust SDK，并固定 `submission_enabled=false`。它仅在
    native id、OptionTrader remark、symbol、side、quantity、原生订单类型和提交价格完全一致时
    认领持久化订单；拆腿逐 leg 唯一匹配。随后把 SDK 账户、持仓、订单和当日成交投影到统一
    BrokerSnapshot，复用第 17 条的 hash 两阶段持久化协议。独立 mutation adapter 在重启后撤单前
    必须用同一 durable request/native id 完成只读身份重绑定；绑定失败不得恢复订单或执行撤单。
    写侧每次提交前必须先完成自身的全量只读 reconcile，未知活动单或无法归属的成交阻止 submit。
19. 所有计划、风控、订单、成交、对账和失败状态转换都在原事务写入确定性 outbox event。
    PostgreSQL outbox 提供 `SKIP LOCKED` 租约、at-least-once event id、ack、退避重试和 dead letter；
    第一阶段不引入 NATS，数据库仍是事实源，未来消费者必须按 event id 去重。
20. 外部 paper 提交和撤单不得持有 workflow 全局锁执行网络 I/O。返回值必须逐字段绑定原请求；
    超时、断线或无法证明是否提交成功统一进入 ReconcilePending/残余敞口，并关闭 Broker authority，
    不得把 unknown 当作 rejected，也不得自动重发。
21. 保护性 CLOSE 仍要求新鲜 ThetaData exact-contract proof、Broker HEALTHY+已对账、当前规则版本、
    计划 hash/TTL 与最新已提交持仓事实；每腿必须反向且数量不超过对应 Broker native contract 净持仓。
    CLOSE 可绕过开仓专属的事件窗口、策略白名单、购买力、日损/次数/冷却与 kill switch，但不能绕过
    数据、Broker、身份或持仓证明。多腿仍限价执行，不允许以市价拆腿。

## 当前限制

- PostgreSQL workflow 已自动重建；可能已提交的订单先恢复为 `RECONCILE_PENDING`。IBKR 与
  Longbridge 均已自动复核 native identity/完整订单形状和新鲜账户快照，不以 Submit 代替恢复。
- capability key ring、启动原子轮换和损坏密文整体回滚已实现；所有 API 实例仍必须使用
  相同 key ring 与相同首 key，缺失或错误均 fail closed。
- 两家 Broker 的自动恢复和持续全账户事实账本已更新动态 buying power/health/reconciled，并
  持久化净值、持仓与成交；保护性 CLOSE 已使用最新提交的 Broker native 持仓，组合级 Greeks/
  压力风险仍待后续风险模型扩展。
- ThetaData Standard 的二阶/all-Greeks entitlement 不可用。derived Gamma 已有确定性实现和
  时间同步闸门，但完整 RTH option soak 仍是 paper Gate。
- IBKR sidecar gRPC、Longbridge Rust SDK 与受控外部 paper 路由代码已完成；尚未完成真实
  TWS/Gateway/Longbridge paper 账户的全天现场认证、部分成交/重启故障演练和 Q3 参数批准，
  live 提交仍不存在可达路径。

## 后果

首个切片可以确定性验证候选计划、两阶段风控、人工确认、paper 幂等和审计链，同时不
具备误触实盘的通路。代价是重启或执行事实不一致时需要保持 No Trade，直到后续对账
服务用 Broker 权威快照完成恢复。
