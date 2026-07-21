# Broker Adapter 设计与认证边界

## 统一订单语义

Rust 是价格权威。Broker adapter 只接收已经确定的订单级方向、类型和提交价：

- 行情、期权报价、Greeks、期权链和每腿定价证明只能来自 ThetaData；Broker 只提供
  账户、持仓、订单与成交事实，不允许用 Broker quote 覆盖 Candidate。

- `MARKET`：不带提交价。新开仓硬风控固定拒绝；仅允许有最新 Broker native 持仓证明的
  单腿保护性 `CLOSE`，且方向和数量必须可证明减少净持仓。多腿市价固定拒绝。
- `LIMIT`：提交人工批准的保护价。
- `ADAPTIVE_LIMIT`：Rust 以 `mid ± aggressiveness × half_spread` 定价；买单向 ask
  移动、卖单向 bid 移动，按 aggressive 方向取整到 tick，且买价不得超过保护价、卖价
  不得低于保护价。坏报价、陈旧报价、交叉盘、过宽点差一律拒绝，不退化为 touch 或市价。

自适应尝试从 `initial_aggressiveness_bps` 线性走向 10000 bps。当前执行闭环只提交第
一次尝试；cancel/replace 定时器、最大尝试后的处置属于后续订单管理切片，禁止自动退化市价。

## Longbridge

- 使用官方 Rust SDK `longbridge 4.3.3` 的 blocking TradeContext。
- 凭证仅由 SDK 从 `LONGBRIDGE_*`（兼容 `LONGPORT_*`）环境变量读取。
- 支持账户购买力/净资产/币种、持仓数量/均价、当日订单/成交、期权市价/限价/自适应限价映射、撤单。
- Longbridge OpenAPI 当前不支持原生多腿组合，因此由 Rust 执行受控拆腿：按 Candidate
  原始腿保存审计顺序，但提交顺序固定为全部 BUY 腿在前、SELL 腿在后。每条 BUY 腿必须
  查询到完整成交后才允许下一条腿；partial/unfilled/rejected/unknown 立即停止，不再提交卖腿。
  `PartialFilled` 会先撤销该子单剩余数量，并等待 `PartialWithdrawal/Cancelled/Filled`；撤单
  结果未知时进入 `RECONCILE_PENDING`，不把“已发撤单”当成“撤单已完成”。
- 每腿限价由 Rust 基于 ThetaData quote 独立计算。买腿使用不高于 ask 的保护价，卖腿使用
  不低于 bid 的保护价，并再次验证组合净价不突破人工确认的 package protection。
- 子订单使用 `plan_hash + leg_index` remark，并投影到 `broker_child_order_ids`。中途已有成交时
  标记 `residual_exposure=true`；提交结果未知也按“可能有残仓”处理。重连时按精确 remark、
  symbol、side、quantity 扫描当日订单恢复 child ID；零匹配或多匹配均保持闭锁。系统保留
  受保护的多头残仓并要求对账，不自动市价砍仓。
- `ExecutionOrder 1.1` 同时投影每个子单的腿序号、方向、委托量、成交量、状态和提交价；
  Rust、Python 和 React 必须能从明细独立核对 residual，界面不得只显示父单布尔值。
- SDK submit/order/cancel 被限制在窄 I/O 适配层；脚本化测试覆盖部分成交后撤单、撤单失败、
  轮询超时、终态回报和恢复时重复匹配。生产实现仍只使用官方 Rust SDK。
- `OPTIONTRADER_LONGBRIDGE_LEG_FILL_TIMEOUT_MS` 默认 8000（1000-60000），轮询间隔
  `OPTIONTRADER_LONGBRIDGE_LEG_POLL_INTERVAL_MS` 默认 250（50-1000）；非法配置拒绝启动。
- `broker_contract_id` 必须是 Longbridge SDK 原生期权 symbol；`contract_id` 仅作跨数据源审计标识。
- SDK error 不写入日志正文，连接或未知提交结果使 adapter 进入未对账状态。
- reconcile 会扫描全部当日活动订单和成交；发现无法归属到当前幂等账本的活动订单，或成交
  无法映射到已知 plan/leg 时返回 `NotReconciled`，不会只对账“本进程记得的订单”。
- 持续对账 authority 始终以 `submission_enabled=false` 构造；外部 paper 执行 adapter 是独立实例，
  只有全部 Phase 3 paper 门槛满足时才允许 mutation。写侧每次提交前还会先执行全量只读 reconcile；
  未知活动单、断线或无法归属的成交都会阻止 submit。读取成功绝不隐含提交许可。

## IBKR TWS / Gateway

- 使用官方 TWS API socket 模型。`OPTIONTRADER_IBKR_MODE=TWS|GATEWAY` 选择端点；paper
  默认端口分别为 7497 / 4002，live 默认端口分别为 7496 / 4001。
- 只允许连接 `127.0.0.1`、`localhost` 或 `::1`；账户和独立 `client_id` 必填。
- 必须收到 `nextValidId` 且配置账户出现在 managed accounts 后才开始只读快照；随后账户汇总、
  持仓、未结订单、成交四个 end callback 必须全部完成才令 `reconciled=true`。
- 每条腿必须携带数字 `conId`。多腿订单构造一个 `BAG`；SELL parent 会反转 BAG 内部
  canonical leg action，使最终成交方向仍与 Candidate 每条腿一致。
- `ADAPTIVE_LIMIT` 映射为受保护的 `LMT` + IB Adaptive algo，priority 为
  Patient/Normal/Urgent；不设置 `NonGuaranteed`，避免主动接受拆腿成交语义。
- `OPTIONTRADER_IBKR_SUBMISSION_ENABLED` 默认 false，且必须精确写为 true 才允许调用
  `placeOrder` / `cancelOrder`。
- `make dev-ibkr-sidecar` 启动 loopback `BrokerAdapterService`，提供完整 snapshot、幂等 submit、
  cancel、只读 `RecoverBrokerOrder` 和 sequence-bound reconcile。恢复请求必须同时匹配已知 native
  order id、`orderRef`、合约/腿、方向、数量、类型、价格和 Adaptive 参数；零匹配、多匹配或
  历史成交无法完整证明原请求时拒绝，恢复 RPC 永不提交。未知活动订单也进入 snapshot，并强制
  account health/reconciled 同时降为 RECONCILING/false。
- `OPTIONTRADER_IBKR_TIMEZONE` 默认 `America/New_York`，仅用于 TWS 返回不带时区的 execution
  时间；带显式时区的回报优先，非法时区拒绝启动。

## 进程恢复

Application API 启动时从 PostgreSQL 重建 Rust workflow。只有未 claim 且未过期的确认能力
可以继续确认；终态只读恢复；任何可能已经跨过 Broker 提交边界的订单都提升为
`RECONCILE_PENDING`、版本加一并保留残余敞口。恢复 RPC 从不提交或撤单，返回的订单清单必须
随后与所选 Broker snapshot 对账。Application API 会逐项调用 Rust
`ReconcileExecutionOrder`；IBKR 由 Rust 向 sidecar 发只读恢复请求，Longbridge 由 Rust 官方 SDK
authority 只读匹配 native id、remark 与完整订单形状。两者均复核新鲜账户快照，成功后更新订单
投影和动态 buying power。Longbridge 外部 paper 在恢复成功后，还必须让独立 mutation adapter
以同一 durable request/native id 完成一次只读身份重绑定；失败时订单继续保持
`RECONCILE_PENDING`，不会尝试撤单。隔离的外部 paper 主确认路径已经
接线，但默认仍走 simulated-paper；任何不确定 submit/cancel 结果都会转 ReconcilePending 并关闭 authority。

持续对账使用两阶段账户握手：Rust 先闭锁并从 IBKR sidecar 或 Longbridge 只读 SDK authority 获取严格验证的全量快照，返回原始
protobuf 字节、单调 sequence、SHA-256 和短 TTL；Application API 在一个 PostgreSQL 事务中写入
`risk.broker_snapshots`、`trading.position_snapshots`、`trading.fills` 与审计，再把同一哈希和差异码
交回 Rust。只有持久化成功、无差异、回执未过期且 workflow 无 `RECONCILE_PENDING` 或残余敞口才开闸。
`reqAllOpenOrders` 虽返回登录下全部账户，sidecar 的 `openOrder` 与恢复形状匹配均强制配置账户；
`orderStatus` 不得重新引入已过滤订单。Longbridge 只读 authority 强制 `submission_enabled=false`，并可由
`OPTIONTRADER_BROKER_RECONCILIATION_BROKERS` 选择为 supervisor 的唯一 Broker；共享 authority 分片
完成前禁止同时启用两家，两类历史状态仍可通过查询参数分别查看。

## 外部 paper 路由

Rust 仅接受 `disabled | simulated-paper | ibkr-paper | longbridge-paper` 四种执行 backend。两种外部
backend 都要求 `OPTIONTRADER_ENV=paper`、`LIVE_TRADING_ENABLED=false`、全局 paper mutation opt-in，
并与唯一 reconciliation Broker 一致；IBKR 还要求 paper 与 submission 开关，Longbridge 要求明确的
paper 开关。配置歧义、Broker 不匹配或任何 live 标志都会在启动时 fail closed。

网络 submit/cancel 在 workflow 锁外执行，响应必须完整匹配请求的 broker、idempotency key、订单
类型、价格与全部腿。超时、断线或形状不一致按 outcome unknown 处理，不自动重发。保护性 CLOSE
同样通过 Stage/Confirm 和 ThetaData proof，但以最新已提交 Broker position map 证明每条腿确实减仓。

## 现场认证 Gate

代码可编译和模拟测试不等于 paper/live 签收。启用真实提交前仍必须完成：

1. 两家 broker 的 account/position/open-order/fill 全量快照与 PostgreSQL 投影对账。
2. 进程重启、1100/1101/1102/1300、未知提交结果、部分成交、拒单、撤单竞争的故障演练。
3. Longbridge 单腿/受控拆腿和 IBKR BAG 在 paper 账户完成限价、自适应限价现场认证。
   必须注入买腿部分成交、卖腿拒绝、查询超时、撤单竞争和进程重启。
4. 完整 RTH soak、API pacing、订单事件流和审计链检查。
5. Q3 风控参数、策略白名单和 live submission 开关书面批准。

上述 Gate 完成前，日常 workflow 继续使用 simulated-paper。外部 paper backend 只用于隔离现场认证，
不能被描述为已获准持续运行；live 在 Phase 3 没有可达执行路径。
