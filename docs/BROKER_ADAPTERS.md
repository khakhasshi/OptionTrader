# Broker Adapter 设计与认证边界

## 统一订单语义

Rust 是价格权威。Broker adapter 只接收已经确定的订单级方向、类型和提交价：

- `MARKET`：不带提交价。adapter 支持映射，但新开仓硬风控固定拒绝。
- `LIMIT`：提交人工批准的保护价。
- `ADAPTIVE_LIMIT`：Rust 以 `mid ± aggressiveness × half_spread` 定价；买单向 ask
  移动、卖单向 bid 移动，按 aggressive 方向取整到 tick，且买价不得超过保护价、卖价
  不得低于保护价。坏报价、陈旧报价、交叉盘、过宽点差一律拒绝，不退化为 touch 或市价。

自适应尝试从 `initial_aggressiveness_bps` 线性走向 10000 bps。当前执行闭环只提交第
一次尝试；cancel/replace 定时器、最大尝试后的处置属于后续订单管理切片，禁止自动退化市价。

## Longbridge

- 使用官方 Rust SDK `longbridge 4.3.3` 的 blocking TradeContext。
- 凭证仅由 SDK 从 `LONGBRIDGE_*`（兼容 `LONGPORT_*`）环境变量读取。
- 支持账户购买力、持仓、单腿期权市价/限价/自适应限价映射、撤单。
- Longbridge OpenAPI 当前不支持多腿期权组合。两腿及以上请求返回
  `UnsupportedOrderShape`，绝不拆腿提交。
- `broker_contract_id` 必须是 Longbridge SDK 原生期权 symbol；`contract_id` 仅作跨数据源审计标识。
- SDK error 不写入日志正文，连接或未知提交结果使 adapter 进入未对账状态。

## IBKR TWS / Gateway

- 使用官方 TWS API socket 模型。`OPTIONTRADER_IBKR_MODE=TWS|GATEWAY` 选择端点；paper
  默认端口分别为 7497 / 4002，live 默认端口分别为 7496 / 4001。
- 只允许连接 `127.0.0.1`、`localhost` 或 `::1`；账户和独立 `client_id` 必填。
- 必须收到 `nextValidId` 且配置账户出现在 managed accounts 后才算连接完成。
- 每条腿必须携带数字 `conId`。多腿订单构造一个 `BAG`；SELL parent 会反转 BAG 内部
  canonical leg action，使最终成交方向仍与 Candidate 每条腿一致。
- `ADAPTIVE_LIMIT` 映射为受保护的 `LMT` + IB Adaptive algo，priority 为
  Patient/Normal/Urgent；不设置 `NonGuaranteed`，避免主动接受拆腿成交语义。
- `OPTIONTRADER_IBKR_SUBMISSION_ENABLED` 默认 false，且必须精确写为 true 才允许调用
  `placeOrder` / `cancelOrder`。

## 现场认证 Gate

代码可编译和模拟测试不等于 paper/live 签收。启用真实提交前仍必须完成：

1. 两家 broker 的 account/position/open-order/fill 全量快照与 PostgreSQL 投影对账。
2. 进程重启、1100/1101/1102/1300、未知提交结果、部分成交、拒单、撤单竞争的故障演练。
3. Longbridge 单腿和 IBKR BAG 在 paper 账户完成市价、限价、自适应限价现场认证。
4. 完整 RTH soak、API pacing、订单事件流和审计链检查。
5. Q3 风控参数、策略白名单和 live submission 开关书面批准。

上述 Gate 完成前，生产 workflow 继续使用 PaperBroker，真实 adapter 不接入确认路径。
