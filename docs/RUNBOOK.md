# OptionTrader 运维手册

本文记录当前阶段已知的 fail-closed 行为与受支持的恢复步骤。任何恢复操作都不得
绕过 DataHealth、sequence continuity、delivery phase 或 high-watermark 闸门。

## 同 session 下 Trading Core 重启

### 现象

Rust Trading Core 重启后，其内存 replay cursor 会从 1 重新开始。如果 Python
Application API 仍保留同一个 `session_id` 的 `CockpitProjector`，新的
`high_watermark_sequence` 会低于 Projector 已观察到的 watermark。Projector 会按
设计返回 `DISCONNECTED` / No Trade，并在 `risk_flags` 中报告
`invalid transport sequence metadata`。

这是粘滞式 fail-closed 行为：系统不会把无法证明属于同一数据世代的低序列记录当成
实时行情，也不会自动清空 watermark 后继续交易。

### 当前恢复步骤

1. 保持新开仓禁用，确认没有把旧 Cockpit LIVE 帧作为交易依据。
2. 确认 Trading Core 已重新启动，HTTP/gRPC 健康检查可达，数据源身份与预期一致。
3. 重启 Application API，使 `SessionHub` 和 `CockpitProjector` 从空状态重建。当前
   Phase 2 骨架不支持在线重置单个 Projector。
4. 重新连接驾驶舱。历史恢复帧必须显示 STALE/No Trade；只有回补完成后的新 LIVE
   帧，且 DataHealth=HEALTHY，才可能恢复新开仓许可。
5. 若仍出现 watermark 回退、sequence discontinuity 或 DISCONNECTED，维持 No Trade，
   不得通过修改内存字段、伪造 session id 或跳过回补强制解锁。

### 后续演进

真实实时源接入前应在传输契约中引入显式 `session_epoch` 或等价的数据世代标识。届时
只有经过身份校验的新 epoch 才能重置 sequence/watermark；不能仅凭数值回退推断重启。

## ThetaData 实时源

1. 准备官方 Python SDK 凭证。推荐设置
   `THETADATA_CREDENTIALS_FILE=/absolute/path/to/creds.txt` 并执行 `chmod 600`；也可设置
   `THETADATA_API_KEY` 或 `THETADATA_DOTENV_PATH`。不得复制凭证到仓库或命令输出。
2. 在独立终端运行 `make dev-thetadata-sdk`，确认 `127.0.0.1:50052` 启动；SDK 直接连接
   ThetaData 服务器，不需要启动 Theta Terminal。
3. 在第二个终端运行 `make dev-core-theta-sdk`。Rust 通过
   `THETADATA_SDK_GRPC=http://127.0.0.1:50052` 消费已完成分钟 bar。
4. 盘中启动或重连时，必须先看到从 09:30 开始的连续回补；回补期间 Cockpit 显示
   STALE/No Trade。SDK 请求错误、空/部分占位、分钟缺口、时间冲突、前缀冲突或
   entitlement 错误不得手工改健康状态解锁。
5. 可用以下 opt-in smoke 验证凭证和 Standard 股票 OHLC 权限：
   `THETADATA_CREDENTIALS_FILE=/absolute/path/to/creds.txt uv run pytest
   tests/test_thetadata_sdk_live.py -q`（在 `services/application-api` 下运行）。
6. `GetDataHealth`、`/health` 与 Cockpit 必须一致；市场流持续 90 秒无 tick 时 Python
   发布 DISCONNECTED 并重连，Rust 也按自身阈值将健康降级。

当前仓库已用真实账号完成 QQQ 三分钟历史 OHLC 凭证/字段 smoke，并以 mock SDK gRPC
覆盖回补和增量传输。完整 RTH 连续运行仍需现场执行，未执行前不得作为 paper/live
上线证据。

## 事件上下文日常导入

盘前将四类文件放入 `data/events/YYYY-MM-DD/`，运行 `make events-context`。退出码 2、
`available=false` 或任一来源检查失败时保持 No Trade。需要审计落库时使用 CLI 的
`--persist`；它会在同一事务写入 `events.event_contexts` 与 `audit.audit_events`。

## Phase 3 shadow / paper 执行

1. 默认配置是闭锁状态：`OPTIONTRADER_RISK_LIMITS_CONFIRMED=false`、规则版本
   `UNCONFIRMED`、buying power 为 0。只有经过人工批准的测试参数才可显式覆盖；不得把
   `.env.example` 的占位数值解释为生产批准。
2. `DATABASE_URL` 不可用时，Application API 必须在调用 Rust 前返回
   `execution_audit_unavailable`。不得临时绕过审计继续提交。
3. Stage 返回的确认令牌不出现在 REST、浏览器或日志；Application API 使用
   `OPTIONTRADER_CONFIRMATION_FERNET_KEY` 的逗号分隔 key ring 加密后写入 PostgreSQL
   capability store。首 key 用于新密文，旧 key 仅用于解密；启动会在同一事务原子轮换所有
   legacy-key capability，已使用首 key 的记录不重写，任一密文损坏则整体回滚并拒绝启动。密钥缺失或无效时必须在调用
   Rust 前返回 `confirmation_store_unavailable`。确认页面必须展示 plan hash、全部腿、
   最大损失、Broker、模式和到期时间，并由操作者勾选。
4. 确认后 Rust 重新执行 Final Risk Check。市场、事件、账户、限额、规则版本、快照或
   TTL 任一变化都可否决，不得通过再次点击或修改前端状态覆盖。
   Candidate 1.3 的计划级和每腿 provider 必须均为 `THETADATA`；Broker quote 不得进入
   计划或自适应定价。Python 必须先通过 `GetOptionSnapshots` 获取批次 hash，Rust 在 Stage
   与 Confirm 都向同一 Theta SDK bridge 验证 exact-contract quote/size/Greeks。
   Standard 订阅只提供 first-order Greeks：Gamma 由 ThetaData 的 Delta、IV、underlying price
   和到期时间确定性推导；option/underlying 时间差超过 5 秒即拒绝，不能用 Broker Gamma 回填。
5. 默认 `OPTIONTRADER_BROKER_EXECUTION_BACKEND=simulated-paper`。外部 paper 代码路由只接受
   `ibkr-paper` 或 `longbridge-paper`，并要求 `OPTIONTRADER_ENV=paper`、
   `OPTIONTRADER_BROKER_PAPER_SUBMISSION_ENABLED=true`、对应 Broker 的 paper/submission 开关，
   且必须与本进程唯一持续对账 Broker 一致。任一条件不满足即拒绝启动；
   `LIVE_TRADING_ENABLED=false` 必须保持不变，Phase 3 不存在 live 路由。Broker sidecar 仅绑定本机。
6. Application API 启动时会从 PostgreSQL 读取 plan/order/capability 并调用 Rust
   `RestoreWorkflow`：未 claim 且未过期的确认能力原样恢复；终态保持终态；已提交、已 claim、
   过期但结果不明的订单统一进入 `RECONCILE_PENDING`，版本号加一且残余敞口置 true。返回的
   `reconciliation_order_ids` 会逐个调用 Rust 对账。IBKR 恢复只读匹配 native id/orderRef/完整
   订单形状；Longbridge 由 Rust 官方 SDK 按 native id、OptionTrader remark、symbol、side、quantity、
   原生订单类型和提交价格全字段匹配，拆腿还要求每个 leg remark 唯一。两条路径都要求新鲜、
   HEALTHY、已对账账户快照；成功结果回写 PostgreSQL。失败保持 unresolved/RECONCILING；不得删除
   数据库行或重建计划绕过，恢复 RPC 不得提交、改单或撤单。
7. 订单进入 `RECONCILE_PENDING`、BrokerHealth 非 HEALTHY、账本不一致或 kill switch
   激活时，禁止新开仓；撤单/减仓恢复路径不得依赖 LLM。Candidate 1.3 的保护性 CLOSE
   可绕过仅针对开仓的事件/白名单/购买力/日损/次数/冷却/kill-switch 限制，但仍必须有新鲜
   ThetaData proof、HEALTHY 且已对账的 Broker、当前规则版本和最新已提交 native 持仓。
   每腿方向必须减少该持仓且数量不得超出；市价仅允许单腿 CLOSE，多腿 CLOSE 仍必须限价。
8. capability claim 使用 PostgreSQL 行锁，可跨 API worker 仲裁；CLI worker 参数不再是
   确认安全边界。实时 SessionHub 仍是每进程实例，完整横向扩展尚未认证，paper soak 前
   仍建议单 worker。若 Confirm 的 gRPC 已成功但投影写入失败，claim 不得自动释放；先调用
   GetOrder 对账并回填投影，确认 Rust 已停留在非 `AWAITING_CONFIRMATION` 状态。
9. 配置数据库与 Fernet key 后，持续 Broker 对账默认启用；可设置
   `OPTIONTRADER_BROKER_RECONCILIATION_INTERVAL_SECONDS=30`（仅允许 5–300 秒）。每轮先由
   Rust `BeginBrokerReconciliation` 关闭 Broker 闸门并签发 15 秒快照，再由 Application API
   原子写入 PostgreSQL，最后以同 sequence/hash 回执。查看
   通过 `OPTIONTRADER_BROKER_RECONCILIATION_BROKERS=ibkr` 选择本进程唯一 Broker；共享
   BrokerAuthority 尚未按账户分片，因此禁止同时配置两家。Longbridge 认证时改为 `longbridge`，
   缺凭证会保持闭锁。查看
   `GET /api/v1/trading/reconciliation?broker_id=longbridge`；每个 Broker 独立展示状态，任何
   failure/mismatch/unresolved 或残余敞口都必须保持 false。
   `OPTIONTRADER_BROKER_RECONCILIATION_ENABLED=false` 仅供本地非交易开发，不能作为 paper 配置。
10. 每次计划、风控、订单、成交、对账和失败状态转换会在原数据库事务写入 outbox。
    publisher 使用 `claim_outbox_batch` 的 PostgreSQL `SKIP LOCKED` 租约，并按确定性 `event_id`
    做 at-least-once 去重；成功 ack，失败退避，超过上限进入 dead letter。第一阶段不部署 NATS，
    不得把“已写 outbox”描述为“已发送到外部消息系统”。

## Phase 4 LLM 辅助层

1. LLM 配置只进入服务端环境。根目录 `.env` 必须被 Git 忽略且权限为 `600`；不得把 key
   放进 `.env.example`、React localStorage、URL、日志、fixture 或评审输出。`make dev-api` 在
   本地 `.env` 存在时通过 uv dotenv parser 注入；生产环境由 secret manager 注入。
2. 启动后先检查 `GET /api/v1/llm/status`。配置缺失只表示审阅不可用，不得影响 DataHealth、
   Rust 风控、撤单、保护性减仓或平仓。任何非 COMPLETED 结果必须是 `Review Only` 且
   `confidence=0`。
3. `POST /api/v1/llm/reviews` 只接受严格结构化上下文。PRE_EXECUTION 会忽略调用方携带的
   Candidate/Initial Risk 内容并从 PostgreSQL 重读；计划、初始风控或 review schema 不可用时
   不调用 Provider。LLM 的 Proceed/Cancel/Reduce Risk 均不会调用 Broker。
4. 日常离线门禁运行 `make test-llm-eval`。显式真实调用使用 `make test-llm-live`；完整 5 case
   合成评测使用 `make test-llm-live-eval`。后两者会产生外部 API 调用，只输出状态枚举、聚合
   指标和固定验证代码，不输出模型正文或密钥。
5. 合格基线要求：structured output=1、conflict recall=1、false positive=0、injection block=1、
   unavailable inert=1，且 missed/contract/expectation mismatch case 均为空。任何模型或 prompt
   版本变化都必须重跑；失败时保持 LLM 功能降级，不得放宽 Schema 取得绿灯。
6. Daily Review 与规则研究队列是只读页面。研究假设永远不能直接激活；进入 shadow 前仍需
   成本回测、walk-forward、样本外验证和人工 Gate Review。
7. `0007` 迁移后，每日请求/估算金额配额、request-id single-flight、租约和结果均由
   PostgreSQL 协调；多 API worker 不会对同一 request id 重复调用 Provider。进程内 cache 与
   并发信号量仍按 worker 隔离，因此不同 request id 即使内容相同也不承诺跨 worker 命中缓存。
8. 自动编排默认关闭。完成 `make migrate` 后，只有显式设置
   `OPTIONTRADER_LLM_AUTOMATION_ENABLED=true` 才启动。盘后任务使用 XNYS 交易日历；必须达到
   交易所收盘与 grace、信号/EventContext 完整、每笔已提交订单终态、残余敞口清零，并取得
   同 session、晚于收盘及订单更新时刻的 HEALTHY Broker 对账证明，才写入 review outbox。
9. 盘中任务只异步读取确定性 transactional outbox 的白名单 topic，按状态指纹去重、去抖、
   限频并批量合并。交易/风控写路径不写 LLM 队列，也不等待调度器；停止 API 时直接取消
   supervisor，不为清空积压而延迟退出。
10. 查看 `GET /api/v1/llm/automation/status` 和 `/api/v1/llm/automation/runs`。缺数据或未完成对账
    会保留 `WAITING_INERT` 与原因码且不调用 Provider；消费失败按 outbox 租约重试，达到上限
    进入 dead letter。若 Provider 调用期间 worker 消失，租约恢复生成
    `COORDINATION_LEASE_EXPIRED` 惰性结果，禁止再次调用 Provider；这是有意的 at-most-once
    取舍，不得人工删除租约后重放同一 request id。

## Broker SDK 认证前启动

Longbridge 原生 adapter 从 `LONGBRIDGE_APP_KEY`、`LONGBRIDGE_APP_SECRET`、
`LONGBRIDGE_ACCESS_TOKEN` 读取凭证。不要把值写入 compose、日志或提交。持续对账始终实例化
`submission_enabled=false` 的只读 authority；只有独立 paper 认证进程满足全部执行路由门槛时，
才会另外构造可提交 adapter。进程重启后的 Longbridge 撤单前，写侧还必须用只读恢复已经认证的
durable request/native id 重建本地身份账本；重绑定失败保持 `RECONCILE_PENDING`。两者不能用
“能读取账户”推导“允许下单”。每次 Longbridge submit 前，写侧还会执行自身的全量只读
reconcile；发现未知活动单、无法归属的成交或连接异常时关闭 Broker authority，不进入 submit。

凭证写入 Git 忽略且权限为 `600` 的根目录 `.env` 后，可显式执行两条只读 demo smoke。测试
默认 `ignored`，且内部再次要求 opt-in；第一条先断言 submit 返回 `LiveSubmissionDisabled`，
第二条验证统一 BrokerSnapshot、sequence 和 SHA-256，不打印账户明细或凭证：

```bash
set -a; source .env; set +a
OPTIONTRADER_RUN_LONGBRIDGE_DEMO_SMOKE=true cargo test --manifest-path services/trading-core/Cargo.toml -p broker longbridge::tests::demo_account_read_only_reconciliation_smoke -- --ignored --exact
OPTIONTRADER_RUN_LONGBRIDGE_DEMO_SMOKE=true cargo test --manifest-path services/trading-core/Cargo.toml -p trading-core-bin broker_registry::tests::demo_account_longbridge_authority_snapshot_smoke -- --ignored --exact
```

Longbridge 多腿认证还需设置并记录以下参数；非法值会阻止 adapter 启动：

```text
OPTIONTRADER_ENV=paper
LIVE_TRADING_ENABLED=false
OPTIONTRADER_BROKER_EXECUTION_BACKEND=longbridge-paper
OPTIONTRADER_BROKER_PAPER_SUBMISSION_ENABLED=true
OPTIONTRADER_LONGBRIDGE_PAPER=true
OPTIONTRADER_BROKER_RECONCILIATION_BROKERS=longbridge
OPTIONTRADER_LONGBRIDGE_LEG_FILL_TIMEOUT_MS=8000
OPTIONTRADER_LONGBRIDGE_LEG_POLL_INTERVAL_MS=250
```

逐腿日志必须证明 BUY 完整成交先于任何 SELL。出现 partial、unknown 或
`residual_exposure=true` 时停止新开仓并先完成 Broker 对账。

IBKR sidecar 必须先在 TWS 或 Gateway 中启用 socket client、关闭 Read-Only API，并核对
paper/live 端口。配置至少包含：

```text
OPTIONTRADER_IBKR_MODE=TWS|GATEWAY
OPTIONTRADER_IBKR_PAPER=true
OPTIONTRADER_IBKR_ACCOUNT=DU...
OPTIONTRADER_IBKR_CLIENT_ID=37
OPTIONTRADER_IBKR_SUBMISSION_ENABLED=false
OPTIONTRADER_BROKER_EXECUTION_BACKEND=simulated-paper
OPTIONTRADER_BROKER_PAPER_SUBMISSION_ENABLED=false
OPTIONTRADER_BROKER_RECONCILIATION_BROKERS=ibkr
```

执行 `make dev-ibkr-sidecar` 后，必须同时观察到 `nextValidId`、managed account，以及
account summary / positions / open orders / executions 四个 end callback；缺任一项时 sidecar
保持 RECONCILING。发现不属于本进程幂等账本的活动订单时 `account.reconciled=false`。只有 paper Gate
逐项签收时，才可在隔离认证进程把 backend 改为 `ibkr-paper`，并把全局 opt-in 与
`OPTIONTRADER_IBKR_SUBMISSION_ENABLED` 同时改为 true；日常主系统继续保持上述 false/default。
