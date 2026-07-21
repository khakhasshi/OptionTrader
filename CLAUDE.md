# CLAUDE.md

本文件为 Claude Code 与后续开发者提供项目的目标、约束、规范、命令和关键决策。它是开发期的第一入口，与 `PROJECT_PLAN.md`（架构与阶段）、`TASKS.md`（任务状态）、`ASSUMPTIONS.md`（假设与待确认）配套使用。

## 1. 项目目标

QQQ 日内期权波动率交易系统。目标不是预测 QQQ 方向，而是判断盘中实际波动相对期权隐含波动是否错配，并把交易流程标准化、量化、可复盘。

第一优先级：**先避免错误交易，再寻找优秀交易。** 系统首要目标是阻止不可解释、不可审计或超出风险边界的交易，而非保证盈利。

分阶段交付：离线研究与回放 → 实时驾驶舱 → 半自动订单计划 → 受控自动执行（须长期 shadow/paper 验证后单独启用）。

## 2. 权威边界（不可违反）

```text
Rust Risk & Execution Gateway > Python Strategy Engine > LLM > React UI
```

- Rust Risk & Execution Gateway 是所有开仓请求的最终权威。Python、LLM、React 只能提出候选交易，不能绕过硬风控。
- **单一行情事实源**：ThetaData。
- **单一执行事实源**：当前 `CandidateTradePlan.broker_id` 所选 Broker 的账户/订单/成交/持仓回报。
- **单一主数据库**：PostgreSQL。不以 SQLite、DuckDB、浏览器存储替代。DuckDB 仅用于本地只读研究 Parquet。
- **LLM 无交易授权**：只做解释、审核、归因、研究假设。不得位于止损、撤单、kill switch、平仓的关键路径。`Proceed` 不是下单授权。
- 每个 `CandidateTradePlan` 必须指定唯一 `broker_id`，一次计划只能提交给一个 Broker。
- 数据缺失、时钟异常、行情陈旧、Broker 状态不一致时默认 **fail closed**。

## 3. 语言与服务职责

| 层 | 技术 | 职责 | 不承担 |
|---|---|---|---|
| Web UI | React + TypeScript | 驾驶舱、图表、告警、人工确认、复盘 | 不存密钥、不做权威风控、不直连 Broker |
| Application Service | Python + FastAPI | SOP 编排、Regime/Vol/Strategy、事件上下文、回放、研究、LLM、Web API | 不绕过 Rust Gateway 下单 |
| Market Core | Rust + Tokio | ThetaData 流、标准化、去重、时间排序、确定性底层特征、DataHealth | 不做 Regime/Strategy 决策或 LLM 判断 |
| Risk & Execution Gateway | Rust | 硬风控、订单状态机、Broker 适配、幂等、对账、kill switch | 不接受缺审计上下文的自由文本指令 |
| Research Jobs | Python | 历史导入、参数研究、walk-forward、报告 | 不直接改生产规则 |

计算归属唯一：ThetaData 官方 Python/gRPC SDK 只用于 Python Research Job 历史下载；Rust 权威计算 bar、VWAP、opening range、HV20/HV60、ATM、straddle、spread、quote age 与 DataHealth；Python 基于 Rust 快照生成 `VolState`、`RegimeState`、`Signal`、`CandidateTradePlan`。Python 同名函数只能作为离线 fixture/reference，不得进入 paper/live 交易许可链。同一决策规则不得两语言重复实现。

三个可独立启动的服务：`web`（React）、`application-api`（Python FastAPI）、`trading-core`（Rust workspace，内部按 crate 隔离 Market/Risk/Execution/Broker Adapter）。

## 4. 技术约束

- **时间**：UTC 存储、ET 决策、用户时区展示。禁止无时区时间戳。全部 `timestamptz`。
- **金额/价格/数量/Greeks**：`numeric` 或明确缩放整数。禁止用二进制浮点做最终风险和订单金额判断。
- **契约优先**：跨语言结构先定 Schema 再实现。API 契约用 Protobuf；持久化与 LLM 边界用 JSON Schema/Pydantic；前端客户端由 OpenAPI 生成。
- **迁移权威唯一**：Alembic 是唯一 schema migration 权威。Rust SQLx 只消费已迁移 schema 与离线元数据，不维护第二套迁移。生产 DDL 只允许 CI/CD 迁移任务执行。
- **通信**：React↔Python 用 HTTPS REST + WebSocket；Python↔Rust 用 gRPC（实时行情/状态用 server streaming）。
- **可追溯**：每个信号、计划、风控判断、LLM 输出可追溯到输入快照与规则版本。通用字段：`schema_version / event_id / correlation_id / causation_id / session_id / occurred_at_utc / received_at_utc / source / source_sequence / rule_version`。
- **期权合约主键**：`underlying + expiry + strike + right + multiplier`。
- 先建确定性规则基线，再引入 LLM 解释与规则研究。参数优化必须时间序列切分、walk-forward、样本外验证，不随机打乱时间。
- 第一版不引入 Python/Rust FFI、消息队列（NATS 待需要时）、Kubernetes、多区域部署。

## 5. 执行模式与环境

执行模式：`REPLAY/SHADOW`（不连 Broker）、`PAPER/MANUAL_CONFIRM`（开仓与普通退出需人工确认）、`CONTROLLED_AUTO`（仅白名单策略）。任何模式下 LLM 都不在关键风险路径。

环境：`local / replay / shadow / paper / live`。live 默认关闭，需显式环境开关与启动检查。环境切换不得只靠前端参数；Rust Gateway 必须校验服务端环境与账户白名单。

DataHealth：`HEALTHY / DEGRADED / STALE / DISCONNECTED / RECONCILING`。BrokerHealth：`HEALTHY / DEGRADED / DISCONNECTED / RECONCILING`。只有 DataHealth=HEALTHY 且所选 Broker BrokerHealth=HEALTHY 且已对账时允许新开仓。

两阶段风控：Initial Risk Check（LLM/人工确认前，不通过则终止且不调用 LLM）；Final Risk Check（人工确认后、Broker 提交前，重读最新状态，人工确认与 LLM Proceed 都不能覆盖其结论）。

## 6. 本机工具链

已验证可用：Node v22 / npm 10.9、uv 0.11（Python 3.14）、cargo 1.95、protoc 35.1、psql 16.14、Docker + Compose v5。

决策：JS 包管理用 **npm workspaces**（无 pnpm/yarn）；Python 依赖与虚拟环境用 **uv**（无 poetry）。见 ASSUMPTIONS.md A1。

## 7. 常用命令

统一入口为根 `Makefile`（占位命令随各服务落地补全）：

```bash
make setup        # 安装三端依赖
make dev          # 本地并行启动 web / application-api / trading-core
make health       # 检查三个服务 health endpoint
make test         # 运行三端单元测试
make lint         # TS/Python/Rust lint + format check
make contracts    # 生成 Protobuf / JSON Schema / OpenAPI 客户端
make migrate      # Alembic 迁移到最新
make up / make down  # docker compose 本地依赖（PostgreSQL 等）
```

## 8. 开发规范

- 契约或数据模型变更先改 `packages/contracts` 并重新生成，再改实现。
- 所有跨服务标识（plan_id/signal_id/order_id/event_id/idempotency_key）建立唯一约束。
- 订单、成交、风控、审计写入与状态转换须同事务或用 transactional outbox。
- 密钥不入 Git、日志、前端 bundle；用本地 secret store 或环境密钥管理。日志对账户号、token、订单原始凭证、PII 脱敏。
- 不提交大量 tick 数据到 Git。fixture 必须脱敏且体积可控。
- 每完成一个阶段更新 `TASKS.md`；重要架构决策写入本文件第 9 节并在 `docs/adr/` 建 ADR。
- 遇错先定位根因，不通过绕过检查或隐藏错误强行继续。

## 9. 关键决策记录

- **D1**：JS 用 npm workspaces，Python 用 uv（本机无 pnpm/yarn/poetry）。
- **D2**：monorepo 目录结构遵循 DEVELOPMENT_PLAN.md 第 4 节。
- **D3**：Phase 0 先落骨架 + 契约 + 迁移基础 + 端到端 smoke（fixture 快照从 Rust→Python→React）；不在 Phase 0 接实盘。
- **D4**：Phase 2 实时传输——Python↔Rust 用 gRPC（`market.proto` 的 `MarketService`，server streaming 返回 `stream MarketTick{snapshot,bar,delivery_phase,high_watermark_sequence}` + `GetDataHealth`）；React↔Python 用 WebSocket 增量推送 + REST 快照恢复。快照/DataHealth 权威在 Rust；流带原始每分钟 bar 以保证实时引擎输出与离线回放逐位一致。重连通过 `resume_after_sequence` 回补，BACKFILL 只重建状态且强制 No Trade；Projector 独立校验 sequence/high-watermark，恢复目标帧本身仍闭锁，只有越过目标后新产生的 LIVE 帧才可恢复许可。数据源=回放时钟驱动 + 可插拔实时适配器（`SnapshotSource`）。生成代码不入仓（Rust build.rs / Python 脚本）；`make test` 强制构建当前 Rust 二进制并执行跨语言 smoke。详见 `docs/adr/0001-phase2-realtime-transport.md`。
- **D5**：Phase 2 真实源与事件上下文——`OPTIONTRADER_MARKET_SOURCE=theta-sdk` 只消费内部 `ThetaDataSdkService`；Python 官方 SDK 直连 ThetaData 并轮询当日 Nasdaq Basic 已完成 1m OHLC，凭证不跨入 Rust。每次连接首批必须从 09:30 连续回补，Python 过滤空占位/当前未完成分钟，Rust 再校验时间、OHLC、连续性及已发布前缀；任一失败保持闭锁。Python 从四类严格来源文件生成 EventContext，缺失、陈旧、未来接收时间或关键低置信度输入禁止新开仓；事件文本不进入指令通道。真实凭证小样本 smoke 已通过，完整 RTH soak 仍是现场 Gate，不以 mock 测试替代。
- **D6**：Phase 3 半自动执行——CandidateTradePlan 1.2 使用确定性 Protobuf hash，计划级及每腿行情来源固定为 THETADATA，并携带 quote/size/Greeks/chain proof；Broker 只提供账户/持仓/订单/成交事实。Rust 在 Stage 与人工确认后各执行一次权威风控。确认令牌不进浏览器/日志，以 Fernet 密文进入 PostgreSQL 共享 capability store，并在确认意图事务内一次性 claim；不确定结果先对账。订单由 Rust 单调 `state_version` 仲裁，PostgreSQL 独立列+行锁+条件更新及 React last-known 锚点共同阻止回退。自适应 package 与每腿限价只由 Rust 基于 ThetaData proof 计算并受保护价约束，坏报价不退化；市价新开仓闭锁。REPLAY/SHADOW 不接 Broker，PAPER/MANUAL_CONFIRM 当前只接确定性 PaperBroker，CONTROLLED_AUTO/live 禁用。Longbridge 使用官方 Rust SDK并按 BUY 全成交后 SELL 的顺序受控拆腿，partial/unknown 停止且投影残仓；IBKR TWS/Gateway 使用 BAG。受信任 ThetaData option registry、真实订单/成交流、sidecar gRPC 与重启对账仍未完成。风险参数默认未确认、购买力默认 0。详见 `docs/adr/0002-phase3-semi-automated-execution.md` 与 `docs/BROKER_ADAPTERS.md`。
- 后续决策追加于此并同步 ADR。

## 10. 暂不实现（第一阶段）

裸卖 0DTE straddle/strangle；LLM 直接生成并提交 Broker 原生订单；自动上线 LLM 建议规则；多标的泛化；高频做市/co-location；Kubernetes/复杂消息总线/多区域。

## 11. 风险声明

本项目是软件工程与研究计划，不构成投资建议。QQQ 0DTE 期权具有极高 Gamma、Theta、流动性和执行风险。
