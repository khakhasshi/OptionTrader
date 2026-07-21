# PROJECT_PLAN.md

QQQ 日内期权波动率交易系统的整体架构、阶段计划与里程碑。本文件综合 `DEVELOPMENT_PLAN.md`、`QQQ_INTRADAY_VOL_TRADING_SYSTEM_DESIGN.md`、`EXTERNAL_DATA_INTERFACE_PLAN.md` 与 `LLM_SOP_FLOWCHARTS.html`，是执行侧的浓缩视图。原始设计文档为准，本文件不覆盖它们。

## 1. 系统架构

```text
ThetaData ──► Rust Market Core ──► PostgreSQL / Parquet
   (行情事实源)      (标准化+DataHealth)      │
                          │                   ▼
官方事件源 ──► Python Application Service ──► React Trading Cockpit
Longbridge内容    (Regime/Vol/Strategy/Event/         │
                   Review/Replay/Research/LLM)   人工确认
                          │  ▲                        │
                          ▼  │ 已签名候选计划          │
              Rust Risk & Execution Gateway ◄─────────┘
                   (硬风控/订单状态机/对账/kill switch)
                          │
              Longbridge / IBKR Broker Adapter
                   (执行事实源 = 当前 broker_id)
```

数据流：Rust Market Core 从 ThetaData 建立不可变快照（含递增 sequence_number）与 DataHealth，聚合快照入 PostgreSQL、原始高频行情入 Parquet。Python 消费快照生成 Regime/Vol/Signal/CandidateTradePlan，经 Rust 两阶段风控，人工确认后向唯一 Broker 提交限价、自适应限价，或仅用于单腿保护性 CLOSE 的市价单；市价新开仓与多腿市价固定闭锁，全过程状态转换和 deterministic outbox 写入 PostgreSQL。

## 2. 目录结构

```text
OptionTrader/
├── apps/web/                       # React + TypeScript (Vite)
├── services/
│   ├── application-api/            # Python FastAPI
│   └── trading-core/               # Rust workspace (market/risk/execution/broker crates)
├── packages/
│   ├── contracts/                  # Protobuf / JSON Schema / OpenAPI
│   ├── ui/                         # 可复用 UI 组件
│   └── config/                     # 非敏感共享配置模板
├── data/
│   ├── events/                     # 事件 JSON (macro/holdings/earnings/news)
│   ├── replay/                     # 本地 Parquet（仅目录说明入库）
│   └── fixtures/                   # 可提交的脱敏测试数据
├── migrations/                     # PostgreSQL / Alembic
├── infra/{docker,compose,monitoring}/
├── scripts/                        # 开发/导入/回放/契约生成
├── tests/{contract,integration,replay,e2e}/
├── docs/{adr,runbooks,api}/
└── *.md 原始设计文档 + 治理文档
```

## 3. 核心模块

- **Rust Market Core**：生产 ThetaData 流接入、MarketEvent 权威标准化、去重/排序/回补、分钟 bar/VWAP/opening range/HV20/HV60/ATM/straddle/spread/quote age、DataHealth。
- **Python Research Jobs**：通过 ThetaData 官方 Python/gRPC SDK 下载历史 QQQ/期权/VIX，生成带 manifest/checksum 的离线 Parquet；其参考指标只用于与 Rust fixture 对拍。
- **Rust Risk & Execution Gateway**：`risk-policy`、`initial-risk-check`、`final-risk-check`、`order-state`、`broker-longbridge`、`broker-ibkr`、`reconciliation`、`kill-switch`、`audit`。
- **Python Application Service**：`api`、`domain`、`orchestration`（SOP 状态机）、`regime`、`volatility`、`strategy`、`events`、`review`、`replay`、`research`、`llm`、`adapters`。
- **React Cockpit**：Live Cockpit、Positions & Orders、Replay、Daily Review、System Health。
- **Event Context Layer**（Python）：macro_events / qqq_holdings / qqq_top20_earnings / qqq_top20_news_events 四类统一 Schema，导入标准化入 PostgreSQL `events`。

## 4. 数据模型（PostgreSQL schema）

```text
market   聚合行情、指标快照、DataHealth
events   宏观、持仓、财报、新闻、EventContext + 来源元数据
trading  会话、Signal、CandidateTradePlan、订单、成交、持仓
risk     RiskDecision、BrokerHealth、限额、kill switch
review   LLMReview、DailyReview、研究报告索引
config   rule_version、系统配置、发布记录
audit    人工确认、状态转换、不可变审计事件
```

首批核心表见 DEVELOPMENT_PLAN.md 第 7 节。Parquet 按 `provider/data_type/symbol/trading_date/hour` 分区，PostgreSQL 保存数据集清单/路径/checksum/覆盖时段/导入状态。

## 5. 主要接口

REST（React↔Python）：session/current、dashboard/snapshot、events/today、signals、trade-plans/{id}(+confirm/cancel)、orders、positions、risk/status、risk/kill-switch、replays、reviews/{date}。

WebSocket 主题：market.snapshot、regime.state、vol.state、risk.state、signal.created、trade_plan.updated、order.updated、position.updated、system.health。

gRPC（Python↔Rust）：已实现 StreamMarketSnapshots、GetDataHealth、EvaluateCandidate、StageCandidate、ConfirmCandidate、CancelOrder、GetOrder；计划追加 ClosePosition、StreamOrderEvents、GetBrokerSnapshot、ReconcileState、ActivateKillSwitch。`ConfirmCandidate` 须携带人工确认令牌、plan hash 与最新 EventContext，并由 Rust 重读规则版本、过期时间和权威状态。Broker adapter/sidecar 使用独立 `broker.proto`。

## 6. 状态流转

系统状态机：PreMarket → DataHealthCheck →（双健康）PlanReady → OpeningObserve(9:30) → RegimeDetect(9:35) → 候选(LongGamma/ShortPremium/EventMode/Chaos/NoTrade) → InitialRiskCheck → SOPReview(LLM) → WaitingForConfirm → FinalRiskCheck → Submitting → PositionOpen → ManagePosition → Closed → (RegimeDetect | DailyStop→Review)。

订单状态机：Proposed →(风控) RiskRejected | AwaitingConfirmation →(确认) Approved →(二次风控) Submitting → Working → PartialFill → Filled；分支 CancelPending / Cancelled / Rejected / Expired / ReconcilePending，SHADOW 终止于 Shadowed。终态：Filled / Cancelled / Rejected / RiskRejected / Expired / Shadowed。

## 7. 错误处理策略

- 全局 fail closed：数据/时钟/行情/Broker 异常一律禁新开仓，记录原因。
- LLM 超时/错误/Schema 无效 → 记 Review Unavailable，核心流程继续，不阻塞减仓/平仓。
- 幂等键防重复订单；启动/重连/下单前/成交后对账；未知状态禁开仓。
- 事务一致性：订单/成交/风控/审计与状态转换同事务或 outbox。原始数据只追加，修正产生新版本；审计不可原地改，更正用补偿事件。

## 8. 测试方案

单元（Python 指标/评分/状态；Rust 去重/定点/风控/状态机/幂等/对账；React 过期/断线只读/确认）、契约（Protobuf 兼容、OpenAPI 客户端编译、Python/Rust 对同一 fixture 一致）、回放（趋势/震荡/缺口/CPI/FOMC/IV crush/低流动性/尾盘 gamma/断流乱序陈旧/Broker 部分成交拒单断线/连亏停机）、策略研究（walk-forward、样本外、成本建模、稳定区间）、LLM 评估（冲突识别率、误报、漏报、注入防护、不可用降级）。

## 9. 阶段计划与里程碑

| Phase | 内容 | 里程碑/验收 |
|---|---|---|
| 0 工程基础与契约 | monorepo、三端骨架、lint/CI、契约生成、PostgreSQL/Alembic/Parquet、compose | 三服务本地起+health；fixture 快照 Rust→Python→React；迁移可升级/回滚 |
| 1 历史数据与离线回放 | ThetaData 历史适配、Parquet、确定性回放时钟、Vol/Regime/Risk/Strategy 初版 | 交易日可重复回放 hash 一致；指标与独立 fixture 一致；覆盖趋势/震荡/事件/数据故障 |
| 2 事件上下文与实时驾驶舱 | 四类事件导入、实时流+健康监控+重连、Live Cockpit/System Health、WS 推送 | 实时状态可追溯；陈旧/断流进禁开仓；连续跑完整交易日无未处理异常 |
| 3 候选交易与半自动闭环 | CandidateTradePlan/SOP 检查、Rust 硬风控/状态机/幂等/审计、Broker paper adapter、UI 确认 | 重复提交不重复下单；部分成交/拒单/撤单/断线恢复/对账通过；订单全链可追溯 |
| 4 LLM 辅助与复盘 | 盘后/盘前/盘中/执行前 SOP Review、Schema/超时/评估集、注入防护、Daily Review | LLM 不可用不影响核心；LLM 不改硬风控/下单；评估集出冲突/误报/漏报指标 |
| 5 Shadow 与 Paper 验证 | 连续运行、滑点校准、walk-forward/样本外、runbook、故障演练 | 达数据质量/风控/对账/审计门槛；无 SOP 绕过；书面 Gate Review |
| 6 受控实盘 | 单独批准、小仓、白名单策略、限时段、双重 kill switch | 逐项启用自动化；异常即停机 |

工期估算见 DEVELOPMENT_PLAN.md 第 14 节；实际以验收条件为准。数据完整性、风控正确性、可恢复性、审计完整性拥有上线否决权。

## 10. MVP 完成定义

可导入并确定性回放 QQQ 股票/期权/VIX；实时展示 Market/Vol/Regime/Risk/Event；生成 NoTrade/LongGamma/ShortPremium 候选信号；记录交易与未交易信号并生成每日复盘；生成候选交易但默认不提交实盘；所有候选经 Rust Gateway；能 fail closed；LLM 只解释不执行；回放/契约/故障/E2E 测试通过；四环境严格隔离。

## 11. 当前进度

Phase 0、1 已完成，Phase 2 代码签收且完整 RTH 现场 soak 待执行。Phase 3 代码闭环已完成：Candidate 1.3 强制 ThetaData 行情证明并区分 OPEN/CLOSE；两阶段 Rust 风控、可信期权快照、Rust package/每腿定价、Longbridge BUY-first 受控拆腿、IBKR BAG、simulated/external paper 路由、PostgreSQL 事务审计/outbox、Fernet key ring 原子轮换、Broker 全账户事实账本、重启恢复、保护性减仓和 UI 人工确认均已接线并通过离线/集成门禁。Phase 3 的代码完成不等于 paper/live 放行：完整 RTH option soak、两家真实 paper 账户的提交/部分成交/撤单/断线/重启故障演练及 Q3 参数书面批准仍归现场 Gate，live 继续无可达路由。

Phase 4 主体切片已完成：五阶段 LLMReview v1.0、严格输入/输出边界、DeepSeek OpenAI-compatible 适配、失败惰性降级、缓存/成本/并发控制、PostgreSQL 0006、审阅 API、中文 Daily Review 与研究队列、离线及真实模型对抗评测均已接通。LLM 端点与任何 Stage/Confirm/Submit/Cancel/Close 路由无调用关系，研究假设在模型、API 与数据库三层均禁止激活。自动盘后聚合/定时调度、盘中异步触发去抖和长期评测基线仍待下一切片。详见 `TASKS.md` 与 ADR 0003。
