# TASKS.md

任务清单，按依赖关系排序。状态：⬜ 待办 / 🔄 进行中 / ✅ 完成。每完成一个阶段更新本文件。

任务原则：范围明确、可独立验证、有清晰完成标准、避免一次改过多模块。

---

## 已完成

- ✅ 阅读并理解全部项目文档（4 份）
- ✅ 探查环境：Git 干净、工具链齐全（Node22/uv/cargo/protoc/psql16/compose v5）
- ✅ 创建治理文档：CLAUDE.md / PROJECT_PLAN.md / ASSUMPTIONS.md / TASKS.md

---

## 进行中

- 🔄 Phase 3：候选交易与半自动闭环。首个 paper/shadow 闭环已实现；Broker 实盘适配、动态账户快照、重启对账和真实期权报价证明仍待完成。

---

## Phase 0：工程基础与契约 ✅ Gate 已通过（2026-07-20）

> Gate Review 结论：技术验收通过，未发现阻塞问题。验证：`make test`（16 契约 / 34 React / 27 Python / 3 Rust）、`make lint`（tsc/Ruff/format/Mypy/cargo fmt/Clippy）、`make contracts`、`git diff --check`、Gitleaks 全部通过；非法上游响应（字符串布尔、字符串整数、非法时间、`nan` 价格、缺失 `schema_version`）严格拒绝并降级为 unreachable / HTTP 503 / No Trade；核心健康但快照失效时 Cockpit 保持 No Trade；PostgreSQL 升级/回滚/再升级 16 表正常。
> 非阻塞跟进：FastAPI 测试使用的 Starlette TestClient 提示未来迁移到 httpx2，不影响 Phase 1。

> 目标：三服务可本地启动并 health check；fixture 快照 Rust→Python→React 贯通；迁移可升级/回滚。

| ID | 任务 | 依赖 | 完成标准 |
|---|---|---|---|
| P0-1 ✅ | monorepo 骨架：目录结构 + `.gitignore` + 根 Makefile + npm workspaces 根配置 + `.env.example` | — | 目录按 PROJECT_PLAN 第 2 节建立；`.gitignore` 覆盖密钥/node_modules/target/.venv/parquet；`make help` 列出目标 |
| P0-2 ✅ | React 骨架（Vite+TS）+ Cockpit 页（拉取 core-health，数据不健康默认 STALE/No Trade） | P0-1 | `npm run build` 通过；`make test-web` 覆盖 typecheck+build；Cockpit 渲染交易许可状态 |
| P0-3 ✅ | Python application-api 骨架（FastAPI+uv）+ `GET /api/v1/health` | P0-1 | uv sync 成功；`/health` live 返回 200；pytest 通过 |
| P0-4 ✅ | Rust trading-core workspace 骨架（market-core/risk-gateway/execution/broker + bin crates）+ HTTP health | P0-1 | `cargo build`+`cargo clippy -D warnings`+`cargo test`(2 单测)通过；`/health` live 返回 fail-closed DISCONNECTED 默认 |
| P0-5 ✅ | 核心数据契约：JSON Schema 冻结首批（common/MarketSnapshot/OptionSnapshot/Signal/CandidateTradePlan/EventContext/DataHealth/BrokerHealth）+ 通用字段 + 契约测试 | P0-1 | schema 入 `packages/contracts/jsonschema`；9 项契约测试通过（编译+fixture 正/负校验）。Protobuf 生成待 proto/ 定案后补（gen_contracts.sh 已就位） |
| P0-6 ✅ | PostgreSQL + Alembic：compose 起库 + 初始迁移（7 schema + 首批 16 核心表骨架） | P0-1 | compose 定义 PG16；`make migrate` 空库升级成功（live 验证 16 表创建）；`make migrate-down` 回滚至 base 干净；再升级幂等。SQLx 离线元数据待 trading-core 出现真实查询后补（Phase 0 crate 仅 health，无 DB 查询） |
| P0-7 ✅ | 端到端 smoke（Phase 0 版）：core-health 经 Rust(HTTP)→Python(BFF proxy)→React 显示；core 下线时全链路 fail-closed 切 STALE/No Trade | P0-2,3,4,5 | live 验证：core→API proxy 返回实时数据；core 下线时 proxy 返回 STALE/DISCONNECTED，Cockpit 渲染 No Trade。gRPC stream + WS 推送在 Phase 2 落地 |
| P0-8 ✅ | 环境与密钥模板 + 日志规范 + pre-commit（lint/format/secret scan）+ CI 骨架 | P0-1 | `.env.example` 无真实密钥；`.pre-commit-config.yaml` 本地钩子调用已装工具，gitleaks 实测拦截真实高熵密钥（exit 1）；`docs/LOGGING.md` 定义结构化 JSON+脱敏+审计通道；`.github/workflows/ci.yml` 跑 secret-scan/contracts/web/api(含 PG16+migrate)/core 五 job；`make lint-api` 修正既有类型标注后 exit 0 |

## Phase 1：历史数据与离线回放（摘要，Phase 0 完成后细化）

- P1-1 ThetaData 历史适配器：Python Research Job 使用官方 Python/gRPC SDK 下载 QQQ 股票/期权/VIX；Rust Market Core 负责生产标准化与数据质量，Python 保留离线兼容 Parquet 转换
- P1-2 确定性底层特征：VWAP/opening range/HV20/HV60/ATM/straddle/spread/quote age（Rust 权威实现；Python 仅作离线 fixture 对拍）
- P1-3 确定性回放时钟 + 事件驱动管线（Python）
- P1-4 Regime Engine 初版（Trend/Range/Event/Chaos/No Trade + 评分）（Python）
- P1-5 Vol Engine 初版（IV/HV、implied/realized move、状态分类）（Python）
- P1-6 Strategy Engine 初版（No Trade/Long Gamma/Short Premium 选择 + 只读风险预检查）（Python）
- P1-7 信号与 No Trade 原因记录 → PostgreSQL review/audit（Python）
- P1-8 回放可重复性：同一交易日结果 hash 一致 + 指标 fixture 对拍 + 场景覆盖

> Phase 1 职责说明：ThetaData v3 本地 SDK 当前通过 Python/gRPC 提供，因此历史下载属于 Python Research Job；进入交易许可链的标准化记录、DataHealth 与确定性底层特征仍以 Rust Market Core 为唯一权威。Python 同名计算仅用于离线研究和独立 fixture，不得作为 paper/live 交易许可输入。
>
> 2026-07-21 修复复测：`make test`（16 契约 / 34 React / 145 Python / 10 Rust）与 `make lint` 全部通过；另在隔离 PostgreSQL 16 上通过迁移、真实 FK/JSONB/timestamptz 和 4 路并发 signal 幂等测试。已覆盖固定 09:30 ET Opening Range、缺 bar 降级、provider VWAP、HV20/HV60 日线口径、同 expiry/同时间 straddle、Short Premium 完整门槛、event/data-fault replay，以及完整 Signal label→contract enum 映射和 `signal.json` 校验。进入 Phase 2 前仍需以真实 ThetaData 样本完成 entitlement/字段映射和 Rust runtime snapshot 接线验收。

## Phase 2-6：见 PROJECT_PLAN.md 第 9 节

Phase 2 事件上下文与实时驾驶舱 / Phase 3 候选交易与半自动闭环 / Phase 4 LLM 辅助与复盘 / Phase 5 Shadow 与 Paper 验证 / Phase 6 受控实盘。各阶段任务在前一阶段收尾时展开。

### Phase 3 候选交易与半自动闭环（开发中）

| 任务 | 状态 | 说明 |
|---|---|---|
| P3-A 执行契约 | ✅ | Candidate/Risk/Order JSON Schema，`execution.proto` 两阶段风控与订单 RPC，`broker.proto` 账户/持仓/订单/成交/提交/撤单/对账 sidecar 契约。 |
| P3-B 候选计划与仓位 | ✅ | Python 确定性组合定价、defined-risk 校验、风险预算取整、最大张数、TTL、Protobuf hash/idempotency key；CONTROLLED_AUTO 禁用。 |
| P3-C Rust 两阶段硬风控 | 🔄 | 已覆盖市场/Broker/EventContext、快照/hash/TTL、定义风险、账户限额、quote age/spread/size、Greeks、chain proof、策略白名单、美东开仓窗口、市价新开仓闭锁、规则版本与 kill switch；真实 Rust 期权报价权威源和 Q3 参数批准待补。 |
| P3-D 状态机与 paper adapter | 🔄 | 已覆盖确认、幂等、三类订单语义、Rust 自适应定价、提交/部分成交/拒单/撤单/断线与 ReconcilePending；API 读取会与 Rust 对账并拒绝重启后的幽灵投影。workflow 自动重建和真实 broker 对账仍未完成。 |
| P3-E PostgreSQL 审计与恢复保护 | 🔄 | 计划/风险/订单/确认意图/状态事件事务化；确认能力以 Fernet 密文共享并一次性 claim；独立 `state_version` 列、行锁和条件更新阻止并发回退。自动对账恢复/outbox 与密钥轮换待补。 |
| P3-F React 人工确认 | ✅ | 精确计划 hash 确认、TTL、Cockpit 双闸门、取消、状态/成交显示、异常响应 fail closed；request generation + `state_version` 防迟到轮询回退；浏览器视觉回归仍待环境修复后执行。 |
| P3-G Longbridge/IBKR adapter | 🔄 | Longbridge 4.3.3 Rust SDK 已接账户/持仓/单腿下单撤单并拒绝多腿；IBKR 已完成 TWS/Gateway 严格端点、nextValidId/account 握手、OPT/BAG 与 MKT/LMT/Adaptive 映射。订单/成交全量流、sidecar gRPC 服务、重启对账和 paper 现场认证待补，live 提交保持禁用。 |
| P3-H Gate/E2E | ⬜ | 需完成 Broker 部分成交/拒单/断线/重启对账 E2E、完整订单追溯、真实期权报价证明和 Q3 风控参数批准后签收。 |

### Phase 2 事件上下文与实时驾驶舱（开发完成；现场验收待执行）

| 任务 | 状态 | 说明 |
|---|---|---|
| P2-A 契约 + codegen 骨架 | ✅ | `market.proto`（MarketService: StreamMarketSnapshots→`stream MarketTick` / GetDataHealth）、`cockpit_state.json`；Rust tonic-build（build.rs）、Python grpcio-tools（`scripts/gen_python_grpc.sh`→`app/grpc_gen/`，git 忽略、排除出 gate）。 |
| P2-B Rust 快照流 + DataHealth | ✅ | `market-core/health.rs` 状态机、replay source、Python ThetaData SDK bridge + Rust 严格校验/当日回补；tonic :50051 与 axum :8080 同进程。 |
| P2-C Python 客户端 + 引擎 + WS | ✅ | `app/realtime/`（projector 纯投影 / client proto→dict / session asyncio 桥）；`WS /api/v1/stream/cockpit` + `GET /api/v1/cockpit/state`。 |
| P2-D React 实时驾驶舱 | ✅ | `cockpitState.ts`（解析+双维闸门）、`useCockpitStream.ts`（WS 重连+恢复+断流清帧 fail-closed）、Cockpit 面板+信号日志。 |
| P2 事件上下文导入 | ✅ | 四类严格契约、文件导入/覆盖与来源校验、确定性 EventContext、PostgreSQL+审计、Strategy/Cockpit/API 注入；缺失即 fail closed。 |
| P2 ThetaData 实时流 | ✅ 代码与短 smoke / ⏳ 全日现场 | 官方 Python SDK 直连、凭证隔离、已完成 RTH 1m 轮询、占位过滤、Rust 双重校验/前缀回补、静默 watchdog 与 gRPC mock 集成完成；真实 Standard 凭证 QQQ 三分钟 OHLC 已通过，完整 RTH soak 待验收。 |

> 2026-07-21 骨架验收：`make lint`（web/api/core）通过；`make test`——契约 20 / React 48 / Python 159（+1 skip）/ Rust 28 全绿。真实端到端：Rust gRPC 流→Python WS 推帧→React 严格解析。整日回放 smoke：390 根 1m 帧全部 schema 合规、无异常，STALE 窗口正确 fail-closed（不放行新开仓）。gRPC/WS 决策见 `docs/adr/0001-phase2-realtime-transport.md`（CLAUDE.md D4）。

> 2026-07-21 评审复测修复（骨架签收前 5 项）：
> - **P0-1** 干净 checkout 无法启动：`grpc_gen` 被忽略但无 codegen 步骤。已让 `setup-api`/`test-api`/`dev-api`/`lint-api` 依赖 `gen-py-grpc`，CI 增加干净 checkout boot smoke（`import app.main`）。
> - **P0-2** React 断线 fail-open 窗口：最终闸门强制含 `link==="OPEN"`；malformed frame 立即 fail closed（清帧+断链+重连）；recovery 与 WS 帧按 generation+seq 仲裁，旧响应不覆盖新状态。新增 malformed/schema-invalid/late-recovery/CONNECTING 四类测试（web 48→52）。
> - **P0-3** `GetDataHealth` 首帧前误报 HEALTHY：改为共享 `DataHealthMachine`，初始 RECONCILING，仅记录实际发出后推进；实测订阅前=RECONCILING、放完流=HEALTHY。
> - **P1-4** `server_time_utc` 非 Z 结尾：projector 与 main.py 统一 UTC `Z`；补"从未收到帧""缺 snapshot/bar"两分支 schema 校验测试。
> - **P1-5** DataHealth 对缺口/late-start 过乐观：首记录须落在固定 09:30 ET 才 HEALTHY，late-start 保持 RECONCILING；gap/乱序/重连置 sticky，仅 `mark_reconciled()` 后才恢复 HEALTHY（缺口自愈不算）。market-core 25 / bin 4 全绿。

> 2026-07-21 评审复测第 2 轮修复（2 个新 P0/P1 阻塞 + 整日 smoke 入库）：
> - **阻塞1 [P0]** 多订阅污染全局 DataHealth：`grpc.rs` 每个 StreamMarketSnapshots 客户端都从 replay 起点重发并推进同一 DataHealthMachine（第二订阅→DEGRADED/out_of_order=6，且 snapshot 仍 HEALTHY 与 GetDataHealth 矛盾＝fail-open）。重构为**单一生产者**：构造期预计算 ticks + 逐记录 health_states（status 钉死为 snapshot.data_health，二者永不矛盾）；单 producer clock 推进共享 cursor，仅它驱动 DataHealth；订阅者从当前 cursor 前进、不重摄历史、不推进 health。实测 after_second=HEALTHY/0。bin 4→5。
> - **阻塞2 [P1]** WS 重连 seq 重置：`session.py` 每连接新建 projector→seq 从 0，前端丢弃全部重连帧。重构为 **per-session SessionHub**：唯一 projector（seq 全生命周期单调）、唯一上游、唯一最新帧、多 WS 订阅同一广播、引擎每 tick 每 session 只跑一次。新增 hub 级测试：重连后首 LIVE seq > 断线前最后 seq、多客户端同投影。
> - **整日 smoke 入库**：`tests/test_fullday_smoke.py`——390 RTH 帧全 schema 合规、STALE 整段 fail-closed、seq 单调、无异常。
> - Gate：lint 三端通过；契约 20 / web 52 / api 163(+1skip) / core 31。实测双 gRPC 订阅不改 DataHealth、订阅前 RECONCILING。

> 2026-07-21 评审复测第 3 轮修复（故障注入发现的 2 阻塞 + smoke 范围）：
> - **[P0]** Rust 唯一 producer 在消费者暂时断开后永久停止：`watch::send()` 零 receiver 报错→producer 退出且 `started` 恒 true 不重启，后续订阅 DEADLINE_EXCEEDED、GetDataHealth 卡在 HEALTHY。修复：service 永久持有 keepalive receiver + `send_replace()`→producer 生命周期与订阅者数量无关；跑完置 finished→GetDataHealth 返回 DISCONNECTED（不停留 HEALTHY）；晚到订阅者得干净结束流不 hang。实测 health_after_gap=DISCONNECTED、second_received=[]。bin 5→7。
> - **[P1]** SessionHub 上游结束后无法重建：`_ended` 后 hub 永久留在 `_HUBS`，前端重连只拿旧 DISCONNECTED。修复：`_run` 改带退避重连循环，SAME projector 保持 seq 单调，end/error 发 DISCONNECTED 后退避重连、恢复后续 LIVE 更高 seq；加 `stop()`；`get_hub` 对 stopped hub 重建。新增重连测试（恢复后 LIVE seq > 断线前 seq）。
> - **[P1] smoke 范围**：原 test_fullday_smoke 高估。重命名 test_projector_fullday_smoke（诚实标注仅 projector 层）+ 真实 data gap（minute_et 非连续）+ seq==range(390)；新增 test_integration_smoke（真启 trading-core 二进制→真 gRPC→SessionHub→CockpitState，全帧 schema 合规、≥6 LIVE、feed 结束现 DISCONNECTED；二进制缺失则 skip）。
> - Gate：lint 三端通过；契约 20 / web 52 / api 165(+1 既有 DATABASE_URL skip) / core 33。集成 smoke 实测 PASSED。

> 2026-07-21 评审复测第 4 轮修复（重连数据完整性 fail-open + CI + smoke 断言）：
> - **[P0]** 重连丢失 bar 未回补即放行：新订阅者从当前 cursor 起，断线期间记录被跳过；projector 不校验 sequence 连续性。修复：proto `StreamRequest.resume_after_sequence`，Rust 从 session buffer 回放 seq>resume（resume=0 从 session open）；projector 加连续性守卫（下一条须 =_last+1，gap/reorder/dup/首条>1 → STALE+禁开仓+risk_flag，不推进不 append，待回补）；hub 追踪 last market seq 并在重连时传 resume 回补缺口。实测端到端 accepted LIVE seqs=[1..6] gap-free。
> - **[P1]** 集成 smoke 干净 CI 未跑（api job 不 build Rust → skip）：新增 CI `integration` job（build core + 跑 smoke，`OPTIONTRADER_REQUIRE_INTEGRATION=1` 禁 skip、缺二进制硬 FAIL）。
> - **[P2]** projector smoke gap 断言形同虚设：改为解析 timestamp 断言实际分钟差 == 1+_GAP_MINUTES。
> - Gate：lint 三端通过；契约 20 / web 52 / api 167(+1 既有 DATABASE_URL skip) / core 35。

> 2026-07-21 Codex 第 5 轮修复（回补阶段交易许可 P0）：
> - proto `MarketTick` 新增 `DeliveryPhase{BACKFILL,LIVE}` 与 `high_watermark_sequence`；Rust 单 producer 将落后订阅者追平 high-watermark 的历史记录全部标为 BACKFILL，只有追平后新产生的记录才为 LIVE。
> - Python client 透传 transport phase；Projector 在 BACKFILL 期间照常按序重建 bars/Regime/Vol/Signal，但 CockpitState 强制 STALE/No Trade。sequence gap 记录 reconcile target，补齐目标前持续阻断；SessionHub 以 projector 已接受的连续 market sequence 作为下一次 resume cursor。
> - 新增应用重启历史回放、gap target 持续阻断及真实 Rust→gRPC→SessionHub 集成断言：producer 已结束后重启 Application Service，6 根历史快照全部 BACKFILL、无任何 LIVE/可开仓帧。
> - Gate：lint 三端通过；契约 20 / web 52 / api 168(+1 既有 DATABASE_URL skip) / core 36；强制跨语言 integration smoke 通过。

> 2026-07-21 Codex 第 6 轮修复（独立评审 P2 纵深防御）：
> - Projector 不再单点信任 `delivery_phase`：`high_watermark_sequence` 必须为正整数、不小于当前 market sequence、且 session 内单调；LIVE 尚未到达 watermark 时仍 STALE/No Trade。
> - gap/reconcile 闭锁保持到越过恢复目标；即使上游把回补错误标为 LIVE，目标帧本身仍禁止新开仓，只有下一条真实 live-edge 记录才可能恢复许可。
> - 新增 `make test-integration`，强制构建当前 `trading-core` 并禁用 integration smoke 的 skip 路径；根 `make test` 纳入该门禁，CI 复用同一入口。Rust 测试移除依赖调度速度的“首订阅全 LIVE”假设。
> - Gate：`make lint` 三端通过；`make test`——契约 20 / React 52 / Python 170（+1 既有 DATABASE_URL skip）/ Rust 36 / 强制跨语言 integration 1 全绿。

> 2026-07-21 独立复审签收（commit `c22c0d1`）：未发现 P0/P1；watermark 独立闸门、目标帧闭锁和强制跨语言门禁均经独立故障注入确认。实时传输骨架正式关闭，可开展下一切片。非阻塞尾项已跟进：Rust 增加确定性的“追平后下一条记录为 LIVE”状态测试；`docs/RUNBOOK.md` 记录同 session Core 重启后的粘滞闭锁与恢复步骤。尾项 Gate：契约 20 / React 52 / Python 170（+1 既有 DATABASE_URL skip）/ Rust 37 / 强制跨语言 integration 1 全绿。Phase 2 的事件上下文导入和真实 ThetaData 实时源仍未完成，不随骨架签收自动视为完成。

> 2026-07-21 Phase 2 剩余开发：新增四类事件输入契约与 EventContext file importer，覆盖 ET 日期、来源时间、置信度、持仓时效、事件窗口及 PostgreSQL/审计写入；Projector/Strategy/API/React 最终闸门均要求事件上下文 available。Theta 源已改为官方 Python SDK 直连：Python 隔离凭证并输出完整分钟，Rust 严格校验并负责动态 gRPC/DataHealth；真实凭证短区间 smoke 已通过。完整交易日 soak 尚未执行，因此 Phase 3 开发可并行开始，但 paper/live Gate 不得据此签收。
> 最终 Gate：`make lint` 全通过；`make test` 为契约 24 / React 53 / Python 181（+1 既有数据库环境 skip）/ Rust 44 / 强制跨语言 integration 1 全绿。

---

## 阻塞与待确认

- Phase 1 后期与 Phase 3 依赖 ASSUMPTIONS.md 中 Q1（ThetaData 接入）、Q2（Broker 接入）、Q3（风控数值）、Q4（LLM）的确认。Phase 0 不受阻塞。
