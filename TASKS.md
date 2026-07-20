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

- ⬜ Phase 1：历史数据与离线回放（待细化）。Phase 0 已通过 Gate Review，可正式开始。

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

### Phase 2 实时传输骨架（进行中）

| 任务 | 状态 | 说明 |
|---|---|---|
| P2-A 契约 + codegen 骨架 | ✅ | `market.proto`（MarketService: StreamMarketSnapshots→`stream MarketTick` / GetDataHealth）、`cockpit_state.json`；Rust tonic-build（build.rs）、Python grpcio-tools（`scripts/gen_python_grpc.sh`→`app/grpc_gen/`，git 忽略、排除出 gate）。 |
| P2-B Rust 快照流 + DataHealth | ✅ | `market-core/health.rs`（DataHealthMachine 状态机）、`replay.rs`（`SnapshotSource`/`ReplaySnapshotSource` 复用 features.rs、`LiveThetaSource` 占位）；tonic :50051 与 axum :8080 同进程。 |
| P2-C Python 客户端 + 引擎 + WS | ✅ | `app/realtime/`（projector 纯投影 / client proto→dict / session asyncio 桥）；`WS /api/v1/stream/cockpit` + `GET /api/v1/cockpit/state`。 |
| P2-D React 实时驾驶舱 | ✅ | `cockpitState.ts`（解析+双维闸门）、`useCockpitStream.ts`（WS 重连+恢复+断流清帧 fail-closed）、Cockpit 面板+信号日志。 |
| P2 事件上下文导入 | ⏳ 待做 | 宏观/财报/新闻/持仓→EventContext→risk_flags 喂 Strategy（骨架之后的独立批次）。 |
| P2 真实 ThetaData 实时流 | ⏳ 待做 | 经 `LiveThetaSource` 适配器，entitlement/字段映射验收后落地。 |

> 2026-07-21 骨架验收：`make lint`（web/api/core）通过；`make test`——契约 20 / React 48 / Python 159（+1 skip）/ Rust 28 全绿。真实端到端：Rust gRPC 流→Python WS 推帧→React 严格解析。整日回放 smoke：390 根 1m 帧全部 schema 合规、无异常，STALE 窗口正确 fail-closed（不放行新开仓）。gRPC/WS 决策见 `docs/adr/0001-phase2-realtime-transport.md`（CLAUDE.md D4）。

> 2026-07-21 评审复测修复（骨架签收前 5 项）：
> - **P0-1** 干净 checkout 无法启动：`grpc_gen` 被忽略但无 codegen 步骤。已让 `setup-api`/`test-api`/`dev-api`/`lint-api` 依赖 `gen-py-grpc`，CI 增加干净 checkout boot smoke（`import app.main`）。
> - **P0-2** React 断线 fail-open 窗口：最终闸门强制含 `link==="OPEN"`；malformed frame 立即 fail closed（清帧+断链+重连）；recovery 与 WS 帧按 generation+seq 仲裁，旧响应不覆盖新状态。新增 malformed/schema-invalid/late-recovery/CONNECTING 四类测试（web 48→52）。
> - **P0-3** `GetDataHealth` 首帧前误报 HEALTHY：改为共享 `DataHealthMachine`，初始 RECONCILING，仅记录实际发出后推进；实测订阅前=RECONCILING、放完流=HEALTHY。
> - **P1-4** `server_time_utc` 非 Z 结尾：projector 与 main.py 统一 UTC `Z`；补"从未收到帧""缺 snapshot/bar"两分支 schema 校验测试。
> - **P1-5** DataHealth 对缺口/late-start 过乐观：首记录须落在固定 09:30 ET 才 HEALTHY，late-start 保持 RECONCILING；gap/乱序/重连置 sticky，仅 `mark_reconciled()` 后才恢复 HEALTHY（缺口自愈不算）。market-core 25 / bin 4 全绿。

---

## 阻塞与待确认

- Phase 1 后期与 Phase 3 依赖 ASSUMPTIONS.md 中 Q1（ThetaData 接入）、Q2（Broker 接入）、Q3（风控数值）、Q4（LLM）的确认。Phase 0 不受阻塞。
