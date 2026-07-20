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

- P1-1 ThetaData 历史适配器（QQQ 股票/期权/VIX）→ 标准化 → Parquet（Rust）
- P1-2 确定性底层特征：VWAP/opening range/HV20/HV60/ATM/straddle/spread（Rust）
- P1-3 确定性回放时钟 + 事件驱动管线（Python）
- P1-4 Regime Engine 初版（Trend/Range/Event/Chaos/No Trade + 评分）（Python）
- P1-5 Vol Engine 初版（IV/HV、implied/realized move、状态分类）（Python）
- P1-6 Strategy Engine 初版（No Trade/Long Gamma/Short Premium 选择 + 只读风险预检查）（Python）
- P1-7 信号与 No Trade 原因记录 → PostgreSQL review/audit（Python）
- P1-8 回放可重复性：同一交易日结果 hash 一致 + 指标 fixture 对拍 + 场景覆盖

## Phase 2-6：见 PROJECT_PLAN.md 第 9 节

Phase 2 事件上下文与实时驾驶舱 / Phase 3 候选交易与半自动闭环 / Phase 4 LLM 辅助与复盘 / Phase 5 Shadow 与 Paper 验证 / Phase 6 受控实盘。各阶段任务在前一阶段收尾时展开。

---

## 阻塞与待确认

- Phase 1 后期与 Phase 3 依赖 ASSUMPTIONS.md 中 Q1（ThetaData 接入）、Q2（Broker 接入）、Q3（风控数值）、Q4（LLM）的确认。Phase 0 不受阻塞。
