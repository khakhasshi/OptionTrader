# ASSUMPTIONS.md

记录当前假设、待确认问题和文档冲突。分三类处理：**假设**（不影响整体架构，已做合理默认并记录）、**必须确认**（可能导致大规模返工的关键待确认事项）、**文档冲突/歧义**（发现的不一致）。

更新规则：假设一旦被确认或推翻，移动到对应结论并注明日期。

---

## A. 已做假设（不阻塞开发，可后续调整）

| ID | 假设 | 依据 | 影响面 | 可逆性 |
|---|---|---|---|---|
| A1 | JS 包管理用 npm workspaces，Python 用 uv | 本机无 pnpm/yarn/poetry，均已验证 npm/uv 可用 | 工程配置 | 高，切换成本低 |
| A2 | 单仓 monorepo，三服务同库 | DEVELOPMENT_PLAN 第 3/4 节 | 目录结构 | 中 |
| A3 | Phase 0 的 gRPC「快照」端到端 smoke 使用 fixture 数据，不接真实 ThetaData | Phase 0 验收只要求「一个示例快照」贯通 | 无 | 高 |
| A4 | PostgreSQL 本地开发实例通过 docker compose 提供（版本 16，与本机 psql 一致） | 本机 psql 16.14 | 迁移/连接配置 | 高 |
| A5 | Rust trading-core 第一版单进程、内部 crate 隔离，暂不拆 market-core / execution-gateway | DEVELOPMENT_PLAN 3.3 明确允许 | 部署 | 中 |
| A6 | 账户净值单日最大亏损默认取区间下限 1%（设计文档给 1%-2%） | 保守优先，符合「先避免错误交易」 | 风控参数（config，非架构） | 高 |
| A7 | opening range 定义为开盘 15 分钟高低点（9:30-9:45 ET） | 设计文档趋势分「突破开盘 15 分钟区间」 | 指标计算 | 中 |
| A8 | 时区库/交易日历以 ET（America/New_York，含夏令时）为决策时区 | UTC 存储、ET 决策 | 全局时间处理 | 低（须一开始正确） |
| A9 | 金额定点方案：价格与权利金用 numeric(全库统一小数位待迁移定稿) | 禁止浮点做最终判断 | 数据模型 | 低 |

---

## B. 必须确认（可能大规模返工，进入关键待确认事项）

| ID | 问题 | 为何关键 | 暂行默认 |
|---|---|---|---|
| Q1 | ThetaData 实时流的 Rust 接入形态及并发/速率限额仍待实测；历史接口已确认使用本地官方 Python/gRPC SDK | 决定 Rust Market Core 实时连接层、限流、缓存与回补设计 | Phase 1 历史下载由 Python Research Job 执行；生产标准化、DataHealth 和底层特征仍由 Rust 权威处理；实时接入在 Phase 2 前完成实测 |
| Q2 | Longbridge/IBKR paper 账户的现场认证步骤及异常状态映射 | adapter 领域契约已固定，但决定会话恢复和现场 Gate | Longbridge 原生 Rust SDK adapter，无原生 combo 时 BUY-first 受控拆腿；IBKR 本机 TWS/Gateway sidecar 使用 BAG；两者统一 `broker.proto`。订单/成交流、自动重启对账与 paper 现场认证仍待 Phase 3 后续切片 |
| Q3 | 账户货币、初始净值、最大张数、单笔最大损失等硬风控具体数值由谁批准？ | 架构与闸门已落地，但 paper/live Gate 前必须由人确认并版本化 | 默认 `RISK_LIMITS_CONFIRMED=false`、规则 `UNCONFIRMED`、buying power=0；占位数值不能作为批准值 |
| Q4 | LLM 供应商与模型（Anthropic/OpenAI/本地）、是否有可用密钥、成本预算？ | 决定 llm 模块的 client、Schema 校验、超时/重试/缓存策略 | Phase 4 才接入；先定义 Pydantic Schema 与 provider 无关接口 |
| Q5 | 是否需要用户认证/多用户？权限分 Viewer/Trader/Risk Admin 如何落地（本地单人还是团队）？ | 决定 API 鉴权与前端权限模型 | 假定单用户本地部署，权限角色先建枚举，认证 Phase 3 细化 |
| Q6 | 部署目标：纯本地开发机，还是需要云/服务器长时间运行 shadow/paper？ | 决定 infra、备份、RPO/RTO、监控栈选型 | 先本地 docker compose；shadow/paper 前再定 |

---

## C. 文档冲突与歧义

| ID | 冲突/歧义 | 位置 | 处理 |
|---|---|---|---|
| C1 | 示例 `CandidateTradePlan` 用 `rule_version: "rules_0.2.0"`，但文档正文版本为 v0.3 | DESIGN 第 10 节 vs 文档头 | 视为示例值，非规范；rule_version 由 config 独立管理，与文档版本无关 |
| C2 | 趋势/震荡分类：`Trend Score>=5 且 Range Score<4` 判 Trend，但 4 分区间（如两者都=4）未定义 | DESIGN 4.3 | 归入 No Trade（保守）；实现时把「未明确命中」一律落 No Trade，记录原因 |
| C3 | 时间窗口重叠：Long Gamma 主窗口 9:45-11:00 与 Short Premium 可评估窗口 10:00-11:30 重叠 | DESIGN 第 9 节 | 不冲突：窗口是「允许评估」而非互斥；主剧本唯一性由「只允许一个主剧本」规则保证 |
| C4 | Realized Intraday Move 用 max(|price-open|,|price-high|,|price-low|)，「high/low」是日内累计极值口径需明确 | DESIGN 4.4 | 取自开盘以来日内累计高/低（intraday running high/low），实现时写入指标文档并加 fixture 验证 |
| C5 | 单日最大亏损 1%-2% 为区间；连亏暂停 30 分钟、连亏 3 笔停止为固定值 | DESIGN 4.2 | 见 A6，取保守默认，最终数值由 Q3 确认 |
| C6 | 「0DTE ATM straddle」与「1DTE 方向期权」混用，MVP 期权到期口径（是否只做 0DTE）未完全统一 | DESIGN 4.5 | MVP 聚焦 0DTE，1DTE 作为 Long Gamma 可选结构保留字段，回放/风控先按 0DTE 校准 |

---

## D. 缺失信息清单

- 缺 Longbridge / IBKR / LLM 的现场认证结果与连接参数（凭证不入 Git，用 secret 模板占位）。ThetaData SDK 凭证短区间 smoke 已通过。
- 缺具体硬风控数值表（见 Q3）。
- 缺 QQQ top 20 持仓与财报的初始种子数据（Phase 2 用免费官方源补齐）。
- 缺 RPO/RTO、备份策略的量化目标（paper 前确定）。
- 缺 CI 平台选择（GitHub Actions / 其他）——Phase 0 先写平台无关的脚本，CI 配置待定。

---

## 开工结论

文档足以支撑 Phase 0（工程基础与契约）与 Phase 1（历史回放）启动：核心交易数据源、执行接口、架构边界、数据契约、状态机、测试与上线门槛均已明确。B 类问题主要影响 Phase 1 后期（真实数据接入）与 Phase 3（Broker 下单），不阻塞 Phase 0。按原始文档指引：先 Phase 0，再 Phase 1，不第一步做自动下单。
