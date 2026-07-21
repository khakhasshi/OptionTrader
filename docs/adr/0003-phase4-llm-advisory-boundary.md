# ADR 0003：Phase 4 LLM 只读辅助边界

- 状态：Accepted
- 日期：2026-07-22

## 背景

系统需要盘前解释、盘中异步说明、执行前 SOP 语义审阅、盘后复盘和规则研究，同时首要
安全目标仍是阻止错误交易。LLM 输出不稳定、外部 Provider 可用性不可控，输入中的新闻或
事件文本也可能包含提示词注入，因此 LLM 不能进入确定性风险和执行关键路径。

## 决策

1. 权威顺序保持 `Rust Risk & Execution Gateway > Python deterministic engine > LLM > UI`。
   LLM 没有工具、Broker、订单、撤单、平仓或风险参数接口；`Proceed` 仅表示未发现语义冲突。
2. 输入使用 `llm_review_request.json` / Pydantic 白名单。发送前删除 `raw_ref`，拒绝 secret-like
   字段、非有限数、超限结构和中英文注入模式。新闻与事件正文始终按不可信数据处理。
3. 输出使用 LLMReview v1.0 严格契约。Provider 返回必须依次通过 JSON、Pydantic、阶段互斥、
   来源 ID 和建议动作校验。错误可在固定次数内重试，仍失败则生成惰性的 Review Only。
4. PRE_EXECUTION 不信任调用方提交的计划和 Initial Risk；Application API 必须从 PostgreSQL
   重读同 plan hash 的 Candidate 与 APPROVED Initial Risk。审阅结果不接 Stage/Confirm/Submit。
5. Provider 采用配置化 OpenAI-compatible JSON mode，当前开发配置为 DeepSeek。密钥只来自
   服务端环境；React 设置页只生成本地配置草稿，不保存、不上传、不回显已有密钥。
6. 每次审阅原子写入 `review.llm_reviews`、审计事件和 outbox。盘后详情写入 daily review；研究
   假设固定 `activation_allowed=false`，数据库 check constraint 禁止越权激活。
7. 缓存 key 包含 Provider、模型、prompt 版本、规则版本、阶段和输入 hash。成本预留包含完整
   prompt，并按最大尝试数保守计算；另设并发、每日请求和每日估算金额上限。第一版缓存与
   预算计数为单进程状态，多 worker 前必须迁移到共享 PostgreSQL 配额账本。
8. 评测集必须报告结构化成功率、冲突召回、误报、漏报 case、注入阻断和不可用惰性降级。
   模型或 prompt 版本变化后重新运行，不以一次真实 smoke 替代长期漂移监控。

## 后果

- Provider 不可用只减少解释能力，不降低硬风控或退出能力。
- LLM 可能建议 Wait/Cancel/Reduce Risk，但当前版本不会自动改变候选计划或执行状态。
- 请求驱动五阶段能力已经接通；自动盘后聚合、交易日调度和盘中去抖触发另行实现。
- 当前建议单 API worker；数据库唯一 request_id 保证落库不重复，但共享 single-flight 与
  跨 worker 成本配额尚未实现。
- 新 Provider 必须通过相同契约、故障注入和评测 Gate，不能因兼容性放宽 Schema。
