# 日志规范 (Logging Conventions)

适用于三端：Rust `trading-core`、Python `application-api`、React `web`。
目标：**结构化、可追溯、脱敏**。日志是审计与事故复盘的一等公民，不是调试残留。

## 1. 格式

- 一律 **JSON 单行**（每行一个事件），UTF-8，时间戳为 UTC ISO-8601（毫秒精度，带 `Z`）。
- stdout 输出；由部署环境采集。禁止写本地文件作为主日志通道。
- Rust: `tracing` + `tracing-subscriber`（JSON formatter）。
- Python: `structlog`（或 stdlib `logging` + JSON formatter），FastAPI 请求中间件注入关联字段。
- Web: 仅错误与关键用户操作上报；禁止把行情/持仓明细打到浏览器 console。

## 2. 必备字段

每条日志必须包含：

| 字段 | 说明 |
|---|---|
| `timestamp` | 事件发生时间 (UTC ISO-8601) |
| `level` | `trace/debug/info/warn/error` |
| `service` | `trading-core` \| `application-api` \| `web` |
| `message` | 人类可读简述（不含敏感值） |
| `correlation_id` | 贯穿一次业务链路（与数据契约通用字段一致） |

链路相关时追加：`event_id / causation_id / session_id / source / rule_version`
（对齐 CLAUDE.md 通用字段与 DEVELOPMENT_PLAN 第 9 节可追溯要求）。

## 3. 级别用法

- `error`：需要人工介入或已触发 fail-closed 的异常（Broker 对账不一致、风控评估失败、Schema 校验失败）。
- `warn`：降级但仍可运行（行情 STALE、重连、LLM Schema 重试）。
- `info`：状态迁移与关键业务事件（信号产生、计划生成、风控放行/拒单、订单提交/成交、模式切换）。
- `debug/trace`：仅开发；生产默认关闭。

## 4. 脱敏 (安全关键)

以下**禁止**进入日志（对齐 DEVELOPMENT_PLAN 第 11 节、CLAUDE.md 安全约束）：

- 账户号、Broker token / access token / API key / secret。
- 订单原始凭证、回调 raw payload 中的凭证段。
- 任何 PII（姓名、邮箱、电话、身份证件）。

规则：
- 记录**引用**而非**值**（如 `account_ref` 用内部脱敏 ID，不用真实账号）。
- token 类字段若必须出现，只留末 4 位并标注 `redacted`。
- 密钥扫描由 pre-commit `gitleaks` 兜底，但日志脱敏是主防线，不得依赖扫描。

## 5. 指标 (Metrics)

结构化日志之外，关键指标以 metrics 暴露（Phase 2+ 落地），指标名对齐 DEVELOPMENT_PLAN 第 11 节：
`market_event_lag_ms / quote_age_ms / out_of_order_count / risk_evaluation_latency_ms /
order_submit_latency_ms / broker_reject_count / position_reconciliation_diff / llm_latency_ms` 等。

## 6. 审计日志 (Audit)

交易确认、规则变更、停机/恢复、LLM 输出写入 PostgreSQL `audit` schema，**不可原地修改**；
更正使用补偿事件（对齐 DEVELOPMENT_PLAN 第 9 节）。审计写入与应用日志是两条独立通道。
