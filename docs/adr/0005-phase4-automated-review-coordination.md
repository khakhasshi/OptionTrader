# ADR 0005：Phase 4 自动审阅与多 Worker 协调

## 状态

已接受（2026-07-22）

## 背景

请求驱动的 LLM 审阅已经具备严格契约和只读边界，但盘后复盘需要在交易日收盘后自动汇总
完整事实，盘中解释需要监听状态变化，同时多 API worker 不能重复消费预算或调用 Provider。
这些能力不得进入交易和风控关键路径，也不能因队列积压延迟退出。

## 决策

1. `exchange_calendars` 的 XNYS 日历物化最近交易会话，包含节假日和早收盘。POST_MARKET 只有
   在 session 已关闭并超过 grace、信号与可用 EventContext 完整、全部订单终态、残余敞口为零，
   且每笔已提交订单存在同 session、晚于收盘和订单更新时间的 HEALTHY 无差异 Broker 快照时
   才可进入 outbox；否则记录 `WAITING_INERT` 原因且不调用 Provider。
2. 盘后请求从 PostgreSQL 聚合当日信号、候选计划、订单、成交、风险判断、事件上下文、
   Broker 状态和未交易原因。request id 固定为 `post_market:<date>:v1`，输入与来源排序确定。
3. 盘中审阅以 `audit.outbox_events` 为只读事实源。LLM 自有 cursor 只复制白名单 topic，按
   `(session_id, event_fingerprint)` 去重，经过 debounce 和 session 级最小间隔后最多合并 50 条
   事件。交易/风控生产者不知道 LLM 队列，不等待它，也不承担其 backlog。
4. 自动请求写入既有 transactional outbox。worker 通过 `SKIP LOCKED` 租约消费；消息 payload
   必须与 durable automation run 的 event id、request id、session、阶段、日期和 SHA-256
   trigger hash 完全一致。存储/worker 失败退避重试，达到上限进入 dead letter。
5. `review.llm_daily_budgets` 以日期行锁原子预留请求数和最坏估算成本；
   `review.llm_request_leases` 以 request id 保存输入身份、所有者、租约、最终 LLMReview 和实际
   遥测。相同 request id/输入跨 worker 只有一个 leader；不同输入立即冲突。
6. Provider 调用开始后若 worker 消失，外部结果是否产生不可判定。租约过期后只生成
   `COORDINATION_LEASE_EXPIRED` 惰性结果，不再次调用 Provider。这里选择 at-most-once 的
   协调调用语义；Provider adapter 内部既有的有界 HTTP 重试仍属于同一次协调调用。
7. 进程内内容 cache 和并发信号量不迁移到数据库。它们仅影响性能；跨 worker 安全边界由
   request-id 去重和全局每日配额提供。不同 request id 的相同内容不承诺全局缓存命中。
8. 自动 supervisor 默认关闭，必须显式 opt-in。关闭、故障或积压只减少 LLM 解释能力；
   LLM 始终没有 Broker、风控、撤单、减仓或退出权限。

## 后果

- 重启、并发 worker 和 outbox 重投不会对同一固定 request id 再次调用 Provider。
- 对未知外部结果选择惰性失败会损失一次解释，但避免重复费用和不一致结论。
- 日历、事实缺失和 Broker 对账问题可通过 automation run 原因码审计，不会静默跳过。
- `0007` 迁移是启用自动编排和多 worker LLM 调用的前置条件。
- Phase 5 仍需完整 RTH soak、长期模型漂移/成本趋势和真实浏览器视觉回归；本 ADR 不放行
  paper 或 live 交易。
