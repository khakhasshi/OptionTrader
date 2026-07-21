# External Data Interface Plan

> 系统：QQQ 日内期权波动率交易驾驶舱
> 版本：v0.3
> 当前已具备：ThetaData Options Standard、ThetaData VIX 数据、Longbridge、IBKR

## 0. 文档一致性基线

本文档只定义外部数据和 Broker 接口，系统职责与其他文档保持一致：

```text
ThetaData = Market Data Truth
官方事件源 + Longbridge 内容数据 = Event Context
当前 CandidateTradePlan 选定 Broker 的账户、订单、成交和持仓回报 = Execution Truth
Rust Market Core = ThetaData 接入、标准化、底层增量特征和 DataHealth
Python Application Service = 事件清洗、Regime/Vol/Strategy/Review/LLM 编排
Rust Risk & Execution Gateway = Longbridge/IBKR 适配、BrokerHealth、最终硬风控、订单状态机和对账
React Trading Cockpit = 展示和人工确认，不直连任何外部交易接口
PostgreSQL = 生产系统唯一主数据库，保存标准化事件、交易状态和审计
Parquet = 原始行情和回放文件；DuckDB 仅用于本地只读研究
```

Longbridge 和 IBKR 均可作为 Broker Adapter；每个 `CandidateTradePlan` 必须指定唯一 `broker_id`，一次计划不得向两个 Broker 重复提交。

## 1. 当前资源定位

### ThetaData

定位：Market Data Truth

负责：

- QQQ 实时股票行情
- QQQ 历史 K 线和 tick 数据
- QQQ 期权链
- QQQ 期权 NBBO quote/trade
- QQQ 期权 IV
- QQQ 期权一阶 Greeks
- QQQ 期权 OI
- VIX 行情和历史数据

### Longbridge

定位：Event Context 主内容源之一 + 可选 Broker Adapter

负责：

- 股票新闻、公告、SEC filing、社区内容和基本面上下文
- 被选为当前 broker_id 时，提供账户资产、持仓、订单状态和成交记录
- 可用于行情交叉校验，但不能覆盖 ThetaData 的 Market Data Truth

### IBKR

定位：可选 Broker Adapter

负责：

- 账户权益
- buying power
- option buying power
- 实盘下单
- 撤单/改单
- 多腿期权单
- 真实成交价格
- commission / fee
- margin / risk 状态

### Execution Truth 规则

Execution Truth 不是固定等同于某一家 Broker，而是当前 `CandidateTradePlan.broker_id` 所选 Broker 的回报：

```text
broker_id = longbridge → Longbridge account/order/fill/position 为该计划的 Execution Truth
broker_id = ibkr       → IBKR account/order/fill/position 为该计划的 Execution Truth
```

Rust Risk & Execution Gateway 必须在启动、重连、下单前和成交后与所选 Broker 对账。状态未知或对账不一致时，`BrokerHealth` 进入 `RECONCILING`，禁止新开仓。

## 2. 当前缺口

你当前缺的不是核心交易数据，而是 Event Context Layer。

缺口分两类：

```text
1. 宏观事件日历
2. QQQ 成分股财报 / 新闻 / 重大事件
```

这两类数据不直接产生开仓信号，但会决定：

- 是否进入 Event Day
- 是否开盘前禁交易
- 事件前后是否缩小仓位
- 是否允许 short premium
- LLM 盘前解释是否完整
- 当日异常波动是否有上下文

## 3. 必须补充的外部接口

### 3.1 宏观事件日历接口

优先级：P0

最低要求：

- 事件名称
- 发布时间
- 时区
- 国家
- 重要性
- 前值
- 预期值
- 实际值
- 修正值
- 数据来源

必须覆盖：

```text
CPI
Core CPI
PPI
Core PPI
PCE
Core PCE
Nonfarm Payrolls
Unemployment Rate
Average Hourly Earnings
Initial Jobless Claims
ISM Manufacturing
ISM Services
Retail Sales
GDP
Consumer Confidence
University of Michigan Sentiment
FOMC Rate Decision
FOMC Statement
Fed Chair Press Conference
FOMC Minutes
Fed Speakers
Treasury Auctions
Treasury Refunding
```

推荐实现路径：

```text
Event Sync Stage A（总体开发 Phase 2）:
- 使用官方来源和手动配置维护高影响事件
- 每周生成 macro_events.json
- 只覆盖 P0 事件

Event Sync Stage B:
- 接入经济日历 API
- 自动拉取事件、预期、实际和修正值
- 自动生成 Event Day 标签

Event Sync Stage C:
- 把宏观事件实际值和 QQQ/VIX/straddle 反应关联起来
- 形成事件回放数据库
```

建议字段：

```json
{
  "event_id": "cpi_2026_08",
  "event_name": "CPI",
  "category": "Inflation",
  "country": "US",
  "scheduled_at_utc": "2026-08-12T12:30:00Z",
  "scheduled_time_et": "2026-08-12T08:30:00-04:00",
  "importance": "P0",
  "previous": null,
  "consensus": null,
  "actual": null,
  "revised": null,
  "source": "BLS/FRED/Fed/CalendarAPI",
  "source_timestamp_utc": null,
  "received_at_utc": "2026-08-10T02:00:00Z",
  "schema_version": "1.0",
  "trade_rule": {
    "block_new_trades_before_minutes": 5,
    "block_new_trades_after_minutes": 5,
    "reduce_size_factor": 0.5,
    "forbid_short_0dte_premium": true
  }
}
```

### 3.2 QQQ 成分股权重接口

优先级：P0

最低要求：

- ticker
- company name
- weight
- sector
- last updated date

用途：

- 确定 QQQ top weighted names
- 只跟踪前 10-20 个高权重成分股
- 按权重计算事件风险
- LLM 盘前解释 QQQ gap 来源

建议字段：

```json
{
  "as_of": "2026-07-20",
  "fund": "QQQ",
  "holdings": [
    {
      "ticker": "NVDA",
      "name": "NVIDIA Corporation",
      "weight": 0.0816,
      "sector": "Information Technology"
    }
  ]
}
```

### 3.3 QQQ 成分股财报日历接口

优先级：P0

最低要求：

- ticker
- company name
- earnings date
- release timing：BMO / AMC / TAS / unknown
- estimated EPS
- estimated revenue
- actual EPS
- actual revenue
- confirmation status
- source

跟踪范围：

```text
QQQ top 20 holdings
```

事件评分：

```text
Event Impact Score = QQQ Weight * Event Importance * Timing Factor
```

规则：

```text
若 QQQ top 5 成分股盘前或盘后财报，交易日标签至少为 Elevated Event Risk。
若 QQQ top 10 中多个成分股同日财报，禁止开盘前制定 aggressive short premium 主剧本。
若前一晚 QQQ top 5 财报引发盘前 gap > 0.7%，开盘前禁止裸卖 0DTE。
```

建议字段：

```json
{
  "ticker": "NVDA",
  "earnings_date": "2026-08-26",
  "release_timing": "AMC",
  "qqq_weight": 0.0816,
  "estimated_eps": null,
  "estimated_revenue": null,
  "confirmed": true,
  "event_impact_score": 0.82,
  "source": "earnings_calendar_api"
}
```

### 3.4 QQQ 成分股新闻 / 8-K / 重大公告接口

优先级：P1

最低要求：

- ticker
- timestamp
- headline
- summary
- source
- category
- urgency
- url

优先类别：

```text
Earnings release
Guidance
8-K
Management change
Regulatory action
Antitrust
AI / semiconductor supply chain
Cloud capex
Product delay
M&A
Major litigation
Analyst downgrade/upgrade for top-weight names
```

数据源建议分层：

```text
Layer 1:
- Longbridge news / filing / topic
- SEC EDGAR submissions

Layer 2:
- Earnings calendar API
- Company press release / investor relations RSS

Layer 3:
- Paid news API if latency requirement提高
```

## 4. 可选但推荐接口

### 4.1 Fed Speaker Calendar

优先级：P1

用途：

- Powell 或投票委员讲话前降低 short premium 风险
- 事件前后标记 elevated rates volatility

### 4.2 Treasury Auction Calendar

优先级：P1

用途：

- 10Y / 30Y auction 前后识别利率冲击风险
- 避免在 auction 前盲目 short vol

### 4.3 QQQ 成分股盘前异动接口

优先级：P1

用途：

- 识别 QQQ gap 的实际来源
- 判断 gap 是 broad-based 还是 single-name driven

可由 ThetaData 股票数据 + QQQ holdings 计算：

```text
Weighted Premarket Contribution = Holding Weight * Premarket Return
```

### 4.4 VIX Term Structure

优先级：P2

用途：

- 判断 VIX 风险温度
- 如果只有 VIX spot，也能做第一版
- 若后续有 VX futures，可增强风控

## 5. 外部接口完整清单

### P0：上线 MVP 必须

```text
ThetaData:
- QQQ stock snapshot quote/trade/OHLC
- QQQ stock history OHLC/trade/quote
- QQQ option expirations/strikes/contracts
- QQQ option snapshot quote/trade/OHLC/open_interest
- QQQ option snapshot IV
- QQQ option snapshot first_order_greeks
- QQQ option history quote/trade/OHLC/IV/first_order_greeks
- QQQ option quote/trade stream
- VIX price/history
- market calendar

ThetaData Standard 能提供 snapshot quote 和 first-order Delta/Theta/Vega/IV，但二阶/all-Greeks
端点需要 Professional。系统不得从 Broker 补 Gamma；当前使用同步的 ThetaData Delta、IV、
underlying price、underlying timestamp 与到期时间确定性推导 Gamma。option/underlying 时间差
超过 5 秒、quote 陈旧或字段缺失时整批拒绝。升级 Professional 后仍需保留该边界的对拍测试，
不得静默改变交易口径。

Broker:
- Longbridge adapter：account/assets/positions/orders/fills
- IBKR adapter：account/positions/orders/fills/margin
- CandidateTradePlan.broker_id：每个计划唯一选择 longbridge 或 ibkr
- selected broker reconciliation：启动、重连、下单前和成交后必须完成

Event:
- US macro calendar
- FOMC calendar
- QQQ holdings
- QQQ top 20 earnings calendar
```

### P1：实盘质量提升

```text
Event:
- Fed speakers
- Treasury auctions
- QQQ top 20 news
- QQQ top 20 SEC 8-K monitor
- Company IR press release RSS

Market:
- QQQ holdings weighted premarket move
- VIX intraday regime
- SPX / NDX / XLK confirmation if available
```

### P2：进阶优化

```text
Market:
- VIX futures term structure
- VVIX
- rates futures
- sector ETF breadth

Research:
- earnings transcript
- analyst estimate revision
- options unusual flow outside QQQ
```

## 6. 推荐最小实现

你已经有 ThetaData、VIX、Longbridge 和 IBKR，所以第一版只需要补四类 Event Context 输入契约：

```text
1. macro_events.json
2. qqq_holdings.json
3. qqq_top20_earnings.json
4. qqq_top20_news_events.json
```

第一版可以先不买额外 API，采用混合方式：

```text
宏观事件：
- 官方来源 + 每周手动/脚本更新

QQQ holdings：
- Invesco 官方持仓页面或文件

成分股财报：
- 财报日历 API 或手动维护 top 20

新闻/公告：
- Longbridge news/filing 优先
- SEC EDGAR 8-K 监听作为补充
```

上述 JSON 是人工维护、脚本导入和测试 fixture 的交换格式，不是生产数据库。导入后必须标准化写入 PostgreSQL `events` schema，并保留 source、原始引用和导入批次；策略、风控和 LLM 从 PostgreSQL 读取已标准化的 `EventContext`。

## 7. 接入所有权与故障策略

| 外部接口 | 代码所有权 | 标准化输出 | 故障策略 |
|---|---|---|---|
| ThetaData QQQ/Options/VIX | Python Research Job（历史下载）+ Rust Market Core（生产标准化/实时流） | `MarketEvent`、`MarketSnapshot`、`OptionSnapshot`、`DataHealth` | 断流、陈旧或乱序未恢复时禁止新开仓 |
| 官方宏观/持仓/财报源 | Python Event Context | `EventContext` | P0 日历缺失时提高风险或禁用相关策略 |
| Longbridge news/filing | Python Event Context | `NewsEvent`、`FilingEvent` | 仅影响上下文，不生成方向性硬信号 |
| Longbridge Broker Adapter | Rust Risk & Execution Gateway | `BrokerSnapshot`、`BrokerHealth`、`OrderEvent`、`FillEvent`、`PositionSnapshot` | 仅在 broker_id=longbridge 时为 Execution Truth |
| IBKR Broker Adapter | Rust Risk & Execution Gateway | `BrokerSnapshot`、`BrokerHealth`、`OrderEvent`、`FillEvent`、`PositionSnapshot` | 仅在 broker_id=ibkr 时为 Execution Truth |

所有外部数据必须保留：

```text
source
source_timestamp_utc
received_at_utc
schema_version
source_sequence 或可替代去重键
raw_ref
```

行情健康状态统一为：

```text
HEALTHY / DEGRADED / STALE / DISCONNECTED / RECONCILING
```

Broker 健康状态统一为：

```text
HEALTHY / DEGRADED / DISCONNECTED / RECONCILING
```

只有 `DataHealth = HEALTHY`、`BrokerHealth = HEALTHY` 且所选 Broker 已完成对账时允许新开仓。React、Python Strategy 和 LLM 都不能把非健康状态覆盖为可交易。

### 7.1 PostgreSQL 持久化边界

```text
PostgreSQL：
- 标准化 EventContext、来源元数据、导入批次和去重键
- Market/Option 聚合快照及 Parquet 数据集索引
- Signal、CandidateTradePlan、RiskDecision、OrderEvent、FillEvent、PositionSnapshot
- rule_version、LLMReview、DailyReview 和 audit_events

Parquet：
- ThetaData 原始/标准化高频 quote、trade、tick 和回放事件流

JSON：
- 手工维护、批量导入、API 交换和测试 fixture
- 导入成功后不作为生产查询事实源
```

## 8. 免费数据源方案

第一版 MVP 可以优先使用免费官方源，不必马上购买额外事件数据 API。免费方案的核心思路是：官方源负责事件日期和公告事实，Longbridge 负责已有新闻/公告补充，系统内部将它们统一清洗成 Event Context Layer。

### 8.1 宏观事件免费源

推荐来源：

```text
FRED API:
- economic release dates
- release metadata
- historical economic series

BLS official schedule:
- CPI
- PPI
- Employment Situation
- Jobless Claims related releases if available

Federal Reserve official pages:
- FOMC meeting calendar
- FOMC statement dates
- FOMC minutes dates
- Chair press conference
- Fed speeches and testimony calendar

New York Fed calendar:
- economic indicators calendar
```

第一版处理方式：

```text
每周同步未来 30-60 天 P0/P1 事件。
把所有事件统一转换为 macro_events.json。
没有 consensus 时允许为空，但必须保留 scheduled_at_utc；scheduled_time_et 作为决策和展示派生字段。
```

免费源限制：

```text
1. 通常能稳定获得日期和时间，但不一定提供 consensus。
2. 不同官方页面格式不统一，需要适配器。
3. 修正值和实际值可能需要事件发布后再补。
4. 不适合作为毫秒级新闻交易源，但足够用于 SOP 风控。
```

### 8.2 QQQ 持仓免费源

推荐来源：

```text
Invesco QQQ official holdings / product page
```

第一版处理方式：

```text
每日或每周同步一次。
只取 top 20 holdings。
生成 qqq_holdings.json。
```

用途：

```text
计算 QQQ 成分股事件权重。
确定需要跟踪财报和公告的股票池。
解释 QQQ 盘前 gap 是否由高权重成分股驱动。
```

### 8.3 财报日历免费源

可选来源：

```text
免费额度 earnings calendar API
Nasdaq earnings calendar 页面
Yahoo Finance earnings calendar
手动维护 top 20 earnings calendar
```

第一版建议：

```text
先手动或半自动维护 QQQ top 20 财报日历。
只要求 date、ticker、release_timing、confirmed。
不要在第一版依赖不稳定网页结构做关键风控。
```

免费源限制：

```text
1. 财报日期会变更。
2. BMO/AMC/TAS 字段可能不稳定。
3. 免费 API 可能有额度、延迟或字段缺失。
4. SEC EDGAR 只能确认已发布文件，不能可靠预测未来财报日期。
```

### 8.4 新闻和公告免费源

推荐来源：

```text
Longbridge news / filing
SEC EDGAR submissions API
Company investor relations RSS / press releases
```

第一版处理方式：

```text
Longbridge 作为主新闻/公告源。
SEC EDGAR 监听 QQQ top 20 的 8-K、10-Q、10-K。
只将 high-impact 事件写入 qqq_top20_news_events.json。
```

重点监控类型：

```text
8-K
Earnings release
Guidance
Investor presentation
Regulatory action
Major litigation
M&A
Management change
```

## 9. 开工判断

当前资源状态：

```text
已具备：
- ThetaData Options Standard
- ThetaData VIX 数据
- Longbridge
- IBKR

待补齐：
- 免费宏观事件同步
- QQQ holdings 同步
- QQQ top 20 earnings calendar
- QQQ top 20 news / SEC event monitor
```

结论：

```text
可以开工。
```

原因：

```text
1. 核心交易数据已经具备。
2. 核心执行和账户风控接口已经具备。
3. 缺失的数据属于事件上下文，不阻塞 Phase 0 工程基础和 Phase 1 历史回放；实时 Dashboard 属于 Phase 2。
4. 事件上下文可以用免费源和手动维护 top 20 的方式先补齐。
```

建议开工方式：

```text
先做 Phase 0：工程与契约；随后做 Phase 1：ThetaData 历史接入和离线回放。
Event Context 实际接入和 React 实时驾驶舱在 Phase 2 完成。
不要第一步就做自动下单。
```

## 10. Phase 0-2 数据接入任务

### Task 1：项目骨架（Phase 0）

```text
创建 React、Python、Rust 服务结构。
创建配置目录。
创建 data/ 目录保存事件 JSON 和本地缓存。
创建 logs/ 目录保存服务运行日志；权威信号、交易和审计记录写入 PostgreSQL。
创建 PostgreSQL schema、Alembic 单一迁移链、SQLx 离线元数据和本地 compose 服务。
```

### Task 2：ThetaData Adapter（Phase 1 历史 Python SDK；生产标准化与 Phase 2 实时 Rust）

```text
接入 QQQ stock quote / OHLC。
接入 QQQ option expirations / strikes / contracts。
接入 QQQ option quote / IV / first-order Greeks。
接入 VIX price / history。
实现本地缓存和错误重试。
输出 DataHealth、quote age、source sequence 和可回放 Parquet。
```

### Task 3：Event Context Layer（Phase 2，Python）

```text
定义 macro_events.json schema。
定义 qqq_holdings.json schema。
定义 qqq_top20_earnings.json schema。
定义 qqq_top20_news_events.json schema。
实现 event_day_type 和 risk_flags 生成逻辑。
实现导入批次、去重、来源追踪和 PostgreSQL events schema 落库。
```

### Task 4：Core Engines（Phase 1-3）

```text
Phase 1 Python：实现 Vol、Regime、Strategy 和只读风险预检查。
Phase 1 Rust：实现底层增量特征和 DataHealth，不重复实现 Strategy 规则。
Phase 3 Rust：实现权威 Initial/Final Risk Check、订单状态机和 Broker 对账。
实现 Strategy Engine 的 No Trade / Long Gamma / Short Premium 初版。
```

### Task 5：Review Log（Phase 1 起，Python）

```text
记录所有信号。
记录未交易原因。
记录 LLM 审核结果。
写入 PostgreSQL review/audit schema，并可按需导出 daily_review.json。
```

### Task 6：React Dashboard（Phase 2）

```text
展示 QQQ price、VWAP、opening range。
展示 ATM straddle、IV/HV、VIX。
展示 Event Context。
展示当前剧本和 Risk Flags。
展示候选交易，但不下单。
```

## 11. Event Context Layer 输出

统一输出给 Python Strategy Engine、Rust Risk & Execution Gateway 和 LLM：

该对象以 PostgreSQL `events.event_contexts` 中的版本化记录为生产事实源；JSON 仅用于展示字段和交换格式。

```json
{
  "schema_version": "1.0",
  "event_context_id": "event_ctx_20260720_v1",
  "trading_date": "2026-07-20",
  "generated_at_utc": "2026-07-20T12:00:00Z",
  "event_day_type": "Normal | MacroEvent | EarningsEvent | FOMC | Mixed | HighRisk",
  "macro_events": [],
  "earnings_events": [],
  "news_events": [],
  "qqq_weighted_event_score": 0.0,
  "risk_flags": [
    "NO_SHORT_PREMIUM_BEFORE_EVENT",
    "SIZE_HALF",
    "WAIT_AFTER_RELEASE"
  ],
  "deterministic_context_summary": "今日无 P0 宏观事件，QQQ top 20 无盘前财报。"
}
```
