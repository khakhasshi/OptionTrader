# EventContext 文件导入

运行时按纽约交易日期读取：

```text
data/events/YYYY-MM-DD/
  macro_events.json
  qqq_holdings.json
  qqq_top20_earnings.json
  qqq_top20_news_events.json
```

四个文件分别遵循 `packages/contracts/jsonschema/` 下的同名契约。所有事件必须保留
`source`、`source_timestamp_utc`、`received_at_utc`、`confidence` 和 `raw_ref`。
文档本身也必须保留同一组来源字段，因此即使当日事件数组为空，也能证明已查询来源，
不能用一个无来源的空数组声明“今日无事件”。
新闻标题属于不可信文本，只作事件风险输入，不作为方向信号或可执行指令。

推荐免费来源：Fed/BLS 官方日历、Invesco QQQ 官方持仓、发行人 IR/SEC EDGAR；新闻
可由 Longbridge 导出后写入严格契约。财报时间若未由 IR/SEC 明确确认，必须填写
`confirmed=false` 和 `timing=UNKNOWN`，不得猜测 BMO/AMC。

验证某个时间点，并在可用时返回退出码 0：

```bash
cd services/application-api
uv run python -m app.events.cli \
  --event-dir ../../data/events \
  --at-utc 2026-07-20T13:45:00Z
```

加 `--persist` 会使用 `DATABASE_URL` 将 EventContext 与审计事件事务性写入 PostgreSQL。
缺文件、覆盖范围不足、持仓超过 14 天、持仓置信度低于 0.9、来源时间晚于接收时间、
接收时间来自未来或契约错误时命令返回 2，实时系统保持 No Trade。运行时每个 tick
按其 ET 交易日期重新计算事件距离和发布后等待窗口；文件内容仅缓存，不缓存时效判断。
