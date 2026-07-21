# ADR 0001：Phase 2 实时传输（gRPC 快照流 + WebSocket 推送）

- 状态：已接受
- 日期：2026-07-21
- 关联：CLAUDE.md D4、PROJECT_PLAN.md 第 9 节（Phase 2）、DEVELOPMENT_PLAN.md 8.1/8.2

## 背景

Phase 0/1 的 Rust→Python→React 链路是 HTTP 轮询 + fixture 快照。Phase 2 要
交付「实时驾驶舱」：实时行情流、健康监控与断线重连、Live Cockpit、增量推送与
快照恢复。CLAUDE.md 第 3/4 节规定 Python↔Rust 用 gRPC（实时用 server
streaming），React↔Python 用 WebSocket；进入交易许可链的 MarketSnapshot 与
DataHealth 权威唯一在 Rust Market Core。

数据源方面，代码支持 Theta Terminal v3 的股票 TRADE WebSocket 与当日 OHLC REST
回补；真实 Standard entitlement、字段样本和完整交易日 soak 仍需在运行 Terminal 的
现场环境验收。

## 决策

1. **快照流权威在 Rust**。gRPC `MarketService`（`market.proto`）：
   `StreamMarketSnapshots` server streaming + `GetDataHealth`。Python 只消费，
   产出 RegimeState/VolState/Signal，不重算底层特征。

2. **流里同时带原始每分钟 bar 与传输阶段**：`StreamMarketSnapshots` 返回
   `stream MarketTick{snapshot, bar, delivery_phase, high_watermark_sequence}`。
   聚合快照（session 高低/VWAP/opening
   range）无法还原逐分钟 OHLC，若在 Python 侧近似重建会污染 Regime/Vol 信号，
   违反「先避免错误交易」。带上原始 bar 使实时引擎输出与离线回放逐位一致。
   `delivery_phase` 区分 `BACKFILL` 与 `LIVE`：历史 snapshot 即使当时 HEALTHY，
   在追平 producer high-watermark 前也只能重建状态，禁止新开仓。

3. **DataHealth 状态机在 Rust**（`market-core/health.rs`）：依据记录到达节奏
   （间隔/乱序/断流/重连）驱动 HEALTHY→DEGRADED→STALE→DISCONNECTED→
   RECONCILING；首记录前为 RECONCILING，永不默认 HEALTHY，fail closed。

4. **可插拔数据源**：`ReplaySnapshotSource` 读标准化 NDJSON bar（复用
   features.rs）确定性回放；`OPTIONTRADER_MARKET_SOURCE=theta` 启用唯一 Theta
   WebSocket 生产者。实时源订阅 QQQ STOCK/TRADE，严格校验 symbol、状态、sequence、
   价格和 RTH 时间，再聚合一分钟 OHLCV/VWAP。

5. **盘中启动/重连先回补再放行**：建立 WebSocket 并发送订阅后，请求 Theta v3
   `/stock/history/ohlc` 的 Nasdaq Basic 当日 09:30 至上一完整分钟 bars；当前分钟消息
   在 socket 中缓冲。回补必须从 09:30 覆盖到目标分钟，并与已发布前缀完全一致；失败、
   缺口或冲突均保持 RECONCILING/STALE。回补 records 以 BACKFILL 交付，追平后的新
   WebSocket bar 才可能为 LIVE。

6. **gRPC 与 HTTP 同进程并存**：trading-core-bin 内 axum :8080（Phase 0 REST
   不破坏）+ tonic :50051，tokio::select 并发。

7. **Python 双角色**：gRPC 客户端消费流→引擎→CockpitState；FastAPI
   `WS /api/v1/stream/cockpit`（增量推送）+ `GET /api/v1/cockpit/state`
   （重连快照恢复）。

8. **codegen 不入仓**：Rust 经 crates/proto 的 build.rs（tonic-build）编译期
   生成；Python 经 scripts/gen_python_grpc.sh 生成到 app/grpc_gen/（git 忽略、
   排除出 mypy/ruff gate）。生成物不提交，避免与 proto 漂移。

9. **双维 fail-closed 交易许可**：React 端 cockpitCanTrade 要求数据维（帧 LIVE
   + new_position_allowed + snapshot HEALTHY）AND broker 维（/core/health 的
   canOpenNewPosition）。断流时清空 frame，重连窗口内不放行陈旧 LIVE 帧。

10. **断线回补与应用重启恢复**：`StreamRequest.resume_after_sequence` 请求 Rust
   session buffer 回放缺失记录。Rust 对落后订阅者标记 `BACKFILL`；Python 仍按序
   追加 bar 并重建引擎，但强制 CockpitState 为 STALE/No Trade。只有追平后新产生
   的 `LIVE` 记录，且 DataHealth=HEALTHY，才可恢复新开仓许可。Projector 另以
   MarketSnapshot.sequence_number 连续性守卫防御 gap/reorder/duplicate，并独立校验
   high-watermark 合法、单调且已经越过恢复目标；目标记录本身仍禁止新开仓，避免
   单独信任上游 `LIVE` 标签。

11. **跨语言 smoke 是根门禁的一部分**：`make test-integration` 先构建当前
    `trading-core` 二进制，再以 `OPTIONTRADER_REQUIRE_INTEGRATION=1` 执行真实
    Rust→gRPC→Python smoke；`make test` 必须包含该目标，不允许因二进制缺失而跳过。

12. **watermark 回退保持粘滞闭锁**：同一 `session_id` 下 Trading Core 重启会令
    内存 cursor 回退，既有 Projector 不自动接受新的低 watermark。当前受支持的恢复
    方式是确认 Core 健康后重启 Application API；具体步骤见 `docs/RUNBOOK.md`。未来
    以显式 `session_epoch` 区分合法的新数据世代。

## 影响

- 新增依赖栈：Rust tonic/prost/tokio-stream/tokio-tungstenite/reqwest；Python
  grpcio/grpcio-tools/protobuf。
- proto 契约包含 MarketTick/MarketBar、DeliveryPhase、resume cursor 与
  high-watermark；proto 与 jsonschema 各守其边界。
- 四类事件文件通过严格 JSON Schema/Pydantic 导入，生成 EventContext 并注入
  Strategy/Cockpit；缺失、陈旧、未来时间或低置信度输入禁开新仓。

## 备选与否决

- Python 直接读回放快照产流：否决——违反「快照权威在 Rust」。
- 只流聚合快照、Python 重建 bar：否决——污染信号。
- 生成代码入仓：否决——易与 proto 漂移，改为构建时/脚本生成。
