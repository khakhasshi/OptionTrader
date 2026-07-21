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

数据源方面，TASKS.md 记录 ThetaData 实时 entitlement/字段映射尚未验收，当前为
研究/开发环境。

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

4. **可插拔数据源**（`SnapshotSource` trait）：`ReplaySnapshotSource` 读标准化
   NDJSON bar（复用 features.rs）确定性回放；`LiveThetaSource` 为实时适配器
   占位，entitlement 验收前 fail closed。用 NDJSON 而非让 Rust 读 Parquet，
   避免引入 arrow/parquet 重依赖；真实 Parquet 回放后续经适配器补。

5. **gRPC 与 HTTP 同进程并存**：trading-core-bin 内 axum :8080（Phase 0 REST
   不破坏）+ tonic :50051，tokio::select 并发。

6. **Python 双角色**：gRPC 客户端消费流→引擎→CockpitState；FastAPI
   `WS /api/v1/stream/cockpit`（增量推送）+ `GET /api/v1/cockpit/state`
   （重连快照恢复）。

7. **codegen 不入仓**：Rust 经 crates/proto 的 build.rs（tonic-build）编译期
   生成；Python 经 scripts/gen_python_grpc.sh 生成到 app/grpc_gen/（git 忽略、
   排除出 mypy/ruff gate）。生成物不提交，避免与 proto 漂移。

8. **双维 fail-closed 交易许可**：React 端 cockpitCanTrade 要求数据维（帧 LIVE
   + new_position_allowed + snapshot HEALTHY）AND broker 维（/core/health 的
   canOpenNewPosition）。断流时清空 frame，重连窗口内不放行陈旧 LIVE 帧。

9. **断线回补与应用重启恢复**：`StreamRequest.resume_after_sequence` 请求 Rust
   session buffer 回放缺失记录。Rust 对落后订阅者标记 `BACKFILL`；Python 仍按序
   追加 bar 并重建引擎，但强制 CockpitState 为 STALE/No Trade。只有追平后新产生
   的 `LIVE` 记录，且 DataHealth=HEALTHY，才可恢复新开仓许可。Projector 另以
   MarketSnapshot.sequence_number 连续性守卫防御 gap/reorder/duplicate。

## 影响

- 新增依赖栈：Rust tonic/prost/tokio-stream；Python grpcio/grpcio-tools/
  protobuf。
- proto 契约包含 MarketTick/MarketBar、DeliveryPhase、resume cursor 与
  high-watermark；proto 与 jsonschema 各守其边界。
- 事件上下文导入、真实 ThetaData 实时接入是骨架之后的独立批次。

## 备选与否决

- Python 直接读回放快照产流：否决——违反「快照权威在 Rust」。
- 只流聚合快照、Python 重建 bar：否决——污染信号。
- 生成代码入仓：否决——易与 proto 漂移，改为构建时/脚本生成。
