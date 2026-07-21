# OptionTrader 运维手册

本文记录当前阶段已知的 fail-closed 行为与受支持的恢复步骤。任何恢复操作都不得
绕过 DataHealth、sequence continuity、delivery phase 或 high-watermark 闸门。

## 同 session 下 Trading Core 重启

### 现象

Rust Trading Core 重启后，其内存 replay cursor 会从 1 重新开始。如果 Python
Application API 仍保留同一个 `session_id` 的 `CockpitProjector`，新的
`high_watermark_sequence` 会低于 Projector 已观察到的 watermark。Projector 会按
设计返回 `DISCONNECTED` / No Trade，并在 `risk_flags` 中报告
`invalid transport sequence metadata`。

这是粘滞式 fail-closed 行为：系统不会把无法证明属于同一数据世代的低序列记录当成
实时行情，也不会自动清空 watermark 后继续交易。

### 当前恢复步骤

1. 保持新开仓禁用，确认没有把旧 Cockpit LIVE 帧作为交易依据。
2. 确认 Trading Core 已重新启动，HTTP/gRPC 健康检查可达，数据源身份与预期一致。
3. 重启 Application API，使 `SessionHub` 和 `CockpitProjector` 从空状态重建。当前
   Phase 2 骨架不支持在线重置单个 Projector。
4. 重新连接驾驶舱。历史恢复帧必须显示 STALE/No Trade；只有回补完成后的新 LIVE
   帧，且 DataHealth=HEALTHY，才可能恢复新开仓许可。
5. 若仍出现 watermark 回退、sequence discontinuity 或 DISCONNECTED，维持 No Trade，
   不得通过修改内存字段、伪造 session id 或跳过回补强制解锁。

### 后续演进

真实实时源接入前应在传输契约中引入显式 `session_epoch` 或等价的数据世代标识。届时
只有经过身份校验的新 epoch 才能重置 sequence/watermark；不能仅凭数值回退推断重启。

## ThetaData 实时源

1. 启动 Theta Terminal v3，确认本机 `25503`（REST）和 `25520`（WebSocket）可达。
2. 配置 `THETADATA_BASE_URL=http://127.0.0.1:25503/v3`、
   `THETADATA_WS_URL=ws://127.0.0.1:25520/v1/events`，运行 `make dev-core-theta`。
3. 盘中启动或重连时，必须先看到 REST 回补从 09:30 覆盖至上一完整分钟；回补期间
   Cockpit 显示 STALE/No Trade。缺口、HTTP 错误、前缀冲突或 entitlement 错误不得
   手工改健康状态解锁。
4. `GetDataHealth`、`/health` 与 Cockpit 必须一致；市场流持续 90 秒无 tick 时 Python
   发布 DISCONNECTED 并重连，Rust 也按自身阈值将健康降级。

当前仓库以官方消息形状和本地 mock Terminal 覆盖自动测试。真实账号 entitlement、
一份脱敏原始字段样本以及完整 RTH 连续运行需要现场执行，未执行前不得作为 paper/live
上线证据。

## 事件上下文日常导入

盘前将四类文件放入 `data/events/YYYY-MM-DD/`，运行 `make events-context`。退出码 2、
`available=false` 或任一来源检查失败时保持 No Trade。需要审计落库时使用 CLI 的
`--persist`；它会在同一事务写入 `events.event_contexts` 与 `audit.audit_events`。
