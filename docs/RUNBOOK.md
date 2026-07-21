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
