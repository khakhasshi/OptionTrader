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

1. 准备官方 Python SDK 凭证。推荐设置
   `THETADATA_CREDENTIALS_FILE=/absolute/path/to/creds.txt` 并执行 `chmod 600`；也可设置
   `THETADATA_API_KEY` 或 `THETADATA_DOTENV_PATH`。不得复制凭证到仓库或命令输出。
2. 在独立终端运行 `make dev-thetadata-sdk`，确认 `127.0.0.1:50052` 启动；SDK 直接连接
   ThetaData 服务器，不需要启动 Theta Terminal。
3. 在第二个终端运行 `make dev-core-theta-sdk`。Rust 通过
   `THETADATA_SDK_GRPC=http://127.0.0.1:50052` 消费已完成分钟 bar。
4. 盘中启动或重连时，必须先看到从 09:30 开始的连续回补；回补期间 Cockpit 显示
   STALE/No Trade。SDK 请求错误、空/部分占位、分钟缺口、时间冲突、前缀冲突或
   entitlement 错误不得手工改健康状态解锁。
5. 可用以下 opt-in smoke 验证凭证和 Standard 股票 OHLC 权限：
   `THETADATA_CREDENTIALS_FILE=/absolute/path/to/creds.txt uv run pytest
   tests/test_thetadata_sdk_live.py -q`（在 `services/application-api` 下运行）。
6. `GetDataHealth`、`/health` 与 Cockpit 必须一致；市场流持续 90 秒无 tick 时 Python
   发布 DISCONNECTED 并重连，Rust 也按自身阈值将健康降级。

当前仓库已用真实账号完成 QQQ 三分钟历史 OHLC 凭证/字段 smoke，并以 mock SDK gRPC
覆盖回补和增量传输。完整 RTH 连续运行仍需现场执行，未执行前不得作为 paper/live
上线证据。

## 事件上下文日常导入

盘前将四类文件放入 `data/events/YYYY-MM-DD/`，运行 `make events-context`。退出码 2、
`available=false` 或任一来源检查失败时保持 No Trade。需要审计落库时使用 CLI 的
`--persist`；它会在同一事务写入 `events.event_contexts` 与 `audit.audit_events`。
