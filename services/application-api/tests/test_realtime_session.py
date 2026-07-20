"""Session bridge + WebSocket endpoint: deterministic frames via injected ticks."""

from __future__ import annotations

import asyncio
from typing import Any

import grpc
import pytest

from app.realtime import session
from app.realtime.projector import ProjectorConfig
from app.realtime.session import cockpit_frames


def _tick(minute_et: int, seq: int, health: str = "HEALTHY") -> dict[str, Any]:
    hh, mm = minute_et // 60, minute_et % 60
    uhh = hh + 4
    close = 500.0 + seq * 0.1
    return {
        "snapshot": {
            "schema_version": "1.0",
            "snapshot_id": f"mkt_{minute_et}_{seq:06d}",
            "occurred_at_utc": f"2026-07-20T{uhh:02d}:{mm:02d}:00Z",
            "timestamp_et": f"2026-07-20T{hh:02d}:{mm:02d}:00-04:00",
            "symbol": "QQQ.US",
            "price": f"{close:.2f}",
            "open": "498.50",
            "high": f"{close + 1:.2f}",
            "low": "497.90",
            "previous_close": "497.20",
            "vwap": f"{close:.2f}",
            "volume": 1_000_000,
            "premarket_high": None,
            "premarket_low": None,
            "sequence_number": seq,
            "quote_age_ms": 0,
            "data_health": health,
        },
        "bar": {
            "occurred_at_utc": f"2026-07-20T{uhh:02d}:{mm:02d}:00Z",
            "timestamp_et": f"2026-07-20T{hh:02d}:{mm:02d}:00-04:00",
            "minute_et": minute_et,
            "open": f"{close:.2f}",
            "high": f"{close + 0.5:.2f}",
            "low": f"{close - 0.5:.2f}",
            "close": f"{close:.2f}",
            "volume": 1000 + seq,
            "vwap": f"{close:.2f}",
        },
    }


def _drain(config: ProjectorConfig, source: Any) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        return [frame async for frame in cockpit_frames(config, tick_source=source)]

    return asyncio.run(run())


def test_stream_yields_frames_then_terminal_disconnect() -> None:
    config = ProjectorConfig(session_id="s1", rule_version="t", opening_range_minutes=3)
    ticks = [_tick(570 + i, i + 1) for i in range(4)]
    frames = _drain(config, lambda sid: iter(ticks))
    # 4 tick frames + 1 terminal disconnected frame.
    assert len(frames) == 5
    assert all(f["connection"] == "LIVE" for f in frames[:4])
    assert frames[-1]["connection"] == "DISCONNECTED"
    assert frames[-1]["new_position_allowed"] is False
    assert any("stream ended" in flag for flag in frames[-1]["risk_flags"])


def test_transport_error_terminates_with_fail_closed_frame() -> None:
    config = ProjectorConfig(session_id="s2", rule_version="t")

    def boom(_sid: str) -> Any:
        raise grpc.RpcError()
        yield  # pragma: no cover — makes this a generator

    frames = _drain(config, boom)
    assert frames[-1]["connection"] == "DISCONNECTED"
    assert frames[-1]["new_position_allowed"] is False
    assert any("stream error" in flag for flag in frames[-1]["risk_flags"])


def test_latest_frame_is_cached_for_recovery() -> None:
    config = ProjectorConfig(session_id="s3", rule_version="t", opening_range_minutes=3)
    ticks = [_tick(570 + i, i + 1) for i in range(3)]
    _drain(config, lambda sid: iter(ticks))
    cached = session.latest_frame("s3")
    assert cached is not None
    assert cached["connection"] == "DISCONNECTED"  # last frame is terminal
