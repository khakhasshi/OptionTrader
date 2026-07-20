"""SessionHub: one projector/seq per session, fan-out, monotonic across reconnects."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest

from app.realtime import session
from app.realtime.projector import ProjectorConfig
from app.realtime.session import SessionHub, get_hub, reset_hubs


@pytest.fixture(autouse=True)
def _clean_hubs() -> Any:
    reset_hubs()
    yield
    reset_hubs()


def _snapshot(minute_et: int, seq: int, health: str = "HEALTHY") -> dict[str, Any]:
    hh, mm = minute_et // 60, minute_et % 60
    uhh = hh + 4
    close = 500.0 + seq * 0.1
    ts_utc = f"2026-07-20T{uhh:02d}:{mm:02d}:00Z"
    ts_et = f"2026-07-20T{hh:02d}:{mm:02d}:00-04:00"
    return {
        "schema_version": "1.0",
        "snapshot_id": f"mkt_{minute_et}_{seq:06d}",
        "occurred_at_utc": ts_utc,
        "timestamp_et": ts_et,
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
    }


def _bar(minute_et: int, seq: int) -> dict[str, Any]:
    hh, mm = minute_et // 60, minute_et % 60
    uhh = hh + 4
    close = 500.0 + seq * 0.1
    return {
        "occurred_at_utc": f"2026-07-20T{uhh:02d}:{mm:02d}:00Z",
        "timestamp_et": f"2026-07-20T{hh:02d}:{mm:02d}:00-04:00",
        "minute_et": minute_et,
        "open": f"{close:.2f}",
        "high": f"{close + 0.5:.2f}",
        "low": f"{close - 0.5:.2f}",
        "close": f"{close:.2f}",
        "volume": 1000 + seq,
        "vwap": f"{close:.2f}",
    }


def _tick(minute_et: int, seq: int, health: str = "HEALTHY") -> dict[str, Any]:
    return {"snapshot": _snapshot(minute_et, seq, health), "bar": _bar(minute_et, seq)}


class _Controllable:
    """An async tick source a test can drive one event at a time, so a client
    can disconnect and reconnect mid-stream deterministically."""

    def __init__(self) -> None:
        self.queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()

    def push(self, tick: dict[str, Any]) -> None:
        self.queue.put_nowait(("tick", tick))

    def end(self, reason: str = "stream ended") -> None:
        self.queue.put_nowait(("end", reason))

    async def __call__(self, _session_id: str) -> AsyncIterator[tuple[str, Any]]:
        while True:
            kind, payload = await self.queue.get()
            yield (kind, payload)
            if kind == "end":
                return


def _config(sid: str = "s1") -> ProjectorConfig:
    return ProjectorConfig(session_id=sid, rule_version="t", opening_range_minutes=3)


def test_single_subscriber_gets_frames_then_terminal_disconnect() -> None:
    async def run() -> list[dict[str, Any]]:
        src = _Controllable()
        hub = get_hub(_config(), source=src)
        frames: list[dict[str, Any]] = []

        async def consume() -> None:
            async for f in hub.subscribe():
                frames.append(f)

        task = asyncio.create_task(consume())
        await asyncio.sleep(0)
        for i in range(4):
            src.push(_tick(570 + i, i + 1))
        await asyncio.sleep(0.05)
        src.end()
        await asyncio.wait_for(task, timeout=2)
        return frames

    frames = asyncio.run(run())
    assert [f["connection"] for f in frames[:4]] == ["LIVE"] * 4
    assert frames[-1]["connection"] == "DISCONNECTED"
    # seq is monotonic
    seqs = [f["seq"] for f in frames]
    assert seqs == sorted(seqs)


def test_seq_is_monotonic_across_reconnect() -> None:
    """The core blocker-2 guarantee: a client that drops and reconnects sees a
    first LIVE frame with seq strictly greater than its last pre-drop seq."""

    async def run() -> tuple[int, int]:
        src = _Controllable()
        hub = get_hub(_config("recon"), source=src)

        # First connection: consume 3 frames, then disconnect.
        first_seqs: list[int] = []
        gen1 = hub.subscribe()
        for i in range(3):
            src.push(_tick(570 + i, i + 1))
        for _ in range(3):
            f = await asyncio.wait_for(gen1.__anext__(), timeout=2)
            first_seqs.append(f["seq"])
        await gen1.aclose()  # client drops

        # Hub keeps consuming upstream while no client is attached.
        for i in range(3, 6):
            src.push(_tick(570 + i, i + 1))
        await asyncio.sleep(0.05)

        # Reconnect (same session -> same hub) and read the next LIVE frame.
        gen2 = hub.subscribe()
        next_live = None
        for _ in range(6):
            f = await asyncio.wait_for(gen2.__anext__(), timeout=2)
            if f["connection"] == "LIVE" and f["seq"] > first_seqs[-1]:
                next_live = f
                break
        await gen2.aclose()
        assert next_live is not None
        return first_seqs[-1], next_live["seq"]

    last_before, first_after = asyncio.run(run())
    assert first_after > last_before


def test_multiple_clients_share_one_projector_no_engine_rerun() -> None:
    """Two clients attached to one hub receive the SAME frames (one projection
    per tick), not independently re-projected streams."""

    async def run() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        src = _Controllable()
        hub = get_hub(_config("multi"), source=src)
        a: list[dict[str, Any]] = []
        b: list[dict[str, Any]] = []

        async def consume(sink: list[dict[str, Any]]) -> None:
            async for f in hub.subscribe():
                sink.append(f)

        ta = asyncio.create_task(consume(a))
        tb = asyncio.create_task(consume(b))
        await asyncio.sleep(0)
        for i in range(3):
            src.push(_tick(570 + i, i + 1))
        await asyncio.sleep(0.05)
        src.end()
        await asyncio.wait_for(asyncio.gather(ta, tb), timeout=2)
        return a, b

    a, b = asyncio.run(run())
    # Both clients saw the same signal_ids for the LIVE frames (same projection).
    a_ids = [f["signal"]["signal_id"] for f in a if f["signal"]]
    b_ids = [f["signal"]["signal_id"] for f in b if f["signal"]]
    assert a_ids == b_ids and len(a_ids) == 3


def test_latest_frame_cached_for_recovery() -> None:
    async def run() -> dict[str, Any] | None:
        src = _Controllable()
        hub = get_hub(_config("cache"), source=src)
        gen = hub.subscribe()
        src.push(_tick(570, 1))
        await asyncio.wait_for(gen.__anext__(), timeout=2)
        await gen.aclose()
        return session.latest_frame("cache")

    cached = asyncio.run(run())
    assert cached is not None
    assert cached["snapshot"]["snapshot_id"] == "mkt_570_000001"


def test_upstream_error_publishes_disconnected_frame() -> None:
    async def bad_source(_sid: str) -> AsyncIterator[tuple[str, Any]]:
        raise RuntimeError("boom")
        yield  # pragma: no cover

    async def run() -> list[dict[str, Any]]:
        hub = SessionHub(_config("err"), source=bad_source)
        frames: list[dict[str, Any]] = []
        async for f in hub.subscribe():
            frames.append(f)
        return frames

    frames = asyncio.run(run())
    assert frames[-1]["connection"] == "DISCONNECTED"
    assert frames[-1]["new_position_allowed"] is False
