"""Per-session cockpit hub: one projector + one upstream pump, fanned out to
many WebSocket subscribers.

Phase 2 review fix (blocker 2): previously every WS connection built its own
``CockpitProjector``, so ``seq`` restarted at 0 on each reconnect and the
frontend's monotonic-seq arbitration silently dropped every post-reconnect
frame. Now a session has exactly ONE :class:`SessionHub`:

* one persistent ``CockpitProjector`` — ``seq`` is monotonic for the whole
  session lifetime, across any number of WS connect/disconnect cycles;
* one upstream consumer (the Rust gRPC tick stream) — the Regime/Vol/Strategy
  engines run once per tick per session, never once-per-client;
* fan-out to all currently-attached subscriber queues;
* the latest frame cached for REST recovery (same source as the WS stream).

The hub keeps consuming upstream independently of how many clients are attached,
so a client that drops and reconnects resumes at a strictly higher ``seq``.

Fail closed: upstream end or error publishes a terminal DISCONNECTED frame.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
import threading
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from datetime import datetime, timezone
from typing import Any

import grpc

from app.events import EventContext, EventContextStore, unavailable_event_context
from app.realtime.client import stream_ticks
from app.realtime.projector import CockpitProjector, ProjectorConfig

# An upstream event: ("tick", tick_dict) or ("end", reason).
UpstreamEvent = tuple[str, Any]
# Async source of upstream events for (session_id, resume_after_sequence). The
# resume value is the last contiguously-consumed MarketSnapshot.sequence_number;
# the source must replay every record with a higher sequence so a reconnect
# backfills the gap instead of skipping it (Phase 2 review P0).
AsyncTickSource = Callable[[str, int], AsyncIterator[UpstreamEvent]]
EventContextProvider = Callable[[datetime], EventContext]

# session_id -> latest frame, for REST recovery on reconnect.
_LATEST: dict[str, dict[str, Any]] = {}
# session_id -> live hub.
_HUBS: dict[str, "SessionHub"] = {}
_REPO_ROOT = Path(__file__).resolve().parents[4]
_EVENT_DIR = Path(os.getenv("OPTIONTRADER_EVENT_DIR", _REPO_ROOT / "data" / "events"))
_EVENT_STORE = EventContextStore(_EVENT_DIR)


def latest_frame(session_id: str) -> dict[str, Any] | None:
    """Most recent frame for a session, or None if never seen."""
    return _LATEST.get(session_id)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _rpc_code(exc: grpc.RpcError) -> str:
    """Best-effort gRPC status name; some RpcError instances lack code()."""
    code = getattr(exc, "code", None)
    if callable(code):
        try:
            return str(code().name)
        except Exception:  # noqa: BLE001
            return "UNKNOWN"
    return "UNKNOWN"


def _default_event_context(now_utc: datetime) -> EventContext:
    return _EVENT_STORE.get(now_utc)


def _stream_silence_seconds() -> float:
    raw = os.getenv("OPTIONTRADER_STREAM_SILENCE_SECONDS", "90")
    try:
        return max(float(raw), 0.01)
    except ValueError:
        return 90.0


def current_event_context(now_utc: datetime | None = None) -> EventContext:
    """Return the current sourced context for API inspection and readiness checks."""
    return _default_event_context(now_utc or datetime.now(timezone.utc))


def _tick_time(tick: dict[str, Any]) -> datetime:
    raw = (tick.get("snapshot") or {}).get("occurred_at_utc")
    if not isinstance(raw, str):
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return datetime.now(timezone.utc)


async def _grpc_source(session_id: str, resume_after_sequence: int) -> AsyncIterator[UpstreamEvent]:
    """Bridge the blocking gRPC tick iterator into async upstream events,
    resuming after the given sequence so a reconnect backfills missed records."""
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[UpstreamEvent] = asyncio.Queue()

    def pump() -> None:
        try:
            for tick in stream_ticks(session_id, resume_after_sequence=resume_after_sequence):
                loop.call_soon_threadsafe(queue.put_nowait, ("tick", tick))
            loop.call_soon_threadsafe(queue.put_nowait, ("end", "stream ended"))
        except grpc.RpcError as exc:
            loop.call_soon_threadsafe(queue.put_nowait, ("end", f"stream error: {_rpc_code(exc)}"))
        except Exception as exc:  # noqa: BLE001 — fail closed on any pump fault
            loop.call_soon_threadsafe(queue.put_nowait, ("end", f"stream fault: {exc}"))

    threading.Thread(target=pump, name=f"cockpit-{session_id}", daemon=True).start()
    while True:
        kind, payload = await queue.get()
        yield (kind, payload)
        if kind == "end":
            return


class SessionHub:
    """One projector + one upstream pump for a session, fanned out to N subs."""

    def __init__(
        self,
        config: ProjectorConfig,
        source: AsyncTickSource | None = None,
        event_provider: EventContextProvider | None = None,
        stream_silence_seconds: float | None = None,
    ) -> None:
        self._config = config
        self._source = source or _grpc_source
        self._projector = CockpitProjector(config=config)
        self._event_provider = event_provider or _default_event_context
        self._stream_silence_seconds = (
            stream_silence_seconds
            if stream_silence_seconds is not None
            else _stream_silence_seconds()
        )
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._latest: dict[str, Any] | None = None
        self._stopped = False
        self._task: asyncio.Task[None] | None = None
        # Last contiguously-consumed MarketSnapshot.sequence_number, so a
        # reconnect resumes the upstream after it and backfills the gap.
        self._last_market_seq = 0
        # Upstream reconnect backoff (seconds). Bounded so a permanently-dead
        # upstream doesn't hot-loop; short base keeps recovery snappy.
        self._backoff_base = 0.05
        self._backoff_max = 5.0

    def _start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        """Consume upstream, reconnecting with backoff across end/error, using
        ONE persistent projector so seq stays monotonic through every reconnect.

        On each upstream drop we publish a fail-closed DISCONNECTED frame; on the
        next successful attempt we resume projecting LIVE frames at a higher seq.
        The loop runs until the hub is explicitly stopped (a WS client staying
        connected therefore rides through upstream blips instead of being cut)."""
        backoff = self._backoff_base
        while not self._stopped:
            ended_reason = "stream ended"
            try:
                # Resume after the last contiguously-consumed record so the
                # upstream backfills any gap opened while we were disconnected.
                source = self._source(self._config.session_id, self._last_market_seq).__aiter__()
                while True:
                    try:
                        kind, payload = await asyncio.wait_for(
                            anext(source), timeout=self._stream_silence_seconds
                        )
                    except StopAsyncIteration:
                        break
                    except TimeoutError:
                        ended_reason = f"upstream silent for {self._stream_silence_seconds:g}s"
                        break
                    if self._stopped:
                        return
                    if kind == "tick":
                        tick = dict(payload)
                        tick_time = _tick_time(tick)
                        try:
                            event_context = self._event_provider(tick_time)
                        except Exception as exc:  # noqa: BLE001 — event faults fail closed
                            event_context = unavailable_event_context(
                                tick_time, f"event provider fault: {exc}"
                            )
                        tick["event_context"] = event_context.model_dump(mode="json")
                        frame = self._projector.apply(tick)
                        self._track_market_seq(frame)
                        self._publish(frame)
                        backoff = self._backoff_base  # healthy data resets backoff
                    else:
                        ended_reason = str(payload)
                        break
            except Exception as exc:  # noqa: BLE001 — fail closed on any hub fault
                ended_reason = f"upstream fault: {exc}"

            if self._stopped:
                return
            # Upstream dropped: fail closed, then wait and reconnect.
            self._publish(
                self._projector.disconnected_frame(_now_iso(), f"{ended_reason}; reconnecting")
            )
            try:
                await asyncio.sleep(backoff)
            except asyncio.CancelledError:
                return
            backoff = min(backoff * 2, self._backoff_max)

    def _track_market_seq(self, frame: dict[str, Any]) -> None:
        """Remember the last accepted market sequence so a reconnect resumes
        after it. BACKFILL frames are accepted into the projector's contiguous
        history even though their CockpitState stays STALE/No Trade, so use the
        projector's authoritative accepted cursor instead of display state."""
        self._last_market_seq = self._projector.last_market_sequence

    def stop(self) -> None:
        """Stop the hub: end its upstream loop and release its subscribers."""
        self._stopped = True
        if self._task is not None:
            self._task.cancel()

    def _publish(self, frame: dict[str, Any]) -> None:
        self._latest = frame
        _LATEST[self._config.session_id] = frame
        for q in list(self._subscribers):
            q.put_nowait(frame)

    async def subscribe(self) -> AsyncGenerator[dict[str, Any], None]:
        """Attach a WS client. Replays the latest frame immediately (so a
        reconnecting client sees current state), then streams live frames with
        monotonic seq across upstream reconnects. Ends when the hub is stopped or
        the client cancels the generator."""
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._subscribers.add(queue)
        self._start()
        try:
            if self._latest is not None:
                yield self._latest
            while not self._stopped:
                try:
                    frame = await asyncio.wait_for(queue.get(), timeout=0.25)
                except TimeoutError:
                    continue
                yield frame
        finally:
            self._subscribers.discard(queue)


def get_hub(
    config: ProjectorConfig,
    source: AsyncTickSource | None = None,
    event_provider: EventContextProvider | None = None,
    stream_silence_seconds: float | None = None,
) -> SessionHub:
    """Return the session's hub. A reconnecting client reuses the same live hub,
    so its projector's seq stays monotonic across reconnects. A hub that was
    explicitly stopped is replaced with a fresh one."""
    hub = _HUBS.get(config.session_id)
    if hub is None or hub._stopped:  # noqa: SLF001 — internal lifecycle check
        hub = SessionHub(
            config,
            source=source,
            event_provider=event_provider,
            stream_silence_seconds=stream_silence_seconds,
        )
        _HUBS[config.session_id] = hub
    return hub


def reset_hubs() -> None:
    """Test helper: stop and drop all hubs and cached frames for a clean slate."""
    for hub in _HUBS.values():
        hub.stop()
    _HUBS.clear()
    _LATEST.clear()
