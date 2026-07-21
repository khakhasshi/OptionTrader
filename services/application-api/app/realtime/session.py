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
import threading
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from datetime import datetime, timezone
from typing import Any

import grpc

from app.realtime.client import stream_ticks
from app.realtime.projector import CockpitProjector, ProjectorConfig

# An upstream event: ("tick", tick_dict) or ("end", reason).
UpstreamEvent = tuple[str, Any]
# Async source of upstream events for a session id (injectable for tests).
AsyncTickSource = Callable[[str], AsyncIterator[UpstreamEvent]]

# session_id -> latest frame, for REST recovery on reconnect.
_LATEST: dict[str, dict[str, Any]] = {}
# session_id -> live hub.
_HUBS: dict[str, "SessionHub"] = {}


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


async def _grpc_source(session_id: str) -> AsyncIterator[UpstreamEvent]:
    """Bridge the blocking gRPC tick iterator into async upstream events."""
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[UpstreamEvent] = asyncio.Queue()

    def pump() -> None:
        try:
            for tick in stream_ticks(session_id):
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


# PLACEHOLDER_HUB


class SessionHub:
    """One projector + one upstream pump for a session, fanned out to N subs."""

    def __init__(self, config: ProjectorConfig, source: AsyncTickSource | None = None) -> None:
        self._config = config
        self._source = source or _grpc_source
        self._projector = CockpitProjector(config=config)
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._latest: dict[str, Any] | None = None
        self._stopped = False
        self._task: asyncio.Task[None] | None = None
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
                async for kind, payload in self._source(self._config.session_id):
                    if self._stopped:
                        return
                    if kind == "tick":
                        self._publish(self._projector.apply(payload))
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


def get_hub(config: ProjectorConfig, source: AsyncTickSource | None = None) -> SessionHub:
    """Return the session's hub. A reconnecting client reuses the same live hub,
    so its projector's seq stays monotonic across reconnects. A hub that was
    explicitly stopped is replaced with a fresh one."""
    hub = _HUBS.get(config.session_id)
    if hub is None or hub._stopped:  # noqa: SLF001 — internal lifecycle check
        hub = SessionHub(config, source=source)
        _HUBS[config.session_id] = hub
    return hub


def reset_hubs() -> None:
    """Test helper: stop and drop all hubs and cached frames for a clean slate."""
    for hub in _HUBS.values():
        hub.stop()
    _HUBS.clear()
    _LATEST.clear()
