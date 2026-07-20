"""Async bridge from the (blocking) gRPC tick iterator to CockpitState frames.

The gRPC Python client streams synchronously, so we run the iterator in a worker
thread and hand frames to the asyncio event loop via a queue. Each connection
gets its own :class:`CockpitProjector`; the most recent frame per session is
cached so a reconnecting client can recover current state over REST before
resuming the WebSocket.

Fail closed: transport errors and stream end both terminate the generator with a
final DISCONNECTED frame, so the UI can never sit on a stale tradable state.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator, Callable, Iterator
from datetime import datetime, timezone
from typing import Any

import grpc

from app.realtime.client import stream_ticks
from app.realtime.projector import CockpitProjector, ProjectorConfig

# session_id -> latest frame, for REST recovery on reconnect.
_LATEST: dict[str, dict[str, Any]] = {}

TickSource = Callable[[str], Iterator[dict[str, Any]]]


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


async def cockpit_frames(
    config: ProjectorConfig,
    tick_source: TickSource | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """Yield CockpitState frames for one session until the stream ends.

    ``tick_source`` yields tick dicts for a session id (defaults to the live
    gRPC stream); injectable so tests can drive the projector deterministically.
    """
    source = tick_source or (lambda sid: stream_ticks(sid))
    projector = CockpitProjector(config=config)
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()

    def pump() -> None:
        try:
            for tick in source(config.session_id):
                loop.call_soon_threadsafe(queue.put_nowait, ("tick", tick))
            loop.call_soon_threadsafe(queue.put_nowait, ("end", "stream ended"))
        except grpc.RpcError as exc:
            loop.call_soon_threadsafe(queue.put_nowait, ("end", f"stream error: {_rpc_code(exc)}"))
        except Exception as exc:  # noqa: BLE001 — fail closed on any pump fault
            loop.call_soon_threadsafe(queue.put_nowait, ("end", f"stream fault: {exc}"))

    thread = threading.Thread(target=pump, name=f"cockpit-{config.session_id}", daemon=True)
    thread.start()

    while True:
        kind, payload = await queue.get()
        if kind == "tick":
            frame = projector.apply(payload)
        else:
            frame = projector.disconnected_frame(_now_iso(), str(payload))
        _LATEST[config.session_id] = frame
        yield frame
        if kind == "end":
            return
