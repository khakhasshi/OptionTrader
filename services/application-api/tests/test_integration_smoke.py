"""Cross-language integration smoke: Rust producer -> gRPC -> SessionHub -> CockpitState.

This is the reproducible end-to-end smoke the Phase 2 review asked for. It boots
the real trading-core binary (single-producer gRPC MarketService over a replay
fixture), drives the actual Python gRPC client through a real SessionHub, and
asserts the emitted CockpitState frames.

It SKIPS (does not fail) when the trading-core binary is absent, so the api-only
CI job stays green; to run it, build core first:
    make setup-core && (cd services/trading-core && cargo build --bin trading-core)
    make test-api   # this test then executes
"""

from __future__ import annotations

import asyncio
import glob
import json
import os
import socket
import subprocess
import threading
import time
from collections.abc import AsyncIterator
from typing import Any

import grpc
import pytest
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from app.realtime.client import stream_ticks
from app.realtime.projector import ProjectorConfig
from app.realtime.session import SessionHub

_REPO = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
_BINARY = os.path.join(_REPO, "services", "trading-core", "target", "debug", "trading-core")
_SCHEMA_DIR = os.path.join(_REPO, "packages", "contracts", "jsonschema")

pytestmark = pytest.mark.skipif(
    not os.path.exists(_BINARY),
    reason="trading-core binary not built; run cargo build --bin trading-core",
)


def _validator() -> Draft202012Validator:
    res = {
        os.path.basename(f): Resource.from_contents(json.load(open(f)))
        for f in glob.glob(os.path.join(_SCHEMA_DIR, "*.json"))
    }
    reg = Registry().with_resources(list(res.items()))
    return Draft202012Validator(res["cockpit_state.json"].contents, registry=reg)


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = int(s.getsockname()[1])
    s.close()
    return port


def _wait_port(port: int, timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as s:
            s.settimeout(0.25)
            try:
                s.connect(("127.0.0.1", port))
                return
            except OSError:
                time.sleep(0.1)
    raise TimeoutError(f"trading-core gRPC port {port} never came up")


def _grpc_source_for(
    target: str,
) -> Any:
    """A hub tick source that consumes the real gRPC stream at `target`."""

    async def src(session_id: str) -> AsyncIterator[tuple[str, Any]]:
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()

        def pump() -> None:
            try:
                for tick in stream_ticks(session_id, target=target):
                    loop.call_soon_threadsafe(queue.put_nowait, ("tick", tick))
                loop.call_soon_threadsafe(queue.put_nowait, ("end", "stream ended"))
            except grpc.RpcError as exc:
                loop.call_soon_threadsafe(queue.put_nowait, ("end", f"rpc: {exc.code().name}"))

        threading.Thread(target=pump, daemon=True).start()
        while True:
            kind, payload = await queue.get()
            yield (kind, payload)
            if kind == "end":
                return

    return src


def test_end_to_end_rust_grpc_hub_cockpit() -> None:
    grpc_port = _free_port()
    http_port = _free_port()
    env = {
        **os.environ,
        "OPTIONTRADER_REPLAY_TICK_MS": "5",
        "TRADING_CORE_GRPC_PORT": str(grpc_port),
        "TRADING_CORE_PORT": str(http_port),
    }
    proc = subprocess.Popen(
        [_BINARY], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    try:
        _wait_port(grpc_port)

        async def run() -> list[dict[str, Any]]:
            hub = SessionHub(
                ProjectorConfig(
                    session_id="itest", rule_version="itest-1", opening_range_minutes=3
                ),
                source=_grpc_source_for(f"127.0.0.1:{grpc_port}"),
            )
            frames: list[dict[str, Any]] = []
            gen = hub.subscribe()
            # 6 fixture ticks + at least one terminal DISCONNECTED after feed ends.
            try:
                while len(frames) < 7:
                    frames.append(await asyncio.wait_for(gen.__anext__(), timeout=5))
            finally:
                await gen.aclose()
                hub.stop()
            return frames

        frames = asyncio.run(run())
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()

    validator = _validator()
    # Every frame that crossed the whole stack must be schema-valid.
    for f in frames:
        assert list(validator.iter_errors(f)) == [], f"invalid frame: {f}"

    live = [f for f in frames if f["connection"] == "LIVE"]
    assert len(live) >= 6, f"expected >=6 LIVE frames from the 6-tick fixture, got {len(live)}"
    # LIVE frames carry the Rust-authoritative snapshot.
    for f in live:
        assert f["snapshot"] is not None
        assert f["snapshot"]["symbol"] == "QQQ.US"
        assert f["snapshot"]["data_health"] == "HEALTHY"

    # seq is monotonic and starts at 0 across the whole crossed-stack stream.
    seqs = [f["seq"] for f in frames]
    assert seqs == sorted(seqs)
    assert seqs[0] == 0

    # Feed ends -> a fail-closed DISCONNECTED frame appears, not a stale LIVE.
    assert any(f["connection"] == "DISCONNECTED" for f in frames)
    assert all(f["new_position_allowed"] is False for f in frames if f["connection"] != "LIVE")
