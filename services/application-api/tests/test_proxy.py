"""BFF proxy tests: passthrough on success, fail-closed on upstream error.

trading-core is mocked via httpx.MockTransport so these run without a live core.
"""
from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient

import app.main as main

client = TestClient(main.app)

HEALTHY_CORE = {
    "schema_version": "1.0",
    "status": "ok",
    "service": "trading-core",
    "environment": "local",
    "data_health": "HEALTHY",
    "broker_health": "HEALTHY",
    "reconciled": True,
    "new_position_allowed": True,
}

SNAPSHOT_CORE = {
    "schema_version": "1.0",
    "snapshot_id": "mkt_20260720_094500_000123",
    "symbol": "QQQ.US",
    "price": "500.00",
    "data_health": "HEALTHY",
}


def _patch_core(monkeypatch: pytest.MonkeyPatch, handler) -> None:
    """Route the BFF's outbound httpx calls through a mock transport."""
    real_client = httpx.AsyncClient

    def factory(*args: object, **kwargs: object) -> httpx.AsyncClient:
        kwargs.pop("timeout", None)
        return real_client(transport=httpx.MockTransport(handler), timeout=1.5)

    monkeypatch.setattr(main.httpx, "AsyncClient", factory)


def test_core_health_passthrough(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_core(monkeypatch, lambda req: httpx.Response(200, json=HEALTHY_CORE))
    body = client.get("/api/v1/core/health").json()
    assert body == HEALTHY_CORE
    assert body["new_position_allowed"] is True


def test_core_health_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("core down", request=req)

    _patch_core(monkeypatch, boom)
    body = client.get("/api/v1/core/health").json()
    assert body["status"] == "unreachable"
    assert body["data_health"] == "STALE"
    assert body["broker_health"] == "DISCONNECTED"
    assert body["reconciled"] is False
    assert body["new_position_allowed"] is False


def test_market_snapshot_passthrough(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_core(monkeypatch, lambda req: httpx.Response(200, json=SNAPSHOT_CORE))
    body = client.get("/api/v1/market/snapshot").json()
    assert body == SNAPSHOT_CORE
    assert body["symbol"] == "QQQ.US"


def test_market_snapshot_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("core down", request=req)

    _patch_core(monkeypatch, boom)
    body = client.get("/api/v1/market/snapshot").json()
    assert body["status"] == "unreachable"
    assert body["data_health"] == "STALE"
