"""BFF proxy tests: strict upstream validation, fail-closed on any bad response.

trading-core is mocked via httpx.MockTransport so these run without a live core.
Covers: passthrough, transport error, upstream timeout, invalid JSON, missing
required field, and wrong enum value — each must fail closed.
"""

from __future__ import annotations

from collections.abc import Callable

import app.main as main
import httpx
import pytest
from fastapi.testclient import TestClient

client = TestClient(main.app)

Handler = Callable[[httpx.Request], httpx.Response]

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
    "occurred_at_utc": "2026-07-20T13:45:00Z",
    "symbol": "QQQ.US",
    "price": "500.00",
    "open": "498.10",
    "vwap": "499.40",
    "sequence_number": 123,
    "data_health": "HEALTHY",
}


def _patch_core(monkeypatch: pytest.MonkeyPatch, handler: Handler) -> None:
    """Route the BFF's outbound httpx calls through a mock transport."""
    real_client = httpx.AsyncClient

    def factory(*args: object, **kwargs: object) -> httpx.AsyncClient:
        kwargs.pop("timeout", None)
        return real_client(transport=httpx.MockTransport(handler), timeout=1.5)

    monkeypatch.setattr(main.httpx, "AsyncClient", factory)


def _boom(req: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("core down", request=req)


def _timeout(req: httpx.Request) -> httpx.Response:
    raise httpx.ReadTimeout("core slow", request=req)


def _bad_json(req: httpx.Request) -> httpx.Response:
    return httpx.Response(200, content=b"{not json", headers={"content-type": "application/json"})


# --- /core/health ----------------------------------------------------------
def test_core_health_passthrough(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_core(monkeypatch, lambda req: httpx.Response(200, json=HEALTHY_CORE))
    body = client.get("/api/v1/core/health").json()
    assert body["status"] == "ok"
    assert body["new_position_allowed"] is True


def test_core_health_fail_closed_on_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_core(monkeypatch, _boom)
    body = client.get("/api/v1/core/health").json()
    assert body["status"] == "unreachable"
    assert body["data_health"] == "STALE"
    assert body["broker_health"] == "DISCONNECTED"
    assert body["reconciled"] is False
    assert body["new_position_allowed"] is False


def test_core_health_fail_closed_on_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_core(monkeypatch, _timeout)
    body = client.get("/api/v1/core/health").json()
    assert body["status"] == "unreachable"
    assert body["new_position_allowed"] is False


def test_core_health_fail_closed_on_invalid_json(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_core(monkeypatch, _bad_json)
    body = client.get("/api/v1/core/health").json()
    assert body["status"] == "unreachable"
    assert body["new_position_allowed"] is False


def test_core_health_fail_closed_on_missing_field(monkeypatch: pytest.MonkeyPatch) -> None:
    partial = {k: v for k, v in HEALTHY_CORE.items() if k != "new_position_allowed"}
    _patch_core(monkeypatch, lambda req: httpx.Response(200, json=partial))
    body = client.get("/api/v1/core/health").json()
    assert body["status"] == "unreachable"
    assert body["new_position_allowed"] is False


def test_core_health_fail_closed_on_bad_enum(monkeypatch: pytest.MonkeyPatch) -> None:
    bad = {**HEALTHY_CORE, "broker_health": "WOBBLY"}
    _patch_core(monkeypatch, lambda req: httpx.Response(200, json=bad))
    body = client.get("/api/v1/core/health").json()
    assert body["status"] == "unreachable"
    assert body["new_position_allowed"] is False


# --- /market/snapshot ------------------------------------------------------
def test_market_snapshot_passthrough(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_core(monkeypatch, lambda req: httpx.Response(200, json=SNAPSHOT_CORE))
    resp = client.get("/api/v1/market/snapshot")
    assert resp.status_code == 200
    body = resp.json()
    assert body["symbol"] == "QQQ.US"
    assert body["snapshot_id"] == SNAPSHOT_CORE["snapshot_id"]
    assert body["price"] == "500.00"


def test_market_snapshot_fail_closed_on_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_core(monkeypatch, _boom)
    resp = client.get("/api/v1/market/snapshot")
    assert resp.status_code == 503
    body = resp.json()
    assert body["error"] == "snapshot_unavailable"
    assert body["data_health"] == "STALE"
    # Must NOT masquerade as a live snapshot.
    assert "price" not in body
    assert "snapshot_id" not in body


def test_market_snapshot_fail_closed_on_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_core(monkeypatch, _timeout)
    resp = client.get("/api/v1/market/snapshot")
    assert resp.status_code == 503
    assert resp.json()["error"] == "snapshot_unavailable"


def test_market_snapshot_fail_closed_on_invalid_json(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_core(monkeypatch, _bad_json)
    resp = client.get("/api/v1/market/snapshot")
    assert resp.status_code == 503
    assert resp.json()["error"] == "snapshot_unavailable"


def test_market_snapshot_fail_closed_on_missing_field(monkeypatch: pytest.MonkeyPatch) -> None:
    partial = {k: v for k, v in SNAPSHOT_CORE.items() if k != "price"}
    _patch_core(monkeypatch, lambda req: httpx.Response(200, json=partial))
    resp = client.get("/api/v1/market/snapshot")
    assert resp.status_code == 503
    assert resp.json()["error"] == "snapshot_unavailable"


def test_market_snapshot_fail_closed_on_bad_enum(monkeypatch: pytest.MonkeyPatch) -> None:
    bad = {**SNAPSHOT_CORE, "data_health": "SUPER"}
    _patch_core(monkeypatch, lambda req: httpx.Response(200, json=bad))
    resp = client.get("/api/v1/market/snapshot")
    assert resp.status_code == 503
    assert resp.json()["error"] == "snapshot_unavailable"
