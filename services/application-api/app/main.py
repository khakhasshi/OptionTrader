"""OptionTrader Application Service entrypoint.

Phase 0: health check + skeleton + fail-closed proxies to the Rust trading
core. Strategy/regime/vol/replay/llm modules land in later phases (see
PROJECT_PLAN.md). This service never bypasses the Rust Risk & Execution Gateway
to place orders.

The two proxy endpoints validate the upstream response against strict Pydantic
models that mirror the JSON Schema contracts. An unreachable core, invalid
JSON, a missing field, or a bad enum all fail closed: /core/health returns a
complete non-tradable ServiceHealth, and /market/snapshot returns a canonical
SnapshotUnavailable body with HTTP 503 (never a partial fake MarketSnapshot).
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Annotated, Literal

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
)

from app.realtime.projector import ProjectorConfig
from app.realtime.session import get_hub, latest_frame

__all__ = ["app", "httpx"]

app = FastAPI(title="OptionTrader Application API", version="0.0.0")

TRADING_CORE_URL = os.getenv("TRADING_CORE_URL", "http://localhost:8080")
_TIMEOUT = 1.5
_RULE_VERSION = os.getenv("OPTIONTRADER_RULE_VERSION", "phase1-2026-07-21")

DataHealth = Literal["HEALTHY", "DEGRADED", "STALE", "DISCONNECTED", "RECONCILING"]
BrokerHealth = Literal["HEALTHY", "DEGRADED", "DISCONNECTED", "RECONCILING"]


def _check_utc(v: str) -> str:
    """common.json#/$defs/utcTimestamp: RFC3339 date-time ending in Z."""
    if not v.endswith("Z"):
        raise ValueError("utcTimestamp must end in Z")
    datetime.fromisoformat(v.replace("Z", "+00:00"))
    return v


def _check_et(v: str) -> str:
    """common.json#/$defs/etTimestamp: parseable RFC3339 date-time (display only)."""
    datetime.fromisoformat(v.replace("Z", "+00:00"))
    return v


# common.json#/$defs/decimal — fixed-point string; rejects "nan"/"inf"/"".
Decimal = Annotated[str, StringConstraints(pattern=r"^-?[0-9]+(\.[0-9]+)?$")]
UtcTimestamp = Annotated[str, AfterValidator(_check_utc)]
EtTimestamp = Annotated[str, AfterValidator(_check_et)]
NonNegInt = Annotated[int, Field(ge=0)]
SchemaVersion = Literal["1.0"]


class HealthResponse(BaseModel):
    status: str
    service: str
    environment: str


class ServiceHealth(BaseModel):
    """Mirrors health.json#/$defs/ServiceHealth. strict=True so no coercion
    (e.g. "true" -> True) happens; extra upstream fields are rejected. Contract
    drift or a wrong-typed field fails closed rather than passing through.
    schema_version is required with no default — a missing version is invalid."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: SchemaVersion
    status: Literal["ok", "unreachable"]
    service: str
    environment: str | None = None
    data_health: DataHealth
    broker_health: BrokerHealth
    reconciled: bool
    new_position_allowed: bool


class MarketSnapshot(BaseModel):
    """Mirrors market_snapshot.json (required fields, strict extras, JSON Schema
    scalar constraints: decimal pattern, UTC/ET timestamps, non-negative ints).
    strict=True blocks type coercion; schema_version is required (no default)."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: SchemaVersion
    snapshot_id: str
    occurred_at_utc: UtcTimestamp
    timestamp_et: EtTimestamp | None = None
    symbol: Literal["QQQ.US"]
    price: Decimal
    open: Decimal
    high: Decimal | None = None
    low: Decimal | None = None
    previous_close: Decimal | None = None
    vwap: Decimal
    volume: NonNegInt | None = None
    opening_range_high: Decimal | None = None
    opening_range_low: Decimal | None = None
    premarket_high: Decimal | None = None
    premarket_low: Decimal | None = None
    sequence_number: NonNegInt
    quote_age_ms: NonNegInt | None = None
    data_health: DataHealth


class SnapshotUnavailable(BaseModel):
    """Canonical fail-closed body when a valid MarketSnapshot cannot be
    obtained. Deliberately distinct from MarketSnapshot (no price/snapshot_id)
    so downstream consumers cannot mistake it for a live quote."""

    schema_version: Literal["1.0"] = "1.0"
    error: Literal["snapshot_unavailable"] = "snapshot_unavailable"
    reason: str
    data_health: Literal["STALE"] = "STALE"


def _unreachable_health(reason: str) -> ServiceHealth:
    return ServiceHealth(
        schema_version="1.0",
        status="unreachable",
        service="trading-core",
        environment=os.getenv("OPTIONTRADER_ENV", "local"),
        data_health="STALE",
        broker_health="DISCONNECTED",
        reconciled=False,
        new_position_allowed=False,
    )


@app.get("/api/v1/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        service="application-api",
        environment=os.getenv("OPTIONTRADER_ENV", "local"),
    )


@app.get("/api/v1/core/health", response_model=ServiceHealth)
async def core_health() -> ServiceHealth:
    """BFF proxy to trading-core health, validated against the ServiceHealth
    contract. Fail closed on transport error, invalid JSON, or schema mismatch:
    return a complete non-tradable ServiceHealth (new_position_allowed=False)."""
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.get(f"{TRADING_CORE_URL}/health")
            resp.raise_for_status()
            raw = resp.json()
    except httpx.HTTPError as exc:
        return _unreachable_health(f"transport: {type(exc).__name__}")
    except ValueError as exc:  # invalid JSON body
        return _unreachable_health(f"invalid_json: {exc}")

    try:
        return ServiceHealth.model_validate(raw)
    except ValidationError as exc:
        return _unreachable_health(f"contract_violation: {exc.error_count()} errors")


@app.get("/api/v1/market/snapshot")
async def market_snapshot() -> JSONResponse:
    """BFF proxy to trading-core's latest MarketSnapshot, validated against the
    MarketSnapshot contract. Fail closed on transport error, invalid JSON, or
    schema mismatch: return HTTP 503 with a canonical SnapshotUnavailable body
    (never a partial/fake MarketSnapshot)."""

    def unavailable(reason: str) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content=SnapshotUnavailable(reason=reason).model_dump(),
        )

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.get(f"{TRADING_CORE_URL}/market/snapshot")
            resp.raise_for_status()
            raw = resp.json()
    except httpx.HTTPError as exc:
        return unavailable(f"transport: {type(exc).__name__}")
    except ValueError as exc:  # invalid JSON body
        return unavailable(f"invalid_json: {exc}")

    try:
        snapshot = MarketSnapshot.model_validate(raw)
    except ValidationError as exc:
        return unavailable(f"contract_violation: {exc.error_count()} errors")

    return JSONResponse(status_code=200, content=snapshot.model_dump(exclude_none=True))


@app.get("/api/v1/cockpit/state")
def cockpit_state(session_id: str) -> JSONResponse:
    """Snapshot-recovery endpoint: the latest CockpitState frame for a session.

    A reconnecting Cockpit calls this to recover current state before resuming
    the WebSocket. Returns a fail-closed DISCONNECTED frame (never a stale
    tradable one) when the session has no frame yet."""
    frame = latest_frame(session_id)
    if frame is None:
        frame = {
            "schema_version": "1.0",
            "seq": 0,
            "session_id": session_id,
            "server_time_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "connection": "DISCONNECTED",
            "new_position_allowed": False,
            "snapshot": None,
            "regime": None,
            "vol": None,
            "signal": None,
            "risk_flags": ["no frames yet for session"],
        }
    return JSONResponse(status_code=200, content=frame)


@app.websocket("/api/v1/stream/cockpit")
async def stream_cockpit(websocket: WebSocket) -> None:
    """Push CockpitState frames for one session over WebSocket.

    The session_id query param ties the stream to one trading session. All WS
    connections for a session share ONE hub (one projector, monotonic seq, one
    upstream consumer), so a reconnecting client resumes at a strictly higher
    seq and the engines never re-run per client. On disconnect the socket
    closes; the client reconnects and recovers via GET /api/v1/cockpit/state."""
    await websocket.accept()
    session_id = websocket.query_params.get("session_id", "default")
    config = ProjectorConfig(session_id=session_id, rule_version=_RULE_VERSION)
    hub = get_hub(config)
    try:
        async for frame in hub.subscribe():
            await websocket.send_json(frame)
    except WebSocketDisconnect:
        return
