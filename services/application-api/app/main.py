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
from typing import Literal

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, ValidationError

__all__ = ["app", "httpx"]

app = FastAPI(title="OptionTrader Application API", version="0.0.0")

TRADING_CORE_URL = os.getenv("TRADING_CORE_URL", "http://localhost:8080")
_TIMEOUT = 1.5

DataHealth = Literal["HEALTHY", "DEGRADED", "STALE", "DISCONNECTED", "RECONCILING"]
BrokerHealth = Literal["HEALTHY", "DEGRADED", "DISCONNECTED", "RECONCILING"]


class HealthResponse(BaseModel):
    status: str
    service: str
    environment: str


class ServiceHealth(BaseModel):
    """Mirrors health.json#/$defs/ServiceHealth. Extra upstream fields are
    rejected so a contract drift fails closed rather than passing through."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"] = "1.0"
    status: Literal["ok", "unreachable"]
    service: str
    environment: str | None = None
    data_health: DataHealth
    broker_health: BrokerHealth
    reconciled: bool
    new_position_allowed: bool


class MarketSnapshot(BaseModel):
    """Mirrors market_snapshot.json (required fields + strict extras)."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"] = "1.0"
    snapshot_id: str
    occurred_at_utc: str
    timestamp_et: str | None = None
    symbol: Literal["QQQ.US"]
    price: str
    open: str
    high: str | None = None
    low: str | None = None
    previous_close: str | None = None
    vwap: str
    volume: int | None = None
    opening_range_high: str | None = None
    opening_range_low: str | None = None
    premarket_high: str | None = None
    premarket_low: str | None = None
    sequence_number: int
    quote_age_ms: int | None = None
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
