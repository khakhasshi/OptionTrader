"""OptionTrader Application Service entrypoint.

Phase 0: health check + skeleton. Strategy/regime/vol/replay/llm modules land in
later phases (see PROJECT_PLAN.md). This service never bypasses the Rust Risk &
Execution Gateway to place orders.
"""
from __future__ import annotations

import os

import httpx
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="OptionTrader Application API", version="0.0.0")

TRADING_CORE_URL = os.getenv("TRADING_CORE_URL", "http://localhost:8080")


class HealthResponse(BaseModel):
    status: str
    service: str
    environment: str


@app.get("/api/v1/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        service="application-api",
        environment=os.getenv("OPTIONTRADER_ENV", "local"),
    )


# Fail-closed posture returned whenever trading-core cannot be reached or
# returns an error. Shape matches health.json#/$defs/ServiceHealth so the
# Cockpit gate sees a complete, non-tradable contract rather than a partial one.
_UNREACHABLE_HEALTH: dict[str, object] = {
    "schema_version": "1.0",
    "status": "unreachable",
    "service": "trading-core",
    "environment": os.getenv("OPTIONTRADER_ENV", "local"),
    "data_health": "STALE",
    "broker_health": "DISCONNECTED",
    "reconciled": False,
    "new_position_allowed": False,
}


@app.get("/api/v1/core/health")
async def core_health() -> dict[str, object]:
    """BFF proxy to trading-core health for the React Cockpit.

    Fail closed: if trading-core is unreachable or errors, report a complete
    non-tradable ServiceHealth so the UI removes the tradable state rather than
    trusting a stale cache. `new_position_allowed` is authoritative from core;
    the fallback forces it false.
    """
    try:
        async with httpx.AsyncClient(timeout=1.5) as client:
            resp = await client.get(f"{TRADING_CORE_URL}/health")
            resp.raise_for_status()
            data: dict[str, object] = resp.json()
            return data
    except httpx.HTTPError:
        return dict(_UNREACHABLE_HEALTH)


@app.get("/api/v1/market/snapshot")
async def market_snapshot() -> dict[str, object]:
    """BFF proxy to trading-core's latest MarketSnapshot (market_snapshot.json).

    Fail closed: on any upstream error, surface data_health=STALE so downstream
    consumers treat the quote as untradable instead of acting on stale numbers.
    """
    try:
        async with httpx.AsyncClient(timeout=1.5) as client:
            resp = await client.get(f"{TRADING_CORE_URL}/market/snapshot")
            resp.raise_for_status()
            data: dict[str, object] = resp.json()
            return data
    except httpx.HTTPError:
        return {
            "schema_version": "1.0",
            "status": "unreachable",
            "symbol": "QQQ.US",
            "data_health": "STALE",
        }
