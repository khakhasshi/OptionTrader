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


@app.get("/api/v1/core/health")
async def core_health() -> dict[str, object]:
    """BFF proxy to trading-core health for the React Cockpit.

    Fail closed: if trading-core is unreachable, report STALE so the UI removes
    the tradable state rather than trusting a stale cache.
    """
    try:
        async with httpx.AsyncClient(timeout=1.5) as client:
            resp = await client.get(f"{TRADING_CORE_URL}/health")
            resp.raise_for_status()
            data: dict[str, object] = resp.json()
            return data
    except httpx.HTTPError:
        return {
            "status": "unreachable",
            "service": "trading-core",
            "data_health": "STALE",
            "broker_health": "DISCONNECTED",
        }
