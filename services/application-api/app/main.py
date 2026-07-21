"""OptionTrader Application Service entrypoint.

Provides fail-closed proxies to Rust trading-core, the Phase 2 EventContext API,
and the per-session real-time cockpit stream. This service never bypasses the
Rust Risk & Execution Gateway to place orders.

The two proxy endpoints validate the upstream response against strict Pydantic
models that mirror the JSON Schema contracts. An unreachable core, invalid
JSON, a missing field, or a bad enum all fail closed: /core/health returns a
complete non-tradable ServiceHealth, and /market/snapshot returns a canonical
SnapshotUnavailable body with HTTP 503 (never a partial fake MarketSnapshot).
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from functools import lru_cache
from threading import RLock
from typing import Annotated, Literal, Mapping

import grpc
import httpx
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
)
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.persistence import (
    latest_execution_ticket,
    persist_confirmation_intent,
    persist_order_projection,
    persist_staged_candidate,
    staged_plan_projection,
)
from app.realtime.projector import ProjectorConfig
from app.realtime.session import current_event_context, get_hub, latest_frame
from app.trading.grpc_client import (
    cancel_order as grpc_cancel_order,
    confirm_candidate as grpc_confirm_candidate,
    get_order as grpc_get_order,
    stage_candidate as grpc_stage_candidate,
)
from app.trading.models import CandidateTradePlan, ExecutionOrder, RiskDecision

__all__ = ["app", "httpx"]


def _validate_single_worker_configuration(environment: Mapping[str, str]) -> None:
    """Opaque confirmation capabilities are process-local by design."""
    for name in ("OPTIONTRADER_API_WORKERS", "WEB_CONCURRENCY", "UVICORN_WORKERS"):
        raw = environment.get(name)
        if raw is None:
            continue
        try:
            workers = int(raw)
        except ValueError as exc:
            raise RuntimeError(f"{name} must be the integer 1") from exc
        if workers != 1:
            raise RuntimeError(f"{name}=1 is required while confirmation tokens are process-local")


_validate_single_worker_configuration(os.environ)

app = FastAPI(title="OptionTrader Application API", version="0.0.0")

TRADING_CORE_URL = os.getenv("TRADING_CORE_URL", "http://localhost:8080")
_TIMEOUT = 1.5
_RULE_VERSION = os.getenv("OPTIONTRADER_RULE_VERSION", "UNCONFIRMED")

DataHealth = Literal["HEALTHY", "DEGRADED", "STALE", "DISCONNECTED", "RECONCILING"]
BrokerHealth = Literal["HEALTHY", "DEGRADED", "DISCONNECTED", "RECONCILING"]


@lru_cache(maxsize=1)
def _execution_engine(database_url: str) -> Engine:
    url = database_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return create_engine(url, pool_pre_ping=True)


def _require_execution_engine() -> Engine:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise HTTPException(status_code=503, detail="execution_audit_unavailable")
    return _execution_engine(database_url)


def _grpc_http_error(exc: grpc.RpcError) -> HTTPException:
    code = exc.code()
    status = {
        grpc.StatusCode.NOT_FOUND: 404,
        grpc.StatusCode.PERMISSION_DENIED: 403,
        grpc.StatusCode.FAILED_PRECONDITION: 409,
    }.get(code, 503)
    return HTTPException(status_code=status, detail=f"execution_gateway_{code.name.lower()}")


class ConfirmOrderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    plan_hash: Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]


class StagedCandidateView(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    initial_risk_decision: RiskDecision
    order: ExecutionOrder | None


class ExecutionTicket(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    plan: CandidateTradePlan
    order: ExecutionOrder


_CONFIRMATION_TOKENS: dict[str, str] = {}
_CONFIRMATION_TOKENS_LOCK = RLock()


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
            "event_context": None,
            "risk_flags": ["no frames yet for session"],
        }
    return JSONResponse(status_code=200, content=frame)


@app.get("/api/v1/events/context")
@app.get("/api/v1/events/today")
def event_context() -> JSONResponse:
    """Current deterministic EventContext; unavailable inputs remain HTTP 200 but fail closed."""
    context = current_event_context()
    return JSONResponse(status_code=200, content=context.model_dump(mode="json"))


@app.post("/api/v1/trading/candidates/stage", response_model=StagedCandidateView)
def stage_trading_candidate(plan: CandidateTradePlan) -> StagedCandidateView:
    """Run Rust Initial Risk, issue a hash-bound confirmation challenge, and audit it."""
    engine = _require_execution_engine()
    try:
        existing = staged_plan_projection(engine, plan.plan_id)
        if existing is not None:
            _status, durable_order = existing
            if durable_order is None:
                raise HTTPException(status_code=409, detail="candidate_already_audited")
            try:
                gateway_order = grpc_get_order(durable_order.order_id)
            except grpc.RpcError as exc:
                if exc.code() == grpc.StatusCode.NOT_FOUND:
                    raise HTTPException(
                        status_code=409, detail="execution_reconciliation_required"
                    ) from exc
                raise
            if (
                gateway_order.plan_hash != durable_order.plan_hash
                or gateway_order.idempotency_key != durable_order.idempotency_key
            ):
                raise HTTPException(status_code=409, detail="execution_reconciliation_required")
        result = grpc_stage_candidate(plan, current_event_context())
        persist_staged_candidate(engine, plan, result)
        if result.order is not None:
            with _CONFIRMATION_TOKENS_LOCK:
                _CONFIRMATION_TOKENS[result.order.order_id] = result.confirmation_token
        return StagedCandidateView(
            initial_risk_decision=result.initial_risk_decision,
            order=result.order,
        )
    except grpc.RpcError as exc:
        raise _grpc_http_error(exc) from exc
    except (SQLAlchemyError, ValueError) as exc:
        raise HTTPException(status_code=503, detail="execution_audit_write_failed") from exc


@app.post("/api/v1/trading/orders/{order_id}/confirm", response_model=ExecutionOrder)
def confirm_trading_order(order_id: str, body: ConfirmOrderRequest) -> ExecutionOrder:
    """Audit intent, then ask Rust to rerun Final Risk and submit paper/shadow."""
    engine = _require_execution_engine()
    try:
        with _CONFIRMATION_TOKENS_LOCK:
            confirmation_token = _CONFIRMATION_TOKENS.get(order_id)
        if confirmation_token is None:
            raise HTTPException(status_code=409, detail="confirmation_reconciliation_required")
        recorded = persist_confirmation_intent(engine, order_id, body.plan_hash, "local-operator")
        if not recorded:
            existing = grpc_get_order(order_id)
            if existing.state != "AWAITING_CONFIRMATION":
                with _CONFIRMATION_TOKENS_LOCK:
                    _CONFIRMATION_TOKENS.pop(order_id, None)
                return existing
        order = grpc_confirm_candidate(
            order_id,
            body.plan_hash,
            confirmation_token,
            current_event_context(),
        )
        persist_order_projection(
            engine,
            order,
            action="ORDER_CONFIRM_RESULT",
            actor="rust-execution-gateway",
        )
        if order.state != "AWAITING_CONFIRMATION":
            with _CONFIRMATION_TOKENS_LOCK:
                _CONFIRMATION_TOKENS.pop(order_id, None)
        return order
    except grpc.RpcError as exc:
        raise _grpc_http_error(exc) from exc
    except (SQLAlchemyError, ValueError) as exc:
        raise HTTPException(status_code=503, detail="execution_audit_write_failed") from exc


@app.post("/api/v1/trading/orders/{order_id}/cancel", response_model=ExecutionOrder)
def cancel_trading_order(order_id: str) -> ExecutionOrder:
    engine = _require_execution_engine()
    try:
        order = grpc_cancel_order(order_id)
        persist_order_projection(
            engine, order, action="ORDER_CANCEL_RESULT", actor="rust-execution-gateway"
        )
        return order
    except grpc.RpcError as exc:
        raise _grpc_http_error(exc) from exc
    except (SQLAlchemyError, ValueError) as exc:
        raise HTTPException(status_code=503, detail="execution_audit_write_failed") from exc


@app.get("/api/v1/trading/orders/{order_id}", response_model=ExecutionOrder)
def trading_order(order_id: str) -> ExecutionOrder:
    engine = _require_execution_engine()
    try:
        order = grpc_get_order(order_id)
        persist_order_projection(
            engine, order, action="ORDER_RECONCILED", actor="rust-execution-gateway"
        )
        return order
    except grpc.RpcError as exc:
        raise _grpc_http_error(exc) from exc
    except (SQLAlchemyError, ValueError) as exc:
        raise HTTPException(status_code=503, detail="execution_audit_write_failed") from exc


@app.get("/api/v1/trading/orders", response_model=ExecutionTicket)
def latest_trading_order(session_id: str | None = None) -> ExecutionTicket:
    engine = _require_execution_engine()
    try:
        ticket = latest_execution_ticket(engine, session_id=session_id)
    except (SQLAlchemyError, ValueError) as exc:
        raise HTTPException(status_code=503, detail="execution_audit_read_failed") from exc
    if ticket is None:
        raise HTTPException(status_code=404, detail="order_not_found")
    plan, order = ticket
    return ExecutionTicket(plan=plan, order=order)


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
