"""Typed Python client for Rust Final Risk Check."""

from __future__ import annotations

import os
from hashlib import sha256
from typing import Any

import grpc

from app.events import EventContext
from app.grpc_gen import execution_pb2, execution_pb2_grpc
from app.trading.candidate import plan_proto
from app.trading.models import (
    CandidateTradePlan,
    ExecutionOrder,
    RiskDecision,
    StageCandidateResult,
)

TRADING_CORE_GRPC = os.getenv("TRADING_CORE_GRPC", "localhost:50051")

_FLAG_TO_PROTO = {
    "NO_SHORT_PREMIUM_BEFORE_EVENT": execution_pb2.EVENT_RISK_FLAG_NO_SHORT_PREMIUM_BEFORE_EVENT,
    "SIZE_HALF": execution_pb2.EVENT_RISK_FLAG_SIZE_HALF,
    "WAIT_AFTER_RELEASE": execution_pb2.EVENT_RISK_FLAG_WAIT_AFTER_RELEASE,
    "ELEVATED_EVENT_RISK": execution_pb2.EVENT_RISK_FLAG_ELEVATED_EVENT_RISK,
    "NO_NAKED_0DTE": execution_pb2.EVENT_RISK_FLAG_NO_NAKED_0DTE,
}

_REASON_NAME: dict[int, str] = {
    int(execution_pb2.RISK_REASON_CODE_DATA_NOT_HEALTHY): "DATA_NOT_HEALTHY",
    int(execution_pb2.RISK_REASON_CODE_BROKER_NOT_HEALTHY): "BROKER_NOT_HEALTHY",
    int(execution_pb2.RISK_REASON_CODE_BROKER_NOT_RECONCILED): "BROKER_NOT_RECONCILED",
    int(execution_pb2.RISK_REASON_CODE_EVENT_CONTEXT_UNAVAILABLE): "EVENT_CONTEXT_UNAVAILABLE",
    int(execution_pb2.RISK_REASON_CODE_EVENT_CONTEXT_INVALID): "EVENT_CONTEXT_INVALID",
    int(execution_pb2.RISK_REASON_CODE_EVENT_POLICY_BLOCK): "EVENT_POLICY_BLOCK",
    int(execution_pb2.RISK_REASON_CODE_PLAN_EXPIRED): "PLAN_EXPIRED",
    int(execution_pb2.RISK_REASON_CODE_PLAN_INVALID): "PLAN_INVALID",
    int(execution_pb2.RISK_REASON_CODE_PLAN_HASH_MISMATCH): "PLAN_HASH_MISMATCH",
    int(execution_pb2.RISK_REASON_CODE_SNAPSHOT_NOT_CURRENT): "SNAPSHOT_NOT_CURRENT",
    int(execution_pb2.RISK_REASON_CODE_EXECUTION_MODE_BLOCKED): "EXECUTION_MODE_BLOCKED",
    int(execution_pb2.RISK_REASON_CODE_DUPLICATE_CONFLICT): "DUPLICATE_CONFLICT",
    int(execution_pb2.RISK_REASON_CODE_RISK_LIMITS_UNCONFIRMED): "RISK_LIMITS_UNCONFIRMED",
    int(execution_pb2.RISK_REASON_CODE_KILL_SWITCH_ACTIVE): "KILL_SWITCH_ACTIVE",
    int(execution_pb2.RISK_REASON_CODE_DAILY_LOSS_LIMIT): "DAILY_LOSS_LIMIT",
    int(execution_pb2.RISK_REASON_CODE_MAX_TRADES_REACHED): "MAX_TRADES_REACHED",
    int(execution_pb2.RISK_REASON_CODE_LOSS_COOLDOWN_ACTIVE): "LOSS_COOLDOWN_ACTIVE",
    int(execution_pb2.RISK_REASON_CODE_PLAN_RISK_LIMIT): "PLAN_RISK_LIMIT",
    int(execution_pb2.RISK_REASON_CODE_OPEN_RISK_LIMIT): "OPEN_RISK_LIMIT",
    int(execution_pb2.RISK_REASON_CODE_BUYING_POWER_INSUFFICIENT): "BUYING_POWER_INSUFFICIENT",
    int(execution_pb2.RISK_REASON_CODE_MAX_CONTRACTS_EXCEEDED): "MAX_CONTRACTS_EXCEEDED",
    int(execution_pb2.RISK_REASON_CODE_RULE_VERSION_MISMATCH): "RULE_VERSION_MISMATCH",
    int(execution_pb2.RISK_REASON_CODE_QUOTE_PROOF_INVALID): "QUOTE_PROOF_INVALID",
    int(execution_pb2.RISK_REASON_CODE_QUOTE_STALE): "QUOTE_STALE",
    int(execution_pb2.RISK_REASON_CODE_QUOTE_SPREAD_TOO_WIDE): "QUOTE_SPREAD_TOO_WIDE",
    int(execution_pb2.RISK_REASON_CODE_GREEKS_INVALID): "GREEKS_INVALID",
    int(execution_pb2.RISK_REASON_CODE_CHAIN_SNAPSHOT_MISMATCH): "CHAIN_SNAPSHOT_MISMATCH",
    int(execution_pb2.RISK_REASON_CODE_STRATEGY_NOT_ALLOWED): "STRATEGY_NOT_ALLOWED",
    int(execution_pb2.RISK_REASON_CODE_ENTRY_WINDOW_CLOSED): "ENTRY_WINDOW_CLOSED",
    int(execution_pb2.RISK_REASON_CODE_MARKET_ORDER_BLOCKED): "MARKET_ORDER_BLOCKED",
}

_BROKER_NAME: dict[int, str] = {
    int(execution_pb2.BROKER_ID_LONGBRIDGE): "longbridge",
    int(execution_pb2.BROKER_ID_IBKR): "ibkr",
}
_MODE_NAME: dict[int, str] = {
    int(execution_pb2.EXECUTION_MODE_REPLAY): "REPLAY",
    int(execution_pb2.EXECUTION_MODE_SHADOW): "SHADOW",
    int(execution_pb2.EXECUTION_MODE_PAPER): "PAPER",
    int(execution_pb2.EXECUTION_MODE_MANUAL_CONFIRM): "MANUAL_CONFIRM",
    int(execution_pb2.EXECUTION_MODE_CONTROLLED_AUTO): "CONTROLLED_AUTO",
}
_ORDER_STATE_NAME: dict[int, str] = {
    int(execution_pb2.EXECUTION_ORDER_STATE_AWAITING_CONFIRMATION): "AWAITING_CONFIRMATION",
    int(execution_pb2.EXECUTION_ORDER_STATE_RISK_REJECTED): "RISK_REJECTED",
    int(execution_pb2.EXECUTION_ORDER_STATE_APPROVED): "APPROVED",
    int(execution_pb2.EXECUTION_ORDER_STATE_SUBMITTING): "SUBMITTING",
    int(execution_pb2.EXECUTION_ORDER_STATE_WORKING): "WORKING",
    int(execution_pb2.EXECUTION_ORDER_STATE_PARTIAL_FILL): "PARTIAL_FILL",
    int(execution_pb2.EXECUTION_ORDER_STATE_FILLED): "FILLED",
    int(execution_pb2.EXECUTION_ORDER_STATE_CANCEL_PENDING): "CANCEL_PENDING",
    int(execution_pb2.EXECUTION_ORDER_STATE_CANCELLED): "CANCELLED",
    int(execution_pb2.EXECUTION_ORDER_STATE_REJECTED): "REJECTED",
    int(execution_pb2.EXECUTION_ORDER_STATE_EXPIRED): "EXPIRED",
    int(execution_pb2.EXECUTION_ORDER_STATE_RECONCILE_PENDING): "RECONCILE_PENDING",
    int(execution_pb2.EXECUTION_ORDER_STATE_SHADOWED): "SHADOWED",
}


def event_context_proto(context: EventContext) -> execution_pb2.EventRiskContext:
    try:
        flags = [_FLAG_TO_PROTO[flag] for flag in context.risk_flags]
    except KeyError as exc:
        raise ValueError(f"unmapped EventContext risk flag: {exc.args[0]}") from exc
    proto = execution_pb2.EventRiskContext(
        event_context_id=context.event_context_id,
        trading_date=context.trading_date,
        generated_at_utc=context.generated_at_utc,
        available=context.available,
        source_documents=[
            execution_pb2.EventSourceProof(
                category=document.category,
                source_timestamp_utc=document.source_timestamp_utc,
                received_at_utc=document.received_at_utc,
                confidence=document.confidence,
                raw_ref=document.raw_ref,
            )
            for document in context.source_documents
        ],
        risk_flags=flags,
        event_released=context.event_released,
        context_hash="",
    )
    if context.minutes_to_major_event is not None:
        proto.minutes_to_major_event = context.minutes_to_major_event
    digest = sha256(proto.SerializeToString(deterministic=True)).hexdigest()
    proto.context_hash = digest
    return proto


def decision_from_proto(raw: Any) -> RiskDecision:
    decision_names: dict[int, str] = {
        int(execution_pb2.RISK_DECISION_KIND_APPROVED): "APPROVED",
        int(execution_pb2.RISK_DECISION_KIND_REJECTED): "REJECTED",
    }
    decision = decision_names.get(int(raw.decision))
    if decision is None:
        raise ValueError("Rust returned an unspecified risk decision")
    try:
        reasons = [_REASON_NAME[int(reason)] for reason in raw.reason_codes]
    except KeyError as exc:
        raise ValueError(f"Rust returned an unknown risk reason: {exc.args[0]}") from exc
    return RiskDecision.model_validate(
        {
            "schema_version": raw.schema_version,
            "decision_id": raw.decision_id,
            "plan_id": raw.plan_id,
            "plan_hash": raw.plan_hash,
            "session_id": raw.session_id,
            "occurred_at_utc": raw.occurred_at_utc,
            "decision": decision,
            "reason_codes": reasons,
            "manual_confirmation_required": raw.manual_confirmation_required,
            "rule_version": raw.rule_version,
        }
    )


def order_from_proto(raw: Any) -> ExecutionOrder:
    try:
        broker = _BROKER_NAME[int(raw.broker_id)]
        mode = _MODE_NAME[int(raw.execution_mode)]
        state = _ORDER_STATE_NAME[int(raw.state)]
        reasons = [_REASON_NAME[int(reason)] for reason in raw.risk_reason_codes]
    except KeyError as exc:
        raise ValueError(f"Rust returned an unknown execution enum: {exc.args[0]}") from exc
    return ExecutionOrder.model_validate(
        {
            "schema_version": raw.schema_version,
            "order_id": raw.order_id,
            "plan_id": raw.plan_id,
            "plan_hash": raw.plan_hash,
            "idempotency_key": raw.idempotency_key,
            "session_id": raw.session_id,
            "broker_id": broker,
            "execution_mode": mode,
            "state": state,
            "total_quantity": raw.total_quantity,
            "filled_quantity": raw.filled_quantity,
            "broker_order_id": raw.broker_order_id or None,
            "expires_at_utc": raw.expires_at_utc,
            "updated_at_utc": raw.updated_at_utc,
            "state_version": raw.state_version,
            "broker_child_order_ids": list(raw.broker_child_order_ids),
            "residual_exposure": raw.residual_exposure,
            "risk_reason_codes": reasons,
        }
    )


def evaluate_candidate(
    plan: CandidateTradePlan,
    event_context: EventContext,
    *,
    target: str | None = None,
) -> RiskDecision:
    channel = grpc.insecure_channel(target or TRADING_CORE_GRPC)
    try:
        stub = execution_pb2_grpc.RiskExecutionServiceStub(channel)
        raw = stub.EvaluateCandidate(
            execution_pb2.EvaluateCandidateRequest(
                plan=plan_proto(plan), event_context=event_context_proto(event_context)
            )
        )
        return decision_from_proto(raw)
    finally:
        channel.close()


def stage_candidate(
    plan: CandidateTradePlan,
    event_context: EventContext,
    *,
    target: str | None = None,
) -> StageCandidateResult:
    channel = grpc.insecure_channel(target or TRADING_CORE_GRPC)
    try:
        stub = execution_pb2_grpc.RiskExecutionServiceStub(channel)
        raw = stub.StageCandidate(
            execution_pb2.EvaluateCandidateRequest(
                plan=plan_proto(plan), event_context=event_context_proto(event_context)
            )
        )
        if not raw.HasField("initial_risk_decision"):
            raise ValueError("Rust omitted initial risk decision")
        order = order_from_proto(raw.order) if raw.HasField("order") else None
        if order is not None and (
            order.plan_id != plan.plan_id
            or order.plan_hash != plan.plan_hash
            or order.idempotency_key != plan.idempotency_key
            or order.session_id != plan.session_id
            or order.broker_id != plan.broker_id
            or order.execution_mode != plan.execution_mode
            or order.total_quantity != plan.legs[0].quantity
        ):
            raise ValueError("Rust order does not match the staged candidate plan")
        return StageCandidateResult(
            initial_risk_decision=decision_from_proto(raw.initial_risk_decision),
            order=order,
            confirmation_token=raw.confirmation_token,
        )
    finally:
        channel.close()


def confirm_candidate(
    order_id: str,
    plan_hash: str,
    confirmation_token: str,
    event_context: EventContext,
    *,
    target: str | None = None,
) -> ExecutionOrder:
    channel = grpc.insecure_channel(target or TRADING_CORE_GRPC)
    try:
        stub = execution_pb2_grpc.RiskExecutionServiceStub(channel)
        raw = stub.ConfirmCandidate(
            execution_pb2.ConfirmCandidateRequest(
                order_id=order_id,
                confirmed_plan_hash=plan_hash,
                confirmation_token=confirmation_token,
                event_context=event_context_proto(event_context),
            )
        )
        return order_from_proto(raw)
    finally:
        channel.close()


def cancel_order(order_id: str, *, target: str | None = None) -> ExecutionOrder:
    channel = grpc.insecure_channel(target or TRADING_CORE_GRPC)
    try:
        stub = execution_pb2_grpc.RiskExecutionServiceStub(channel)
        return order_from_proto(
            stub.CancelOrder(execution_pb2.CancelOrderRequest(order_id=order_id))
        )
    finally:
        channel.close()


def get_order(order_id: str, *, target: str | None = None) -> ExecutionOrder:
    channel = grpc.insecure_channel(target or TRADING_CORE_GRPC)
    try:
        stub = execution_pb2_grpc.RiskExecutionServiceStub(channel)
        return order_from_proto(stub.GetOrder(execution_pb2.GetOrderRequest(order_id=order_id)))
    finally:
        channel.close()


def event_context_hash(context: EventContext) -> str:
    return event_context_proto(context).context_hash


__all__ = [
    "cancel_order",
    "confirm_candidate",
    "evaluate_candidate",
    "event_context_hash",
    "event_context_proto",
    "get_order",
    "order_from_proto",
    "stage_candidate",
]
