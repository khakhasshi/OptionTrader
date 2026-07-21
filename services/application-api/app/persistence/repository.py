"""Transactional write-path for signals + audit events (P1-7).

One public call, :func:`persist_signal`, writes the ``trading.signals`` row and
its ``audit.audit_events`` row inside a single transaction: either both land or
neither does, so a signal can never exist without its audit trail (DEVELOPMENT_
PLAN §7 — signal/audit writes share a transaction).

Re-persisting the same ``signal_id`` is idempotent: the insert is skipped if the
row already exists (replay re-runs must not duplicate or error).
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from hashlib import sha256
from typing import Any

from sqlalchemy import insert, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine

from app.persistence.serialize import SignalContext, build_signal_rows
from app.persistence.tables import (
    audit_events,
    candidate_trade_plans,
    event_contexts,
    order_events,
    orders,
    risk_decisions,
    signals,
)
from app.events import EventContext
from app.regime import RegimeState
from app.strategy import StrategyDecision
from app.vol import VolState
from app.trading.models import CandidateTradePlan, ExecutionOrder, StageCandidateResult


def _now_utc() -> datetime:
    """created_at_utc stamp. Isolated so callers/tests can reason about it;
    occurred_at_utc (the decision instant) always comes from the caller."""
    return datetime.now(timezone.utc)


def persist_signal(
    engine: Engine,
    ctx: SignalContext,
    regime: RegimeState,
    vol: VolState,
    decision: StrategyDecision,
) -> bool:
    """Persist one signal + its audit event transactionally.

    Returns ``True`` if rows were written, ``False`` if this ``signal_id`` was
    already present (idempotent skip). Raises on serialization or DB errors —
    the caller decides whether a persistence failure should halt the pipeline.
    """
    signal_row, audit_row = build_signal_rows(ctx, regime, vol, decision)
    created = _now_utc()
    signal_row["created_at_utc"] = created
    audit_row["created_at_utc"] = created

    with engine.begin() as conn:
        if conn.dialect.name == "postgresql":
            inserted = (
                conn.execute(
                    postgresql_insert(signals)
                    .values(**signal_row)
                    .on_conflict_do_nothing(index_elements=[signals.c.signal_id])
                    .returning(signals.c.signal_id)
                ).scalar_one_or_none()
                is not None
            )
        elif conn.dialect.name == "sqlite":
            inserted = (
                conn.execute(
                    sqlite_insert(signals)
                    .values(**signal_row)
                    .on_conflict_do_nothing(index_elements=[signals.c.signal_id])
                ).rowcount
                == 1
            )
        else:
            existing = conn.execute(
                select(signals.c.signal_id).where(signals.c.signal_id == ctx.signal_id)
            ).first()
            if existing is not None:
                return False
            conn.execute(insert(signals).values(**signal_row))
            inserted = True
        if not inserted:
            return False
        conn.execute(audit_events.insert().values(**audit_row))
    return True


def persist_event_context(engine: Engine, session_id: str, context: EventContext) -> bool:
    """Persist EventContext + immutable audit event in one idempotent transaction."""
    occurred = datetime.fromisoformat(context.generated_at_utc.replace("Z", "+00:00"))
    created = _now_utc()
    payload: dict[str, Any] = context.model_dump(mode="json")
    row = {
        "event_id": context.event_context_id,
        "session_id": session_id,
        "trading_date": date.fromisoformat(context.trading_date),
        "category": context.event_day_type,
        "occurred_at_utc": occurred,
        "source": "event-context-layer",
        "payload": payload,
        "created_at_utc": created,
    }
    audit = {
        "event_id": f"audit_{context.event_context_id}",
        "session_id": session_id,
        "occurred_at_utc": occurred,
        "actor": "application-service",
        "action": "EVENT_CONTEXT_BUILT",
        "entity_type": "EventContext",
        "entity_id": context.event_context_id,
        "from_status": None,
        "to_status": "AVAILABLE" if context.available else "UNAVAILABLE",
        "payload": payload,
        "created_at_utc": created,
    }

    with engine.begin() as conn:
        if conn.dialect.name == "postgresql":
            inserted = (
                conn.execute(
                    postgresql_insert(event_contexts)
                    .values(**row)
                    .on_conflict_do_nothing(index_elements=[event_contexts.c.event_id])
                    .returning(event_contexts.c.event_id)
                ).scalar_one_or_none()
                is not None
            )
        elif conn.dialect.name == "sqlite":
            inserted = (
                conn.execute(
                    sqlite_insert(event_contexts)
                    .values(**row)
                    .on_conflict_do_nothing(index_elements=[event_contexts.c.event_id])
                ).rowcount
                == 1
            )
        else:
            existing = conn.execute(
                select(event_contexts.c.event_id).where(
                    event_contexts.c.event_id == context.event_context_id
                )
            ).first()
            if existing is not None:
                return False
            conn.execute(insert(event_contexts).values(**row))
            inserted = True
        if not inserted:
            return False
        conn.execute(audit_events.insert().values(**audit))
    return True


def persist_staged_candidate(
    engine: Engine,
    plan: CandidateTradePlan,
    result: StageCandidateResult,
) -> bool:
    """Persist plan, initial Rust risk, optional order and audit atomically.

    The opaque confirmation token is deliberately never serialized here.
    Repeating an identical deterministic plan is an idempotent no-op.
    """
    decision = result.initial_risk_decision
    if decision.plan_id != plan.plan_id or decision.plan_hash != plan.plan_hash:
        raise ValueError("risk decision does not match candidate plan")
    if result.order is not None and (
        result.order.plan_id != plan.plan_id or result.order.plan_hash != plan.plan_hash
    ):
        raise ValueError("execution order does not match candidate plan")
    occurred = datetime.fromisoformat(decision.occurred_at_utc.replace("Z", "+00:00"))
    created = _now_utc()
    plan_payload = plan.model_dump(mode="json", exclude_none=True)
    decision_payload = decision.model_dump(mode="json")
    status = result.order.state if result.order is not None else "RISK_REJECTED"

    with engine.begin() as conn:
        existing = conn.execute(
            select(candidate_trade_plans.c.payload).where(
                candidate_trade_plans.c.plan_id == plan.plan_id
            )
        ).scalar_one_or_none()
        if existing is not None:
            if existing != plan_payload:
                raise ValueError("plan_id already exists with different payload")
            return False
        conn.execute(
            candidate_trade_plans.insert().values(
                plan_id=plan.plan_id,
                signal_id=plan.signal_id,
                session_id=plan.session_id,
                status=status,
                strategy_kind=plan.strategy,
                plan_hash=plan.plan_hash,
                idempotency_key=plan.idempotency_key,
                execution_mode=plan.execution_mode,
                expires_at_utc=datetime.fromisoformat(plan.expires_at_utc.replace("Z", "+00:00")),
                created_at_utc=datetime.fromisoformat(plan.created_at_utc.replace("Z", "+00:00")),
                payload=plan_payload,
            )
        )
        conn.execute(
            risk_decisions.insert().values(
                plan_id=plan.plan_id,
                session_id=plan.session_id,
                occurred_at_utc=occurred,
                decision=decision.decision,
                reason_code=",".join(decision.reason_codes) or None,
                payload=decision_payload,
                created_at_utc=created,
            )
        )
        conn.execute(
            audit_events.insert().values(
                event_id=f"audit_{decision.decision_id}",
                session_id=plan.session_id,
                occurred_at_utc=occurred,
                actor="rust-risk-gateway",
                action="CANDIDATE_STAGED",
                entity_type="CandidateTradePlan",
                entity_id=plan.plan_id,
                from_status=None,
                to_status=status,
                payload={"plan_hash": plan.plan_hash, "decision": decision_payload},
                created_at_utc=created,
            )
        )
        if result.order is not None:
            order = result.order
            conn.execute(
                orders.insert().values(
                    order_id=order.order_id,
                    plan_id=order.plan_id,
                    session_id=order.session_id,
                    idempotency_key=order.idempotency_key,
                    status=order.state,
                    side="COMBO",
                    quantity=order.total_quantity,
                    filled_quantity=order.filled_quantity,
                    limit_price=plan.limit_price,
                    broker_order_id=order.broker_order_id,
                    payload=order.model_dump(mode="json"),
                    created_at_utc=created,
                    updated_at_utc=datetime.fromisoformat(
                        order.updated_at_utc.replace("Z", "+00:00")
                    ),
                )
            )
            conn.execute(
                order_events.insert().values(
                    order_id=order.order_id,
                    occurred_at_utc=occurred,
                    event_type="CANDIDATE_STAGED",
                    from_status=None,
                    to_status=order.state,
                    payload=order.model_dump(mode="json"),
                    created_at_utc=created,
                )
            )
    return True


def persist_confirmation_intent(
    engine: Engine,
    order_id: str,
    plan_hash: str,
    actor: str,
) -> bool:
    """Durably record operator intent before crossing the Rust submit boundary."""
    if not actor or not order_id or len(plan_hash) != 64:
        raise ValueError("confirmation intent fields are invalid")
    event_id = f"confirm_{order_id}_{plan_hash[:16]}"
    occurred = _now_utc()
    with engine.begin() as conn:
        order = conn.execute(select(orders).where(orders.c.order_id == order_id)).mappings().one()
        if order["status"] != "AWAITING_CONFIRMATION":
            return False
        existing = conn.execute(
            select(audit_events.c.event_id).where(audit_events.c.event_id == event_id)
        ).first()
        if existing is not None:
            return False
        conn.execute(
            audit_events.insert().values(
                event_id=event_id,
                session_id=order["session_id"],
                occurred_at_utc=occurred,
                actor=actor,
                action="CONFIRMATION_REQUESTED",
                entity_type="ExecutionOrder",
                entity_id=order_id,
                from_status="AWAITING_CONFIRMATION",
                to_status="CONFIRMATION_PENDING",
                payload={"plan_hash": plan_hash},
                created_at_utc=occurred,
            )
        )
    return True


def persist_order_projection(
    engine: Engine,
    order: ExecutionOrder,
    *,
    action: str,
    actor: str,
) -> bool:
    """Update one Rust order projection and append matching order/audit events."""
    occurred = datetime.fromisoformat(order.updated_at_utc.replace("Z", "+00:00"))
    created = _now_utc()
    payload = order.model_dump(mode="json")
    with engine.begin() as conn:
        current = (
            conn.execute(select(orders).where(orders.c.order_id == order.order_id)).mappings().one()
        )
        if (
            current["plan_id"] != order.plan_id
            or current["idempotency_key"] != order.idempotency_key
        ):
            raise ValueError("Rust order projection conflicts with persisted identity")
        current_order = ExecutionOrder.model_validate(current["payload"])
        if order.state_version < current_order.state_version:
            return False
        if order.state_version == current_order.state_version:
            current_content = current_order.model_dump(mode="json", exclude={"updated_at_utc"})
            incoming_content = order.model_dump(mode="json", exclude={"updated_at_utc"})
            if current_content != incoming_content:
                raise ValueError("Rust reused an order state_version with conflicting content")
            return False
        if order.filled_quantity < current_order.filled_quantity:
            raise ValueError("Rust order projection reduced filled quantity")
        from_status = str(current["status"])
        conn.execute(
            update(orders)
            .where(orders.c.order_id == order.order_id)
            .values(
                status=order.state,
                filled_quantity=order.filled_quantity,
                broker_order_id=order.broker_order_id,
                payload=payload,
                updated_at_utc=occurred,
            )
        )
        conn.execute(
            update(candidate_trade_plans)
            .where(candidate_trade_plans.c.plan_id == order.plan_id)
            .values(status=order.state)
        )
        conn.execute(
            order_events.insert().values(
                order_id=order.order_id,
                occurred_at_utc=occurred,
                event_type=action,
                from_status=from_status,
                to_status=order.state,
                payload=payload,
                created_at_utc=created,
            )
        )
        conn.execute(
            audit_events.insert().values(
                event_id="audit_"
                + sha256(
                    f"{order.order_id}|{from_status}|{order.state}|{action}|{order.state_version}|{order.updated_at_utc}".encode()
                ).hexdigest(),
                session_id=order.session_id,
                occurred_at_utc=occurred,
                actor=actor,
                action=action,
                entity_type="ExecutionOrder",
                entity_id=order.order_id,
                from_status=from_status,
                to_status=order.state,
                payload=payload,
                created_at_utc=created,
            )
        )
    return True


def latest_order_projection(
    engine: Engine, *, session_id: str | None = None
) -> ExecutionOrder | None:
    """Read the newest durable Rust projection for cockpit recovery."""
    query = select(order_events.c.payload).order_by(
        order_events.c.occurred_at_utc.desc(), order_events.c.id.desc()
    )
    if session_id is not None:
        query = query.join(orders, orders.c.order_id == order_events.c.order_id).where(
            orders.c.session_id == session_id
        )
    with engine.connect() as conn:
        payload = conn.execute(query.limit(1)).scalar_one_or_none()
    return ExecutionOrder.model_validate(payload) if payload is not None else None


def latest_execution_ticket(
    engine: Engine, *, session_id: str | None = None
) -> tuple[CandidateTradePlan, ExecutionOrder] | None:
    """Return the newest plan plus Rust order projection for operator review."""
    query = (
        select(candidate_trade_plans.c.payload, orders.c.payload)
        .join(orders, orders.c.plan_id == candidate_trade_plans.c.plan_id)
        .where(orders.c.payload.is_not(None))
        .order_by(orders.c.updated_at_utc.desc())
    )
    if session_id is not None:
        query = query.where(orders.c.session_id == session_id)
    with engine.connect() as conn:
        row = conn.execute(query.limit(1)).one_or_none()
    if row is None:
        return None
    return CandidateTradePlan.model_validate(row[0]), ExecutionOrder.model_validate(row[1])


def staged_plan_projection(
    engine: Engine, plan_id: str
) -> tuple[str, ExecutionOrder | None] | None:
    """Return durable plan status/order before any attempt to restage it."""
    with engine.connect() as conn:
        status = conn.execute(
            select(candidate_trade_plans.c.status).where(candidate_trade_plans.c.plan_id == plan_id)
        ).scalar_one_or_none()
        if status is None:
            return None
        payload = conn.execute(
            select(orders.c.payload).where(orders.c.plan_id == plan_id)
        ).scalar_one_or_none()
    order = ExecutionOrder.model_validate(payload) if payload is not None else None
    return str(status), order


__all__ = [
    "persist_confirmation_intent",
    "persist_event_context",
    "persist_order_projection",
    "persist_signal",
    "persist_staged_candidate",
    "latest_order_projection",
    "latest_execution_ticket",
    "staged_plan_projection",
]
