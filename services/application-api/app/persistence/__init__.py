"""Persistence layer: write signals + No-Trade reasons to PostgreSQL (P1-7).

Serializes regime/vol/strategy engine outputs into the ``trading.signals`` and
``audit.audit_events`` tables (owned by the Alembic migrations) via a single
transactional, idempotent write. Pure serialization is separated from I/O so
the row shapes are testable without a database.
"""

from app.persistence.repository import (
    claim_confirmation_intent,
    latest_order_projection,
    latest_execution_ticket,
    persist_event_context,
    persist_order_projection,
    persist_signal,
    persist_staged_candidate,
    staged_plan_projection,
)
from app.persistence.serialize import SignalContext, build_signal_contract, build_signal_rows
from app.persistence.tables import (
    audit_events,
    candidate_trade_plans,
    confirmation_capabilities,
    event_contexts,
    metadata,
    order_events,
    orders,
    risk_decisions,
    signals,
)

__all__ = [
    "SignalContext",
    "audit_events",
    "candidate_trade_plans",
    "claim_confirmation_intent",
    "confirmation_capabilities",
    "build_signal_contract",
    "build_signal_rows",
    "event_contexts",
    "metadata",
    "latest_order_projection",
    "latest_execution_ticket",
    "order_events",
    "orders",
    "persist_event_context",
    "persist_order_projection",
    "persist_signal",
    "persist_staged_candidate",
    "risk_decisions",
    "staged_plan_projection",
    "signals",
]
