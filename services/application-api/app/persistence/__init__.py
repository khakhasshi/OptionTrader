"""Persistence layer: write signals + No-Trade reasons to PostgreSQL (P1-7).

Serializes regime/vol/strategy engine outputs into the ``trading.signals`` and
``audit.audit_events`` tables (owned by the Alembic migrations) via a single
transactional, idempotent write. Pure serialization is separated from I/O so
the row shapes are testable without a database.
"""

from app.persistence.repository import (
    OutboxMessage,
    claim_outbox_batch,
    claim_confirmation_intent,
    latest_execution_ticket,
    latest_order_projection,
    mark_outbox_published,
    pending_reconciliation_orders,
    persist_broker_reconciliation,
    persist_broker_reconciliation_failure,
    persist_event_context,
    persist_order_projection,
    persist_signal,
    persist_staged_candidate,
    restorable_execution_workflow,
    reschedule_outbox_message,
    rotate_confirmation_capabilities,
    staged_plan_projection,
)
from app.persistence.serialize import SignalContext, build_signal_contract, build_signal_rows
from app.persistence.tables import (
    audit_events,
    broker_snapshots,
    candidate_trade_plans,
    confirmation_capabilities,
    event_contexts,
    fills,
    metadata,
    order_events,
    outbox_events,
    orders,
    position_snapshots,
    risk_decisions,
    signals,
)

__all__ = [
    "OutboxMessage",
    "SignalContext",
    "audit_events",
    "broker_snapshots",
    "candidate_trade_plans",
    "claim_outbox_batch",
    "claim_confirmation_intent",
    "confirmation_capabilities",
    "build_signal_contract",
    "build_signal_rows",
    "event_contexts",
    "fills",
    "metadata",
    "latest_order_projection",
    "latest_execution_ticket",
    "mark_outbox_published",
    "order_events",
    "outbox_events",
    "orders",
    "pending_reconciliation_orders",
    "position_snapshots",
    "persist_broker_reconciliation",
    "persist_broker_reconciliation_failure",
    "persist_event_context",
    "persist_order_projection",
    "persist_signal",
    "persist_staged_candidate",
    "reschedule_outbox_message",
    "restorable_execution_workflow",
    "rotate_confirmation_capabilities",
    "risk_decisions",
    "staged_plan_projection",
    "signals",
]
