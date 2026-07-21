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
from typing import Any

from sqlalchemy import insert, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine

from app.persistence.serialize import SignalContext, build_signal_rows
from app.persistence.tables import audit_events, event_contexts, signals
from app.events import EventContext
from app.regime import RegimeState
from app.strategy import StrategyDecision
from app.vol import VolState


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


__all__ = ["persist_event_context", "persist_signal"]
