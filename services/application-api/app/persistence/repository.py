"""Transactional write-path for signals + audit events (P1-7).

One public call, :func:`persist_signal`, writes the ``trading.signals`` row and
its ``audit.audit_events`` row inside a single transaction: either both land or
neither does, so a signal can never exist without its audit trail (DEVELOPMENT_
PLAN §7 — signal/audit writes share a transaction).

Re-persisting the same ``signal_id`` is idempotent: the insert is skipped if the
row already exists (replay re-runs must not duplicate or error).
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.engine import Engine

from app.persistence.serialize import SignalContext, build_signal_rows
from app.persistence.tables import audit_events, signals
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
        existing = conn.execute(
            select(signals.c.signal_id).where(signals.c.signal_id == ctx.signal_id)
        ).first()
        if existing is not None:
            return False
        conn.execute(signals.insert().values(**signal_row))
        conn.execute(audit_events.insert().values(**audit_row))
    return True


__all__ = ["persist_signal"]
