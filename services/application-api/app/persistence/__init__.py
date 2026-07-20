"""Persistence layer: write signals + No-Trade reasons to PostgreSQL (P1-7).

Serializes regime/vol/strategy engine outputs into the ``trading.signals`` and
``audit.audit_events`` tables (owned by the Alembic migrations) via a single
transactional, idempotent write. Pure serialization is separated from I/O so
the row shapes are testable without a database.
"""

from app.persistence.repository import persist_signal
from app.persistence.serialize import SignalContext, build_signal_contract, build_signal_rows
from app.persistence.tables import audit_events, metadata, signals

__all__ = [
    "SignalContext",
    "audit_events",
    "build_signal_contract",
    "build_signal_rows",
    "metadata",
    "persist_signal",
    "signals",
]
