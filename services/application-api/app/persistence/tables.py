"""SQLAlchemy Core table definitions for the signal/audit write-path (P1-7).

These mirror — do not own — the DDL in ``migrations/versions/0001_initial_schema``.
Migrations remain the single source of truth for the schema (project convention:
pure-SQL migrations, no ORM autogenerate); these Core tables exist only so the
persistence layer can build type-safe, parameterized INSERTs. If a column here
drifts from the migration, the insert fails loudly rather than writing garbage.
"""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Integer,
    MetaData,
    Numeric,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import JSON

metadata = MetaData()

# Use JSONB on Postgres, plain JSON elsewhere (SQLite in tests). variant() keeps
# one definition working across both without a second table.
_JSON = JSONB().with_variant(JSON(), "sqlite")
_BIGINT = BigInteger().with_variant(Integer(), "sqlite")

# trading.signals — one row per engine tick: the selected strategy (or No Trade)
# with the regime/vol context and the No-Trade reason for review.
signals = Table(
    "signals",
    metadata,
    Column("signal_id", Text, primary_key=True),
    Column("session_id", Text, nullable=False),
    Column("occurred_at_utc", DateTime(timezone=True), nullable=False),
    Column("regime", Text),
    Column("vol_state", Text),
    Column("strategy_kind", Text, nullable=False),
    Column("no_trade_reason", Text),
    Column("payload", _JSON),
    Column("created_at_utc", DateTime(timezone=True)),
    schema="trading",
)

# audit.audit_events — immutable audit trail. Every persisted signal also emits
# one audit event so the review layer has a uniform, append-only record.
audit_events = Table(
    "audit_events",
    metadata,
    Column("event_id", Text, primary_key=True),
    Column("session_id", Text),
    Column("occurred_at_utc", DateTime(timezone=True), nullable=False),
    Column("actor", Text, nullable=False),
    Column("action", Text, nullable=False),
    Column("entity_type", Text),
    Column("entity_id", Text),
    Column("from_status", Text),
    Column("to_status", Text),
    Column("payload", _JSON),
    Column("created_at_utc", DateTime(timezone=True)),
    schema="audit",
)

# events.event_contexts — sourced event records and deterministic daily context.
event_contexts = Table(
    "event_contexts",
    metadata,
    Column("event_id", Text, primary_key=True),
    Column("session_id", Text),
    Column("trading_date", Date, nullable=False),
    Column("category", Text, nullable=False),
    Column("occurred_at_utc", DateTime(timezone=True), nullable=False),
    Column("source", Text, nullable=False),
    Column("payload", _JSON, nullable=False),
    Column("created_at_utc", DateTime(timezone=True)),
    schema="events",
)

candidate_trade_plans = Table(
    "candidate_trade_plans",
    metadata,
    Column("plan_id", Text, primary_key=True),
    Column("signal_id", Text, nullable=False),
    Column("session_id", Text, nullable=False),
    Column("status", Text, nullable=False),
    Column("strategy_kind", Text, nullable=False),
    Column("plan_hash", Text, nullable=False, unique=True),
    Column("idempotency_key", Text, nullable=False, unique=True),
    Column("execution_mode", Text, nullable=False),
    Column("expires_at_utc", DateTime(timezone=True), nullable=False),
    Column("created_at_utc", DateTime(timezone=True)),
    Column("payload", _JSON, nullable=False),
    schema="trading",
)

orders = Table(
    "orders",
    metadata,
    Column("order_id", Text, primary_key=True),
    Column("plan_id", Text, nullable=False),
    Column("session_id", Text, nullable=False),
    Column("idempotency_key", Text, nullable=False),
    Column("status", Text, nullable=False),
    Column("side", Text, nullable=False),
    Column("quantity", Numeric, nullable=False),
    Column("filled_quantity", Numeric, nullable=False),
    Column("limit_price", Numeric),
    Column("broker_order_id", Text, unique=True),
    Column("payload", _JSON),
    Column("created_at_utc", DateTime(timezone=True)),
    Column("updated_at_utc", DateTime(timezone=True)),
    UniqueConstraint("idempotency_key"),
    schema="trading",
)

order_events = Table(
    "order_events",
    metadata,
    Column("id", _BIGINT, primary_key=True, autoincrement=True),
    Column("order_id", Text, nullable=False),
    Column("occurred_at_utc", DateTime(timezone=True), nullable=False),
    Column("event_type", Text, nullable=False),
    Column("from_status", Text),
    Column("to_status", Text),
    Column("payload", _JSON),
    Column("created_at_utc", DateTime(timezone=True)),
    schema="trading",
)

risk_decisions = Table(
    "risk_decisions",
    metadata,
    Column("id", _BIGINT, primary_key=True, autoincrement=True),
    Column("plan_id", Text, nullable=False),
    Column("session_id", Text, nullable=False),
    Column("occurred_at_utc", DateTime(timezone=True), nullable=False),
    Column("decision", Text, nullable=False),
    Column("reason_code", Text),
    Column("payload", _JSON),
    Column("created_at_utc", DateTime(timezone=True)),
    schema="risk",
)

__all__ = [
    "audit_events",
    "candidate_trade_plans",
    "event_contexts",
    "metadata",
    "order_events",
    "orders",
    "risk_decisions",
    "signals",
]
