"""Transactional outbox for execution and reconciliation state changes.

Revision ID: 0005
Revises: 0004
Create Date: 2026-07-21
"""

from __future__ import annotations

from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(_UPGRADE_SQL)


def downgrade() -> None:
    op.execute(_DOWNGRADE_SQL)


_UPGRADE_SQL = r"""
CREATE TABLE audit.outbox_events (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id         text NOT NULL UNIQUE,
    topic            text NOT NULL,
    aggregate_type   text NOT NULL,
    aggregate_id     text NOT NULL,
    occurred_at_utc  timestamptz NOT NULL,
    payload          jsonb NOT NULL,
    attempts         integer NOT NULL DEFAULT 0,
    available_at_utc timestamptz NOT NULL DEFAULT now(),
    lease_owner      text,
    lease_expires_at_utc timestamptz,
    published_at_utc timestamptz,
    dead_lettered_at_utc timestamptz,
    last_error_code  text,
    created_at_utc   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_outbox_attempts_nonnegative CHECK (attempts >= 0),
    CONSTRAINT ck_outbox_event_id_nonempty CHECK (length(event_id) > 0),
    CONSTRAINT ck_outbox_topic_nonempty CHECK (length(topic) > 0),
    CONSTRAINT ck_outbox_aggregate_nonempty CHECK (
        length(aggregate_type) > 0 AND length(aggregate_id) > 0
    ),
    CONSTRAINT ck_outbox_lease_pair CHECK (
        (lease_owner IS NULL) = (lease_expires_at_utc IS NULL)
    ),
    CONSTRAINT ck_outbox_terminal_state CHECK (
        NOT (published_at_utc IS NOT NULL AND dead_lettered_at_utc IS NOT NULL)
    ),
    CONSTRAINT ck_outbox_error_code CHECK (
        last_error_code IS NULL OR last_error_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
    )
);

CREATE INDEX ix_outbox_unpublished
    ON audit.outbox_events (available_at_utc, id)
    WHERE published_at_utc IS NULL AND dead_lettered_at_utc IS NULL;
CREATE INDEX ix_outbox_aggregate
    ON audit.outbox_events (aggregate_type, aggregate_id, created_at_utc);
"""

_DOWNGRADE_SQL = r"""
DROP INDEX IF EXISTS audit.ix_outbox_aggregate;
DROP INDEX IF EXISTS audit.ix_outbox_unpublished;
DROP TABLE IF EXISTS audit.outbox_events;
"""
