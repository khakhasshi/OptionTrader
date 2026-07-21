"""Phase 4 automated review orchestration and multi-worker coordination.

Revision ID: 0007
Revises: 0006
Create Date: 2026-07-22
"""

from __future__ import annotations

from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(_UPGRADE_SQL)


def downgrade() -> None:
    op.execute(_DOWNGRADE_SQL)


_UPGRADE_SQL = r"""
CREATE TABLE review.llm_daily_budgets (
    budget_date        date PRIMARY KEY,
    request_count      integer NOT NULL DEFAULT 0,
    reserved_cost_usd  numeric NOT NULL DEFAULT 0,
    actual_cost_usd    numeric NOT NULL DEFAULT 0,
    updated_at_utc     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_llm_daily_budget_nonnegative CHECK (
        request_count >= 0
        AND reserved_cost_usd >= 0
        AND actual_cost_usd >= 0
    )
);

CREATE TABLE review.llm_request_leases (
    request_id         text PRIMARY KEY,
    identity_hash      text NOT NULL,
    state              text NOT NULL,
    owner_id           text,
    lease_expires_at_utc timestamptz,
    budget_date        date NOT NULL,
    reserved_cost_usd  numeric NOT NULL DEFAULT 0,
    actual_cost_usd    numeric NOT NULL DEFAULT 0,
    result_payload     jsonb,
    failure_code       text,
    created_at_utc     timestamptz NOT NULL DEFAULT now(),
    updated_at_utc     timestamptz NOT NULL DEFAULT now(),
    completed_at_utc   timestamptz,
    CONSTRAINT ck_llm_request_identity_hash CHECK (identity_hash ~ '^[a-f0-9]{64}$'),
    CONSTRAINT ck_llm_request_lease_state CHECK (
        state IN ('PENDING', 'IN_FLIGHT', 'INERT_PENDING', 'COMPLETED')
    ),
    CONSTRAINT ck_llm_request_lease_pair CHECK (
        (owner_id IS NULL) = (lease_expires_at_utc IS NULL)
    ),
    CONSTRAINT ck_llm_request_cost_nonnegative CHECK (
        reserved_cost_usd >= 0 AND actual_cost_usd >= 0
    ),
    CONSTRAINT ck_llm_request_failure_code CHECK (
        failure_code IS NULL OR failure_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
    ),
    CONSTRAINT ck_llm_request_result_state CHECK (
        (state = 'COMPLETED' AND result_payload IS NOT NULL
            AND owner_id IS NULL AND completed_at_utc IS NOT NULL)
        OR (state <> 'COMPLETED' AND result_payload IS NULL
            AND completed_at_utc IS NULL)
    )
);
CREATE INDEX ix_llm_request_leases_active
    ON review.llm_request_leases (state, lease_expires_at_utc, updated_at_utc)
    WHERE state <> 'COMPLETED';

CREATE TABLE review.llm_automation_runs (
    run_id             text PRIMARY KEY,
    kind               text NOT NULL,
    request_id         text NOT NULL UNIQUE,
    session_id         text NOT NULL,
    trading_date       date,
    state              text NOT NULL,
    inert_reason_code  text,
    trigger_hash       text NOT NULL,
    outbox_event_id    text UNIQUE,
    source_event_ids   jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at_utc     timestamptz NOT NULL DEFAULT now(),
    updated_at_utc     timestamptz NOT NULL DEFAULT now(),
    completed_at_utc   timestamptz,
    CONSTRAINT ck_llm_automation_kind CHECK (kind IN ('POST_MARKET', 'INTRADAY')),
    CONSTRAINT ck_llm_automation_state CHECK (
        state IN ('WAITING_INERT', 'ENQUEUED', 'PROCESSING', 'COMPLETED', 'DEAD_LETTERED')
    ),
    CONSTRAINT ck_llm_automation_reason CHECK (
        inert_reason_code IS NULL OR inert_reason_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
    ),
    CONSTRAINT ck_llm_automation_trigger_hash CHECK (trigger_hash ~ '^[a-f0-9]{64}$'),
    CONSTRAINT ck_llm_automation_terminal_time CHECK (
        (state IN ('COMPLETED', 'DEAD_LETTERED')) = (completed_at_utc IS NOT NULL)
    )
);
CREATE INDEX ix_llm_automation_due
    ON review.llm_automation_runs (kind, state, trading_date, updated_at_utc);

CREATE TABLE review.llm_trigger_events (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_outbox_id   bigint NOT NULL UNIQUE,
    source_event_id    text NOT NULL UNIQUE,
    session_id         text,
    topic              text NOT NULL,
    aggregate_type     text NOT NULL,
    aggregate_id       text NOT NULL,
    occurred_at_utc    timestamptz NOT NULL,
    event_fingerprint  text NOT NULL,
    payload            jsonb NOT NULL,
    state              text NOT NULL DEFAULT 'PENDING',
    available_at_utc   timestamptz NOT NULL,
    merged_run_id      text REFERENCES review.llm_automation_runs(run_id),
    created_at_utc     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_llm_trigger_fingerprint CHECK (event_fingerprint ~ '^[a-f0-9]{64}$'),
    CONSTRAINT ck_llm_trigger_state CHECK (state IN ('PENDING', 'MERGED', 'IGNORED')),
    CONSTRAINT ck_llm_trigger_merge CHECK (
        (state = 'MERGED') = (merged_run_id IS NOT NULL)
    ),
    CONSTRAINT uq_llm_trigger_session_fingerprint UNIQUE (session_id, event_fingerprint)
);
CREATE INDEX ix_llm_trigger_events_due
    ON review.llm_trigger_events (available_at_utc, id)
    WHERE state = 'PENDING';

CREATE TABLE review.llm_event_cursors (
    cursor_name         text PRIMARY KEY,
    last_outbox_id      bigint NOT NULL DEFAULT 0,
    updated_at_utc      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_llm_event_cursor_nonnegative CHECK (last_outbox_id >= 0)
);
INSERT INTO review.llm_event_cursors (cursor_name, last_outbox_id)
SELECT 'intraday-deterministic-state-v1', COALESCE(MAX(id), 0)
FROM audit.outbox_events;
"""

_DOWNGRADE_SQL = r"""
DROP TABLE IF EXISTS review.llm_event_cursors;
DROP INDEX IF EXISTS review.ix_llm_trigger_events_due;
DROP TABLE IF EXISTS review.llm_trigger_events;
DROP INDEX IF EXISTS review.ix_llm_automation_due;
DROP TABLE IF EXISTS review.llm_automation_runs;
DROP INDEX IF EXISTS review.ix_llm_request_leases_active;
DROP TABLE IF EXISTS review.llm_request_leases;
DROP TABLE IF EXISTS review.llm_daily_budgets;
"""
