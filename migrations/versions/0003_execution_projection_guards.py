"""Database-level order projection and shared confirmation capability guards.

Revision ID: 0003
Revises: 0002
Create Date: 2026-07-21
"""

from __future__ import annotations

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(_UPGRADE_SQL)


def downgrade() -> None:
    op.execute(_DOWNGRADE_SQL)


_UPGRADE_SQL = r"""
ALTER TABLE trading.orders
    ADD COLUMN state_version bigint NOT NULL DEFAULT 1;

UPDATE trading.orders
SET state_version = COALESCE((payload->>'state_version')::bigint, 1);

ALTER TABLE trading.orders
    ALTER COLUMN state_version DROP DEFAULT,
    ADD CONSTRAINT ck_order_state_version_positive CHECK (state_version > 0);

CREATE TABLE risk.confirmation_capabilities (
    order_id text PRIMARY KEY REFERENCES trading.orders(order_id) ON DELETE CASCADE,
    plan_hash text NOT NULL,
    token_ciphertext text NOT NULL,
    expires_at_utc timestamptz NOT NULL,
    claimed_at_utc timestamptz,
    created_at_utc timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_confirmation_plan_hash CHECK (plan_hash ~ '^[a-f0-9]{64}$'),
    CONSTRAINT ck_confirmation_ciphertext_nonempty CHECK (length(token_ciphertext) > 0),
    CONSTRAINT ck_confirmation_expiry CHECK (expires_at_utc > created_at_utc)
);
"""

_DOWNGRADE_SQL = r"""
DROP TABLE IF EXISTS risk.confirmation_capabilities;
ALTER TABLE trading.orders
    DROP CONSTRAINT IF EXISTS ck_order_state_version_positive,
    DROP COLUMN IF EXISTS state_version;
"""
