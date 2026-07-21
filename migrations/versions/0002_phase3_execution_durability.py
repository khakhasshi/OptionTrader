"""Phase 3 execution durability and reconciliation columns.

Revision ID: 0002
Revises: 0001
Create Date: 2026-07-21
"""

from __future__ import annotations

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(_UPGRADE_SQL)


def downgrade() -> None:
    op.execute(_DOWNGRADE_SQL)


_UPGRADE_SQL = r"""
ALTER TABLE trading.candidate_trade_plans
    ADD COLUMN plan_hash text,
    ADD COLUMN idempotency_key text,
    ADD COLUMN execution_mode text,
    ADD COLUMN expires_at_utc timestamptz;

UPDATE trading.candidate_trade_plans
SET plan_hash = payload->>'plan_hash',
    idempotency_key = payload->>'idempotency_key',
    execution_mode = payload->>'execution_mode',
    expires_at_utc = (payload->>'expires_at_utc')::timestamptz;

ALTER TABLE trading.candidate_trade_plans
    ALTER COLUMN plan_hash SET NOT NULL,
    ALTER COLUMN idempotency_key SET NOT NULL,
    ALTER COLUMN execution_mode SET NOT NULL,
    ALTER COLUMN expires_at_utc SET NOT NULL,
    ADD CONSTRAINT uq_candidate_plan_hash UNIQUE (plan_hash),
    ADD CONSTRAINT uq_candidate_idempotency UNIQUE (idempotency_key),
    ADD CONSTRAINT ck_candidate_expiry CHECK (expires_at_utc > created_at_utc);

ALTER TABLE trading.orders
    ADD COLUMN filled_quantity numeric NOT NULL DEFAULT 0,
    ADD COLUMN broker_order_id text,
    ADD COLUMN payload jsonb,
    ADD CONSTRAINT ck_order_quantity_positive CHECK (quantity > 0),
    ADD CONSTRAINT ck_order_fill_bounds CHECK (
        filled_quantity >= 0 AND filled_quantity <= quantity
    );

CREATE UNIQUE INDEX uq_orders_broker_order_id
    ON trading.orders (broker_order_id) WHERE broker_order_id IS NOT NULL;
"""

_DOWNGRADE_SQL = r"""
DROP INDEX IF EXISTS trading.uq_orders_broker_order_id;
ALTER TABLE trading.orders
    DROP CONSTRAINT IF EXISTS ck_order_fill_bounds,
    DROP CONSTRAINT IF EXISTS ck_order_quantity_positive,
    DROP COLUMN IF EXISTS payload,
    DROP COLUMN IF EXISTS broker_order_id,
    DROP COLUMN IF EXISTS filled_quantity;
ALTER TABLE trading.candidate_trade_plans
    DROP CONSTRAINT IF EXISTS ck_candidate_expiry,
    DROP CONSTRAINT IF EXISTS uq_candidate_idempotency,
    DROP CONSTRAINT IF EXISTS uq_candidate_plan_hash,
    DROP COLUMN IF EXISTS expires_at_utc,
    DROP COLUMN IF EXISTS execution_mode,
    DROP COLUMN IF EXISTS idempotency_key,
    DROP COLUMN IF EXISTS plan_hash;
"""
