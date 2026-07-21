"""Durable broker account, position and fill reconciliation ledger.

Revision ID: 0004
Revises: 0003
Create Date: 2026-07-21
"""

from __future__ import annotations

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(_UPGRADE_SQL)


def downgrade() -> None:
    op.execute(_DOWNGRADE_SQL)


_UPGRADE_SQL = r"""
ALTER TABLE risk.broker_snapshots
    ADD COLUMN broker_id text,
    ADD COLUMN snapshot_sequence bigint,
    ADD COLUMN snapshot_hash text,
    ADD COLUMN net_liquidation numeric,
    ADD COLUMN reconciled boolean NOT NULL DEFAULT false,
    ADD COLUMN mismatch_codes jsonb NOT NULL DEFAULT '[]'::jsonb;

UPDATE risk.broker_snapshots
SET broker_id = 'legacy',
    snapshot_sequence = id,
    snapshot_hash = lpad(to_hex(id), 64, '0'),
    mismatch_codes = '["LEGACY_UNVERIFIED"]'::jsonb;

ALTER TABLE risk.broker_snapshots
    ALTER COLUMN broker_id SET NOT NULL,
    ALTER COLUMN snapshot_sequence SET NOT NULL,
    ALTER COLUMN snapshot_hash SET NOT NULL,
    ADD CONSTRAINT ck_broker_snapshot_sequence CHECK (snapshot_sequence > 0),
    ADD CONSTRAINT ck_broker_snapshot_hash CHECK (snapshot_hash ~ '^[a-f0-9]{64}$'),
    ADD CONSTRAINT uq_broker_snapshot_identity UNIQUE
        (broker_id, snapshot_sequence, snapshot_hash);

ALTER TABLE trading.position_snapshots
    ALTER COLUMN session_id DROP NOT NULL,
    ADD COLUMN broker_id text,
    ADD COLUMN snapshot_sequence bigint,
    ADD COLUMN snapshot_hash text,
    ADD COLUMN contract_id text;

UPDATE trading.position_snapshots
SET broker_id = 'legacy',
    snapshot_sequence = id,
    snapshot_hash = lpad(to_hex(id), 64, '0'),
    contract_id = symbol;

ALTER TABLE trading.position_snapshots
    ALTER COLUMN broker_id SET NOT NULL,
    ALTER COLUMN snapshot_sequence SET NOT NULL,
    ALTER COLUMN snapshot_hash SET NOT NULL,
    ALTER COLUMN contract_id SET NOT NULL,
    ADD CONSTRAINT uq_position_broker_snapshot UNIQUE
        (broker_id, snapshot_hash, contract_id);

ALTER TABLE trading.fills
    ALTER COLUMN order_id DROP NOT NULL,
    ALTER COLUMN session_id DROP NOT NULL,
    ADD COLUMN broker_id text,
    ADD COLUMN broker_order_id text,
    ADD COLUMN contract_id text,
    ADD COLUMN side text,
    ADD COLUMN snapshot_hash text;

UPDATE trading.fills
SET broker_id = 'legacy',
    broker_order_id = COALESCE(order_id, fill_id),
    contract_id = COALESCE(payload->>'contract_id', fill_id),
    side = COALESCE(payload->>'side', 'UNSPECIFIED'),
    snapshot_hash = lpad(to_hex(abs(hashtext(fill_id))::bigint), 64, '0');

ALTER TABLE trading.fills
    ALTER COLUMN broker_id SET NOT NULL,
    ALTER COLUMN broker_order_id SET NOT NULL,
    ALTER COLUMN contract_id SET NOT NULL,
    ALTER COLUMN side SET NOT NULL,
    ALTER COLUMN snapshot_hash SET NOT NULL;

CREATE INDEX ix_broker_snapshots_broker_sequence
    ON risk.broker_snapshots (broker_id, snapshot_sequence DESC);
CREATE INDEX ix_position_snapshots_broker_sequence
    ON trading.position_snapshots (broker_id, snapshot_sequence, contract_id);
CREATE INDEX ix_fills_broker_order
    ON trading.fills (broker_id, broker_order_id, occurred_at_utc);
"""

_DOWNGRADE_SQL = r"""
DROP INDEX IF EXISTS trading.ix_fills_broker_order;
DROP INDEX IF EXISTS trading.ix_position_snapshots_broker_sequence;
DROP INDEX IF EXISTS risk.ix_broker_snapshots_broker_sequence;

DELETE FROM trading.fills WHERE broker_id <> 'legacy';
DELETE FROM trading.position_snapshots WHERE broker_id <> 'legacy';
DELETE FROM risk.broker_snapshots WHERE broker_id <> 'legacy';

ALTER TABLE trading.fills
    DROP COLUMN snapshot_hash,
    DROP COLUMN side,
    DROP COLUMN contract_id,
    DROP COLUMN broker_order_id,
    DROP COLUMN broker_id,
    ALTER COLUMN session_id SET NOT NULL,
    ALTER COLUMN order_id SET NOT NULL;

ALTER TABLE trading.position_snapshots
    DROP CONSTRAINT IF EXISTS uq_position_broker_snapshot,
    DROP COLUMN contract_id,
    DROP COLUMN snapshot_hash,
    DROP COLUMN snapshot_sequence,
    DROP COLUMN broker_id,
    ALTER COLUMN session_id SET NOT NULL;

ALTER TABLE risk.broker_snapshots
    DROP CONSTRAINT IF EXISTS uq_broker_snapshot_identity,
    DROP CONSTRAINT IF EXISTS ck_broker_snapshot_hash,
    DROP CONSTRAINT IF EXISTS ck_broker_snapshot_sequence,
    DROP COLUMN mismatch_codes,
    DROP COLUMN reconciled,
    DROP COLUMN net_liquidation,
    DROP COLUMN snapshot_hash,
    DROP COLUMN snapshot_sequence,
    DROP COLUMN broker_id;
"""
