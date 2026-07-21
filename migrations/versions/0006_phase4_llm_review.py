"""Phase 4 advisory LLM review audit, daily review and research queue.

Revision ID: 0006
Revises: 0005
Create Date: 2026-07-22
"""

from __future__ import annotations

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(_UPGRADE_SQL)


def downgrade() -> None:
    op.execute(_DOWNGRADE_SQL)


_UPGRADE_SQL = r"""
ALTER TABLE review.llm_reviews
    ADD COLUMN legacy_payload jsonb,
    ADD COLUMN request_id text,
    ADD COLUMN correlation_id text,
    ADD COLUMN causation_id text,
    ADD COLUMN review_kind text,
    ADD COLUMN review_status text,
    ADD COLUMN trading_date date,
    ADD COLUMN plan_hash text,
    ADD COLUMN input_hash text,
    ADD COLUMN provider text,
    ADD COLUMN prompt_version text,
    ADD COLUMN schema_version text,
    ADD COLUMN received_at_utc timestamptz,
    ADD COLUMN rule_version text,
    ADD COLUMN unavailable_reason_code text,
    ADD COLUMN latency_ms integer NOT NULL DEFAULT 0,
    ADD COLUMN attempts integer NOT NULL DEFAULT 0,
    ADD COLUMN cache_hit boolean NOT NULL DEFAULT false,
    ADD COLUMN input_tokens integer NOT NULL DEFAULT 0,
    ADD COLUMN output_tokens integer NOT NULL DEFAULT 0,
    ADD COLUMN estimated_cost_usd numeric NOT NULL DEFAULT 0;

UPDATE review.llm_reviews
SET legacy_payload = payload;

UPDATE review.llm_reviews AS review_row
SET request_id = review_row.review_id,
    correlation_id = COALESCE(
        NULLIF(review_row.plan_id, ''), NULLIF(review_row.session_id, ''), review_row.review_id
    ),
    review_kind = 'POST_MARKET',
    review_status = 'UNAVAILABLE',
    trading_date = (review_row.occurred_at_utc AT TIME ZONE 'America/New_York')::date,
    plan_hash = plan_row.plan_hash,
    -- Per-row opaque legacy identity avoids collapsing unrelated rows in hash lookups.
    input_hash = md5('legacy-input:v1:' || review_row.review_id)
        || md5('legacy-input:v2:' || review_row.review_id),
    provider = 'legacy-unconfigured',
    prompt_version = 'legacy-unversioned',
    schema_version = '0.legacy',
    received_at_utc = COALESCE(review_row.created_at_utc, review_row.occurred_at_utc),
    rule_version = 'legacy-unversioned',
    unavailable_reason_code = 'CONFIG_MISSING'
FROM trading.candidate_trade_plans AS plan_row
WHERE review_row.plan_id = plan_row.plan_id;

UPDATE review.llm_reviews
SET request_id = COALESCE(request_id, review_id),
    correlation_id = COALESCE(
        NULLIF(correlation_id, ''), NULLIF(session_id, ''), review_id
    ),
    review_kind = COALESCE(review_kind, 'POST_MARKET'),
    review_status = COALESCE(review_status, 'UNAVAILABLE'),
    trading_date = COALESCE(
        trading_date,
        (occurred_at_utc AT TIME ZONE 'America/New_York')::date
    ),
    input_hash = COALESCE(
        input_hash,
        md5('legacy-input:v1:' || review_id) || md5('legacy-input:v2:' || review_id)
    ),
    provider = COALESCE(provider, 'legacy-unconfigured'),
    prompt_version = COALESCE(prompt_version, 'legacy-unversioned'),
    schema_version = COALESCE(schema_version, '0.legacy'),
    received_at_utc = COALESCE(received_at_utc, created_at_utc, occurred_at_utc),
    rule_version = COALESCE(rule_version, 'legacy-unversioned'),
    unavailable_reason_code = COALESCE(unavailable_reason_code, 'CONFIG_MISSING');

UPDATE review.llm_reviews
SET payload = jsonb_build_object(
    'schema_version', '1.0',
    'review_id', review_id,
    'request_id', request_id,
    'correlation_id', correlation_id,
    'causation_id', causation_id,
    'session_id', COALESCE(NULLIF(session_id, ''), 'legacy_session_' || md5(review_id)),
    'occurred_at_utc', to_char(
        occurred_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
    ),
    'received_at_utc', to_char(
        received_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
    ),
    'source', 'llm-intelligence-layer',
    'source_sequence', 0,
    'rule_version', rule_version,
    'stage', review_kind,
    'trading_date', trading_date::text,
    'plan_id', plan_id,
    'plan_hash', plan_hash,
    'review_status', 'UNAVAILABLE',
    'summary', '',
    'decision_support', 'Legacy review preserved during Phase 4 migration; no advisory action.',
    'sop_alignment', 'Unknown',
    'risk_notes', jsonb_build_array(),
    'invalidations', jsonb_build_array(),
    'recommended_action', 'Review Only',
    'confidence', 0,
    'rule_references', jsonb_build_array(),
    'evidence_citations', jsonb_build_array(),
    'daily_review', NULL,
    'rule_hypotheses', jsonb_build_array(),
    'unavailable_reason_code', 'CONFIG_MISSING',
    'provider', jsonb_build_object(
        'provider', left(provider, 80),
        'model', left(COALESCE(NULLIF(model, ''), 'legacy-unconfigured'), 120),
        'provider_request_id', NULL,
        'prompt_version', prompt_version,
        'input_hash', input_hash,
        'latency_ms', 0,
        'attempts', 0,
        'cache_hit', false,
        'input_tokens', 0,
        'output_tokens', 0,
        'estimated_cost_usd', '0'
    ),
    'source_refs', jsonb_build_array()
);

ALTER TABLE review.llm_reviews
    ALTER COLUMN request_id SET NOT NULL,
    ALTER COLUMN correlation_id SET NOT NULL,
    ALTER COLUMN review_kind SET NOT NULL,
    ALTER COLUMN review_status SET NOT NULL,
    ALTER COLUMN input_hash SET NOT NULL,
    ALTER COLUMN provider SET NOT NULL,
    ALTER COLUMN prompt_version SET NOT NULL,
    ALTER COLUMN schema_version SET NOT NULL,
    ALTER COLUMN received_at_utc SET NOT NULL,
    ALTER COLUMN rule_version SET NOT NULL,
    ALTER COLUMN latency_ms DROP DEFAULT,
    ALTER COLUMN attempts DROP DEFAULT,
    ALTER COLUMN cache_hit DROP DEFAULT,
    ALTER COLUMN input_tokens DROP DEFAULT,
    ALTER COLUMN output_tokens DROP DEFAULT,
    ALTER COLUMN estimated_cost_usd DROP DEFAULT,
    ADD CONSTRAINT uq_llm_reviews_request UNIQUE (request_id),
    ADD CONSTRAINT ck_llm_review_kind CHECK (
        review_kind IN ('POST_MARKET', 'PRE_MARKET', 'INTRADAY', 'PRE_EXECUTION', 'RULE_HYPOTHESIS')
    ),
    ADD CONSTRAINT ck_llm_review_status CHECK (
        review_status IN ('COMPLETED', 'UNAVAILABLE', 'INVALID')
    ),
    ADD CONSTRAINT ck_llm_review_input_hash CHECK (input_hash ~ '^[a-f0-9]{64}$'),
    ADD CONSTRAINT ck_llm_review_usage CHECK (
        latency_ms >= 0 AND attempts BETWEEN 0 AND 4
        AND input_tokens BETWEEN 0 AND 1000000
        AND output_tokens BETWEEN 0 AND 65536
        AND estimated_cost_usd >= 0
    );

CREATE INDEX ix_llm_reviews_input_hash
    ON review.llm_reviews (input_hash, review_status, occurred_at_utc DESC);
CREATE INDEX ix_llm_reviews_kind_date
    ON review.llm_reviews (review_kind, trading_date, occurred_at_utc DESC);

ALTER TABLE review.daily_reviews
    ADD COLUMN review_id text,
    ADD COLUMN status text,
    ADD COLUMN legacy_payload jsonb;

UPDATE review.daily_reviews
SET review_id = 'legacy_daily_' || trading_date::text,
    status = 'UNAVAILABLE',
    legacy_payload = payload;

UPDATE review.daily_reviews
SET payload = jsonb_build_object(
    'schema_version', '1.0',
    'review_id', review_id,
    'request_id', review_id,
    'correlation_id', COALESCE(NULLIF(session_id, ''), review_id),
    'causation_id', NULL,
    'session_id', COALESCE(NULLIF(session_id, ''), 'legacy_session_' || md5(review_id)),
    'occurred_at_utc', to_char(
        generated_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
    ),
    'received_at_utc', to_char(
        generated_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
    ),
    'source', 'llm-intelligence-layer',
    'source_sequence', 0,
    'rule_version', 'legacy-unversioned',
    'stage', 'POST_MARKET',
    'trading_date', trading_date::text,
    'plan_id', NULL,
    'plan_hash', NULL,
    'review_status', 'UNAVAILABLE',
    'summary', '',
    'decision_support', 'Legacy daily review preserved during Phase 4 migration; no advisory action.',
    'sop_alignment', 'Unknown',
    'risk_notes', jsonb_build_array(),
    'invalidations', jsonb_build_array(),
    'recommended_action', 'Review Only',
    'confidence', 0,
    'rule_references', jsonb_build_array(),
    'evidence_citations', jsonb_build_array(),
    'daily_review', NULL,
    'rule_hypotheses', jsonb_build_array(),
    'unavailable_reason_code', 'CONFIG_MISSING',
    'provider', jsonb_build_object(
        'provider', 'legacy-unconfigured',
        'model', 'legacy-unconfigured',
        'provider_request_id', NULL,
        'prompt_version', 'legacy-unversioned',
        -- Daily legacy rows also receive a stable, non-active input identity.
        'input_hash', md5('legacy-daily:v1:' || review_id)
            || md5('legacy-daily:v2:' || review_id),
        'latency_ms', 0,
        'attempts', 0,
        'cache_hit', false,
        'input_tokens', 0,
        'output_tokens', 0,
        'estimated_cost_usd', '0'
    ),
    'source_refs', jsonb_build_array()
);

ALTER TABLE review.daily_reviews
    ALTER COLUMN review_id SET NOT NULL,
    ALTER COLUMN status SET NOT NULL,
    ADD CONSTRAINT uq_daily_reviews_review_id UNIQUE (review_id),
    ADD CONSTRAINT ck_daily_review_status CHECK (status IN ('COMPLETED', 'UNAVAILABLE', 'INVALID'));

CREATE TABLE review.rule_hypotheses (
    hypothesis_id      text PRIMARY KEY,
    review_id          text NOT NULL REFERENCES review.llm_reviews(review_id),
    session_id         text REFERENCES trading.trading_sessions(session_id),
    trading_date       date,
    status             text NOT NULL DEFAULT 'PENDING_RESEARCH',
    activation_allowed boolean NOT NULL DEFAULT false,
    payload            jsonb NOT NULL,
    created_at_utc     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_rule_hypothesis_status CHECK (
        status IN ('PENDING_RESEARCH', 'VALIDATING', 'REJECTED', 'APPROVED_FOR_SHADOW')
    ),
    CONSTRAINT ck_rule_hypothesis_no_activation CHECK (activation_allowed = false)
);
CREATE INDEX ix_rule_hypotheses_status
    ON review.rule_hypotheses (status, trading_date, created_at_utc);
"""

_DOWNGRADE_SQL = r"""
DROP INDEX IF EXISTS review.ix_rule_hypotheses_status;
DROP TABLE IF EXISTS review.rule_hypotheses;

UPDATE review.daily_reviews
SET payload = legacy_payload
WHERE legacy_payload IS NOT NULL;

ALTER TABLE review.daily_reviews
    DROP CONSTRAINT IF EXISTS ck_daily_review_status,
    DROP CONSTRAINT IF EXISTS uq_daily_reviews_review_id,
    DROP COLUMN IF EXISTS status,
    DROP COLUMN IF EXISTS review_id,
    DROP COLUMN IF EXISTS legacy_payload;

DROP INDEX IF EXISTS review.ix_llm_reviews_kind_date;
DROP INDEX IF EXISTS review.ix_llm_reviews_input_hash;
UPDATE review.llm_reviews
SET payload = legacy_payload
WHERE legacy_payload IS NOT NULL;

ALTER TABLE review.llm_reviews
    DROP CONSTRAINT IF EXISTS ck_llm_review_usage,
    DROP CONSTRAINT IF EXISTS ck_llm_review_input_hash,
    DROP CONSTRAINT IF EXISTS ck_llm_review_status,
    DROP CONSTRAINT IF EXISTS ck_llm_review_kind,
    DROP CONSTRAINT IF EXISTS uq_llm_reviews_request,
    DROP COLUMN IF EXISTS legacy_payload,
    DROP COLUMN IF EXISTS estimated_cost_usd,
    DROP COLUMN IF EXISTS output_tokens,
    DROP COLUMN IF EXISTS input_tokens,
    DROP COLUMN IF EXISTS cache_hit,
    DROP COLUMN IF EXISTS attempts,
    DROP COLUMN IF EXISTS latency_ms,
    DROP COLUMN IF EXISTS unavailable_reason_code,
    DROP COLUMN IF EXISTS rule_version,
    DROP COLUMN IF EXISTS received_at_utc,
    DROP COLUMN IF EXISTS schema_version,
    DROP COLUMN IF EXISTS prompt_version,
    DROP COLUMN IF EXISTS provider,
    DROP COLUMN IF EXISTS input_hash,
    DROP COLUMN IF EXISTS plan_hash,
    DROP COLUMN IF EXISTS trading_date,
    DROP COLUMN IF EXISTS review_status,
    DROP COLUMN IF EXISTS review_kind,
    DROP COLUMN IF EXISTS causation_id,
    DROP COLUMN IF EXISTS correlation_id,
    DROP COLUMN IF EXISTS request_id;
"""
