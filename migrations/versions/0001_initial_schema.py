"""initial schema: 7 schemas + 16 core tables (skeleton)

首批核心表骨架（DEVELOPMENT_PLAN 第 7 节）。遵循约束：
- 时间统一 timestamptz(UTC)；金额/价格/数量/Greeks 用 numeric，不用浮点做风险判断。
- plan_id/signal_id/order_id/event_id/idempotency_key 唯一约束。
- 高频聚合表按 trading_date 逻辑分区键预留（MVP 先建普通表 + 索引，分区在数据量增长后引入）。

Revision ID: 0001
Revises:
Create Date: 2026-07-20
"""
from __future__ import annotations

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

SCHEMAS = ["market", "events", "trading", "risk", "review", "config", "audit"]


def upgrade() -> None:
    for schema in SCHEMAS:
        op.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    op.execute(_UPGRADE_SQL)


def downgrade() -> None:
    # 开发期回滚：删除表所在 schema（CASCADE），保证空库可重复升级/回滚。
    for schema in reversed(SCHEMAS):
        op.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


# 以纯 SQL 表达 DDL，便于评审与 SQLx 离线元数据对齐。
_UPGRADE_SQL = r"""
-- ============================ trading ============================
CREATE TABLE trading.trading_sessions (
    session_id      text PRIMARY KEY,
    trading_date    date NOT NULL,
    status          text NOT NULL,
    opened_at_utc   timestamptz,
    closed_at_utc   timestamptz,
    created_at_utc  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (trading_date)
);
-- ============================ market ============================
CREATE TABLE market.market_snapshots (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id      text NOT NULL REFERENCES trading.trading_sessions(session_id),
    symbol          text NOT NULL,
    trading_date    date NOT NULL,
    occurred_at_utc timestamptz NOT NULL,
    last_price      numeric,
    vwap            numeric,
    data_health     text NOT NULL,
    payload         jsonb,
    created_at_utc  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (symbol, occurred_at_utc)
);
CREATE INDEX ix_market_snapshots_session ON market.market_snapshots (session_id, occurred_at_utc);
CREATE INDEX ix_market_snapshots_symbol_date ON market.market_snapshots (symbol, trading_date);

CREATE TABLE market.option_snapshots (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id      text NOT NULL REFERENCES trading.trading_sessions(session_id),
    underlying      text NOT NULL,
    occurred_at_utc timestamptz NOT NULL,
    expiry          date NOT NULL,
    strike          numeric NOT NULL,
    option_right    text NOT NULL,
    bid             numeric,
    ask             numeric,
    iv              numeric,
    delta           numeric,
    gamma           numeric,
    theta           numeric,
    vega            numeric,
    payload         jsonb,
    created_at_utc  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (underlying, expiry, strike, option_right, occurred_at_utc)
);
CREATE INDEX ix_option_snapshots_session ON market.option_snapshots (session_id, occurred_at_utc);

-- ============================ events ============================
CREATE TABLE events.event_contexts (
    event_id        text PRIMARY KEY,
    session_id      text REFERENCES trading.trading_sessions(session_id),
    trading_date    date NOT NULL,
    category        text NOT NULL,
    occurred_at_utc timestamptz NOT NULL,
    source          text NOT NULL,
    payload         jsonb NOT NULL,
    created_at_utc  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_event_contexts_date_category ON events.event_contexts (trading_date, category);

-- ==================== trading (signal → plan → order) ====================
CREATE TABLE trading.signals (
    signal_id       text PRIMARY KEY,
    session_id      text NOT NULL REFERENCES trading.trading_sessions(session_id),
    occurred_at_utc timestamptz NOT NULL,
    regime          text,
    vol_state       text,
    strategy_kind   text NOT NULL,
    no_trade_reason text,
    payload         jsonb,
    created_at_utc  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_signals_session ON trading.signals (session_id, occurred_at_utc);

CREATE TABLE trading.candidate_trade_plans (
    plan_id         text PRIMARY KEY,
    signal_id       text NOT NULL REFERENCES trading.signals(signal_id),
    session_id      text NOT NULL REFERENCES trading.trading_sessions(session_id),
    status          text NOT NULL,
    strategy_kind   text NOT NULL,
    created_at_utc  timestamptz NOT NULL DEFAULT now(),
    payload         jsonb NOT NULL
);
CREATE INDEX ix_candidate_trade_plans_session ON trading.candidate_trade_plans (session_id, status);

CREATE TABLE trading.orders (
    order_id        text PRIMARY KEY,
    plan_id         text NOT NULL REFERENCES trading.candidate_trade_plans(plan_id),
    session_id      text NOT NULL REFERENCES trading.trading_sessions(session_id),
    idempotency_key text NOT NULL,
    status          text NOT NULL,
    side            text NOT NULL,
    quantity        numeric NOT NULL,
    limit_price     numeric,
    created_at_utc  timestamptz NOT NULL DEFAULT now(),
    updated_at_utc  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (idempotency_key)
);
CREATE INDEX ix_orders_session_status ON trading.orders (session_id, status);

CREATE TABLE trading.order_events (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id        text NOT NULL REFERENCES trading.orders(order_id),
    occurred_at_utc timestamptz NOT NULL,
    event_type      text NOT NULL,
    from_status     text,
    to_status       text,
    payload         jsonb,
    created_at_utc  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_order_events_order ON trading.order_events (order_id, occurred_at_utc);

CREATE TABLE trading.fills (
    fill_id         text PRIMARY KEY,
    order_id        text NOT NULL REFERENCES trading.orders(order_id),
    session_id      text NOT NULL REFERENCES trading.trading_sessions(session_id),
    occurred_at_utc timestamptz NOT NULL,
    quantity        numeric NOT NULL,
    price           numeric NOT NULL,
    payload         jsonb,
    created_at_utc  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_fills_order ON trading.fills (order_id, occurred_at_utc);

CREATE TABLE trading.position_snapshots (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id      text NOT NULL REFERENCES trading.trading_sessions(session_id),
    occurred_at_utc timestamptz NOT NULL,
    symbol          text NOT NULL,
    quantity        numeric NOT NULL,
    avg_price       numeric,
    unrealized_pnl  numeric,
    payload         jsonb,
    created_at_utc  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_position_snapshots_session ON trading.position_snapshots (session_id, occurred_at_utc);

-- ============================ risk ============================
CREATE TABLE risk.risk_decisions (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    plan_id         text NOT NULL REFERENCES trading.candidate_trade_plans(plan_id),
    session_id      text NOT NULL REFERENCES trading.trading_sessions(session_id),
    occurred_at_utc timestamptz NOT NULL,
    decision        text NOT NULL,
    reason_code     text,
    payload         jsonb,
    created_at_utc  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_risk_decisions_session ON risk.risk_decisions (session_id, occurred_at_utc);

CREATE TABLE risk.broker_snapshots (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id      text REFERENCES trading.trading_sessions(session_id),
    occurred_at_utc timestamptz NOT NULL,
    broker_health   text NOT NULL,
    buying_power    numeric,
    payload         jsonb,
    created_at_utc  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_broker_snapshots_time ON risk.broker_snapshots (occurred_at_utc);

-- ============================ review ============================
CREATE TABLE review.llm_reviews (
    review_id       text PRIMARY KEY,
    plan_id         text REFERENCES trading.candidate_trade_plans(plan_id),
    session_id      text REFERENCES trading.trading_sessions(session_id),
    occurred_at_utc timestamptz NOT NULL,
    verdict         text,
    model           text,
    payload         jsonb NOT NULL,
    created_at_utc  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_llm_reviews_session ON review.llm_reviews (session_id, occurred_at_utc);

CREATE TABLE review.daily_reviews (
    trading_date    date PRIMARY KEY,
    session_id      text REFERENCES trading.trading_sessions(session_id),
    generated_at_utc timestamptz NOT NULL,
    payload         jsonb NOT NULL,
    created_at_utc  timestamptz NOT NULL DEFAULT now()
);

-- ============================ config ============================
CREATE TABLE config.rule_versions (
    rule_version    text PRIMARY KEY,
    kind            text NOT NULL,
    activated_at_utc timestamptz,
    payload         jsonb NOT NULL,
    created_at_utc  timestamptz NOT NULL DEFAULT now()
);

-- ============================ audit ============================
CREATE TABLE audit.audit_events (
    event_id        text PRIMARY KEY,
    session_id      text REFERENCES trading.trading_sessions(session_id),
    occurred_at_utc timestamptz NOT NULL,
    actor           text NOT NULL,
    action          text NOT NULL,
    entity_type     text,
    entity_id       text,
    from_status     text,
    to_status       text,
    payload         jsonb,
    created_at_utc  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_audit_events_entity ON audit.audit_events (entity_type, entity_id);
CREATE INDEX ix_audit_events_session ON audit.audit_events (session_id, occurred_at_utc);
"""
