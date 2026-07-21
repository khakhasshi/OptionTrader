from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
import os
from threading import Barrier
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, delete, event, func, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

from app.llm.automation import LLMAutomationSettings, LLMAutomationSupervisor
from app.llm.automation_repository import (
    AutomationRun,
    due_post_market_dates,
    enqueue_intraday_review,
    ingest_intraday_trigger_events,
    schedule_post_market_review,
)
from app.llm.config import LLMSettings
from app.llm.market_calendar import (
    materialize_recent_xnys_sessions,
    xnys_session_schedule,
)
from app.llm.models import DailyReviewDetail, LLMReviewContent
from app.llm.provider import ContentValidator, ProviderCompletion
from app.llm.service import LLMReviewService
from app.persistence import write_outbox
from app.persistence.tables import (
    broker_snapshots,
    candidate_trade_plans,
    event_contexts,
    fills,
    llm_automation_runs,
    daily_reviews,
    llm_event_cursors,
    llm_reviews,
    llm_trigger_events,
    metadata,
    orders,
    outbox_events,
    risk_decisions,
    signals,
    trading_sessions,
)
from tests.event_support import available_event_context


DAY = date(2026, 7, 22)
SESSION = "session_2026-07-22"
OPEN = datetime(2026, 7, 22, 13, 30, tzinfo=UTC)
CLOSE = datetime(2026, 7, 22, 20, 0, tzinfo=UTC)


class _PostMarketProvider:
    def __init__(self) -> None:
        self.calls = 0

    async def complete(
        self,
        _system: str,
        _provider_payload: str,
        *,
        validator: ContentValidator | None = None,
    ) -> ProviderCompletion:
        self.calls += 1
        content = LLMReviewContent(
            summary="Post-market evidence was reviewed.",
            decision_support="Advisory only.",
            sop_alignment="Aligned",
            risk_notes=[],
            invalidations=[],
            recommended_action="Review Only",
            confidence=0.6,
            rule_references=[],
            evidence_citations=[],
            daily_review=DailyReviewDetail(
                best_trade=None,
                worst_trade=None,
                good_losses=[],
                bad_losses=[],
                sop_violations=[],
                loss_attribution=[],
                one_change_tomorrow="Keep deterministic controls unchanged.",
            ),
            rule_hypotheses=[],
        )
        if validator is not None:
            content = validator(content)
        return ProviderCompletion(
            content=content,
            provider_request_id="post-market-test",
            attempts=1,
            latency_ms=10,
            input_tokens=100,
            output_tokens=50,
        )


def test_automation_settings_are_disabled_by_default_and_opt_in_is_strict() -> None:
    settings = LLMAutomationSettings.from_env({})
    assert settings.enabled is False
    with pytest.raises(ValueError, match="must be true or false"):
        LLMAutomationSettings.from_env({"OPTIONTRADER_LLM_AUTOMATION_ENABLED": "TRUE"})


def _configured_llm_settings() -> LLMSettings:
    return LLMSettings.from_env(
        {
            "LLM_PROVIDER": "deepseek-openai",
            "LLM_BASE_URL": "https://api.deepseek.com",
            "LLM_API_KEY": "test-key-never-real",
            "LLM_MODEL": "deepseek-v4-flash",
        }
    )


def _engine() -> Engine:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _attach(dbapi_conn: Any, _record: Any) -> None:
        cursor = dbapi_conn.cursor()
        for schema in ("trading", "audit", "events", "risk", "review"):
            cursor.execute(f"ATTACH DATABASE ':memory:' AS {schema}")
        cursor.close()

    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(
            llm_event_cursors.insert().values(
                cursor_name="intraday-deterministic-state-v1",
                last_outbox_id=0,
                updated_at_utc=OPEN,
            )
        )
    return engine


def _seed_session(engine: Engine, *, closed: bool) -> None:
    with engine.begin() as conn:
        conn.execute(
            trading_sessions.insert().values(
                session_id=SESSION,
                trading_date=DAY,
                status="CLOSED" if closed else "OPEN",
                opened_at_utc=OPEN,
                closed_at_utc=CLOSE if closed else None,
                created_at_utc=OPEN,
            )
        )


def _seed_signal(engine: Engine, signal_id: str = "signal-1") -> None:
    with engine.begin() as conn:
        conn.execute(
            signals.insert().values(
                signal_id=signal_id,
                session_id=SESSION,
                occurred_at_utc=OPEN + timedelta(minutes=15),
                regime="Trend",
                vol_state="IVCheap",
                strategy_kind="NoTrade",
                no_trade_reason="deterministic entry conditions were incomplete",
                payload={"signal": {"signal_id": signal_id}},
                created_at_utc=OPEN + timedelta(minutes=15),
            )
        )


def _seed_event(engine: Engine) -> None:
    payload = available_event_context("2026-07-22T15:00:00Z")
    with engine.begin() as conn:
        conn.execute(
            event_contexts.insert().values(
                event_id=payload["event_context_id"],
                session_id=SESSION,
                trading_date=DAY,
                category="Normal",
                occurred_at_utc=datetime(2026, 7, 22, 15, 0, tzinfo=UTC),
                source="event-context-layer",
                payload=payload,
                created_at_utc=datetime(2026, 7, 22, 15, 0, tzinfo=UTC),
            )
        )


def test_post_market_stays_inert_during_session_and_when_evidence_is_missing() -> None:
    engine = _engine()
    _seed_session(engine, closed=False)
    during = schedule_post_market_review(
        engine,
        DAY,
        now=datetime(2026, 7, 22, 18, 0, tzinfo=UTC),
        rule_version="rules-test",
        grace_seconds=60,
    )
    assert during.state == "WAITING_INERT"
    assert during.inert_reason_code == "SESSION_NOT_CLOSED"
    with engine.begin() as conn:
        conn.execute(
            trading_sessions.update()
            .where(trading_sessions.c.session_id == SESSION)
            .values(status="CLOSED", closed_at_utc=CLOSE)
        )
    missing = schedule_post_market_review(
        engine,
        DAY,
        now=CLOSE + timedelta(minutes=2),
        rule_version="rules-test",
        grace_seconds=60,
    )
    assert missing.inert_reason_code == "SIGNALS_MISSING"
    with engine.connect() as conn:
        assert conn.execute(select(outbox_events)).first() is None


def test_post_market_due_dates_retry_waiting_but_skip_already_scheduled_runs() -> None:
    engine = _engine()
    _seed_session(engine, closed=True)
    waiting_day = DAY - timedelta(days=1)
    waiting_session = "session-waiting"
    now = CLOSE + timedelta(minutes=2)
    with engine.begin() as conn:
        conn.execute(
            trading_sessions.insert().values(
                session_id=waiting_session,
                trading_date=waiting_day,
                status="CLOSED",
                opened_at_utc=OPEN - timedelta(days=1),
                closed_at_utc=CLOSE - timedelta(days=1),
                created_at_utc=OPEN - timedelta(days=1),
            )
        )
        conn.execute(
            llm_automation_runs.insert(),
            [
                {
                    "run_id": "run:post_market:2026-07-22:v1",
                    "kind": "POST_MARKET",
                    "request_id": "post_market:2026-07-22:v1",
                    "session_id": SESSION,
                    "trading_date": DAY,
                    "state": "COMPLETED",
                    "inert_reason_code": None,
                    "trigger_hash": "a" * 64,
                    "outbox_event_id": None,
                    "source_event_ids": [],
                    "created_at_utc": now,
                    "updated_at_utc": now,
                    "completed_at_utc": now,
                },
                {
                    "run_id": "run:post_market:2026-07-21:v1",
                    "kind": "POST_MARKET",
                    "request_id": "post_market:2026-07-21:v1",
                    "session_id": waiting_session,
                    "trading_date": waiting_day,
                    "state": "WAITING_INERT",
                    "inert_reason_code": "SIGNALS_MISSING",
                    "trigger_hash": "b" * 64,
                    "outbox_event_id": None,
                    "source_event_ids": [],
                    "created_at_utc": now,
                    "updated_at_utc": now,
                    "completed_at_utc": None,
                },
            ],
        )
    assert due_post_market_dates(engine, now=now) == [waiting_day]


def test_xnys_calendar_handles_holiday_and_early_close_without_hardcoded_1600() -> None:
    assert xnys_session_schedule(date(2026, 7, 3)) is None
    early_close = xnys_session_schedule(date(2026, 11, 27))
    assert early_close is not None
    assert early_close.closed_at_utc == datetime(2026, 11, 27, 18, 0, tzinfo=UTC)

    engine = _engine()
    materialize_recent_xnys_sessions(
        engine,
        now=datetime(2026, 11, 27, 17, 59, tzinfo=UTC),
        lookback_days=0,
    )
    with engine.connect() as conn:
        before = (
            conn.execute(
                select(trading_sessions).where(
                    trading_sessions.c.trading_date == date(2026, 11, 27)
                )
            )
            .mappings()
            .one()
        )
    assert before["status"] == "OPEN"
    assert before["closed_at_utc"] is None

    materialize_recent_xnys_sessions(
        engine,
        now=datetime(2026, 11, 27, 18, 0, tzinfo=UTC),
        lookback_days=0,
    )
    with engine.connect() as conn:
        after = (
            conn.execute(
                select(trading_sessions).where(
                    trading_sessions.c.trading_date == date(2026, 11, 27)
                )
            )
            .mappings()
            .one()
        )
    assert after["status"] == "CLOSED"
    assert after["closed_at_utc"].replace(tzinfo=UTC) == early_close.closed_at_utc


def test_post_market_enqueues_once_with_fixed_request_after_all_gates_pass() -> None:
    engine = _engine()
    _seed_session(engine, closed=True)
    _seed_signal(engine)
    _seed_event(engine)
    now = CLOSE + timedelta(minutes=2)
    first = schedule_post_market_review(
        engine, DAY, now=now, rule_version="rules-test", grace_seconds=60
    )
    second = schedule_post_market_review(
        engine, DAY, now=now + timedelta(minutes=1), rule_version="rules-test", grace_seconds=60
    )
    assert first == second
    assert first.state == "ENQUEUED"
    assert first.request_id == "post_market:2026-07-22:v1"
    with engine.connect() as conn:
        rows = (
            conn.execute(
                select(outbox_events).where(outbox_events.c.topic == "llm.review.requested")
            )
            .mappings()
            .all()
        )
    assert len(rows) == 1
    request = rows[0]["payload"]["request"]
    assert request["stage"] == "POST_MARKET"
    assert request["context"]["session_metrics"]["signal_count"] == 1
    assert request["context"]["session_metrics"]["no_trade_count"] == 1


def test_nonterminal_order_blocks_post_market_provider_queue() -> None:
    engine = _engine()
    _seed_session(engine, closed=True)
    _seed_signal(engine)
    _seed_event(engine)
    with engine.begin() as conn:
        conn.execute(
            orders.insert().values(
                order_id="order-1",
                plan_id="plan-1",
                session_id=SESSION,
                idempotency_key="idem-1",
                status="WORKING",
                side="BUY",
                quantity=1,
                filled_quantity=0,
                state_version=1,
                limit_price=1,
                broker_order_id="broker-1",
                payload={"broker_id": "ibkr", "residual_exposure": False},
                created_at_utc=OPEN,
                updated_at_utc=CLOSE,
            )
        )
    run = schedule_post_market_review(
        engine,
        DAY,
        now=CLOSE + timedelta(minutes=2),
        rule_version="rules-test",
        grace_seconds=60,
    )
    assert run.state == "WAITING_INERT"
    assert run.inert_reason_code == "ORDERS_NOT_TERMINAL"


def test_split_leg_broker_identity_requires_same_session_reconciliation() -> None:
    engine = _engine()
    _seed_session(engine, closed=True)
    _seed_signal(engine)
    _seed_event(engine)
    with engine.begin() as conn:
        conn.execute(
            orders.insert().values(
                order_id="split-order-1",
                plan_id="plan-1",
                session_id=SESSION,
                idempotency_key="split-idem-1",
                status="FILLED",
                side="COMBO",
                quantity=1,
                filled_quantity=1,
                state_version=5,
                limit_price=1,
                broker_order_id=None,
                payload={
                    "broker_id": "longbridge",
                    "broker_order_id": None,
                    "broker_child_order_ids": ["buy-child", "sell-child"],
                    "residual_exposure": False,
                },
                created_at_utc=OPEN,
                updated_at_utc=CLOSE,
            )
        )
    blocked = schedule_post_market_review(
        engine,
        DAY,
        now=CLOSE + timedelta(minutes=2),
        rule_version="rules-test",
        grace_seconds=60,
    )
    assert blocked.inert_reason_code == "BROKER_RECONCILIATION_MISSING"
    with engine.begin() as conn:
        conn.execute(
            broker_snapshots.insert().values(
                session_id=SESSION,
                occurred_at_utc=CLOSE + timedelta(seconds=30),
                broker_health="HEALTHY",
                buying_power=1000,
                payload={},
                created_at_utc=CLOSE + timedelta(seconds=30),
                broker_id="longbridge",
                snapshot_sequence=1,
                snapshot_hash="e" * 64,
                net_liquidation=1000,
                reconciled=True,
                mismatch_codes=[],
            )
        )
    ready = schedule_post_market_review(
        engine,
        DAY,
        now=CLOSE + timedelta(minutes=2),
        rule_version="rules-test",
        grace_seconds=60,
    )
    assert ready.state == "ENQUEUED"


def test_post_market_aggregates_plan_order_fill_risk_and_requires_fresh_broker_proof() -> None:
    engine = _engine()
    _seed_session(engine, closed=True)
    _seed_signal(engine)
    _seed_event(engine)
    with engine.begin() as conn:
        conn.execute(
            candidate_trade_plans.insert().values(
                plan_id="plan-1",
                signal_id="signal-1",
                session_id=SESSION,
                status="FILLED",
                strategy_kind="LongGamma",
                plan_hash="a" * 64,
                idempotency_key="plan-idem-1",
                execution_mode="PAPER",
                expires_at_utc=CLOSE,
                created_at_utc=OPEN + timedelta(minutes=30),
                payload={"plan_id": "plan-1", "strategy_kind": "LongGamma"},
            )
        )
        conn.execute(
            risk_decisions.insert().values(
                plan_id="plan-1",
                session_id=SESSION,
                occurred_at_utc=OPEN + timedelta(minutes=31),
                decision="APPROVED",
                reason_code=None,
                payload={"decision_id": "risk-1", "decision": "APPROVED"},
                created_at_utc=OPEN + timedelta(minutes=31),
            )
        )
        conn.execute(
            orders.insert().values(
                order_id="order-1",
                plan_id="plan-1",
                session_id=SESSION,
                idempotency_key="order-idem-1",
                status="FILLED",
                side="BUY",
                quantity=1,
                filled_quantity=1,
                state_version=4,
                limit_price=1,
                broker_order_id="broker-order-1",
                payload={"broker_id": "ibkr", "residual_exposure": False},
                created_at_utc=OPEN + timedelta(minutes=32),
                updated_at_utc=CLOSE,
            )
        )
        conn.execute(
            fills.insert().values(
                fill_id="ibkr:fill-1",
                order_id="order-1",
                session_id=SESSION,
                occurred_at_utc=CLOSE - timedelta(minutes=1),
                quantity=1,
                price=1,
                payload={"fill_id": "fill-1"},
                created_at_utc=CLOSE,
                broker_id="ibkr",
                broker_order_id="broker-order-1",
                contract_id="QQQ-20260722-C-500",
                side="BUY",
                snapshot_hash="b" * 64,
            )
        )
        conn.execute(
            broker_snapshots.insert().values(
                session_id=SESSION,
                occurred_at_utc=CLOSE - timedelta(seconds=1),
                broker_health="HEALTHY",
                buying_power=1000,
                payload={},
                created_at_utc=CLOSE,
                broker_id="ibkr",
                snapshot_sequence=1,
                snapshot_hash="b" * 64,
                net_liquidation=1000,
                reconciled=True,
                mismatch_codes=[],
            )
        )
        conn.execute(
            trading_sessions.insert().values(
                session_id="another-session",
                trading_date=DAY + timedelta(days=1),
                status="CLOSED",
                opened_at_utc=OPEN + timedelta(days=1),
                closed_at_utc=CLOSE + timedelta(days=1),
                created_at_utc=OPEN + timedelta(days=1),
            )
        )
        conn.execute(
            broker_snapshots.insert().values(
                session_id="another-session",
                occurred_at_utc=CLOSE + timedelta(seconds=10),
                broker_health="HEALTHY",
                buying_power=1000,
                payload={},
                created_at_utc=CLOSE + timedelta(seconds=10),
                broker_id="ibkr",
                snapshot_sequence=2,
                snapshot_hash="d" * 64,
                net_liquidation=1000,
                reconciled=True,
                mismatch_codes=[],
            )
        )
    blocked = schedule_post_market_review(
        engine,
        DAY,
        now=CLOSE + timedelta(minutes=2),
        rule_version="rules-test",
        grace_seconds=60,
    )
    assert blocked.inert_reason_code == "BROKER_RECONCILIATION_INCOMPLETE"
    with engine.begin() as conn:
        conn.execute(
            broker_snapshots.insert().values(
                session_id=SESSION,
                occurred_at_utc=CLOSE + timedelta(seconds=30),
                broker_health="HEALTHY",
                buying_power=1000,
                payload={},
                created_at_utc=CLOSE + timedelta(seconds=30),
                broker_id="ibkr",
                snapshot_sequence=3,
                snapshot_hash="c" * 64,
                net_liquidation=1000,
                reconciled=True,
                mismatch_codes=[],
            )
        )
    ready = schedule_post_market_review(
        engine,
        DAY,
        now=CLOSE + timedelta(minutes=2),
        rule_version="rules-test",
        grace_seconds=60,
    )
    assert ready.state == "ENQUEUED"
    with engine.connect() as conn:
        payload = conn.execute(
            select(outbox_events.c.payload).where(outbox_events.c.event_id == ready.outbox_event_id)
        ).scalar_one()
    context = payload["request"]["context"]
    assert context["session_metrics"] == {
        "signal_count": 1,
        "plan_count": 1,
        "order_count": 1,
        "fill_count": 1,
        "risk_decision_count": 1,
        "event_context_count": 1,
        "no_trade_count": 1,
        "strategy_counts": {"NoTrade": 1},
        "order_status_counts": {"FILLED": 1},
        "risk_decision_counts": {"APPROVED": 1},
        "no_trade_reasons": ["deterministic entry conditions were incomplete"],
        "filled_contracts": "1",
    }
    assert {item["kind"] for item in context["recent_trades"]} == {"order", "fill"}


def test_intraday_trigger_queue_debounces_merges_and_rate_limits() -> None:
    engine = _engine()
    _seed_session(engine, closed=False)
    _seed_signal(engine)
    base = datetime(2026, 7, 22, 15, 0, tzinfo=UTC)
    with engine.begin() as conn:
        write_outbox(
            conn,
            source_event_id="audit-signal-1",
            topic="signal.persisted",
            aggregate_type="Signal",
            aggregate_id="signal-1",
            occurred_at_utc=base,
            payload={"signal_id": "signal-1", "session_id": SESSION},
            created_at_utc=base,
        )
        write_outbox(
            conn,
            source_event_id="audit-order-1",
            topic="order.projected",
            aggregate_type="ExecutionOrder",
            aggregate_id="order-missing",
            occurred_at_utc=base + timedelta(seconds=1),
            payload={"order_id": "order-missing", "session_id": SESSION},
            created_at_utc=base,
        )
    assert ingest_intraday_trigger_events(engine, now=base, debounce_seconds=5) == 2
    assert (
        enqueue_intraday_review(
            engine,
            now=base + timedelta(seconds=4),
            rule_version="rules-test",
            min_interval_seconds=60,
        )
        is None
    )
    first = enqueue_intraday_review(
        engine,
        now=base + timedelta(seconds=5),
        rule_version="rules-test",
        min_interval_seconds=60,
    )
    assert first is not None
    assert len(first.source_event_ids) == 2
    with engine.connect() as conn:
        assert set(conn.execute(select(llm_trigger_events.c.state)).scalars()) == {"MERGED"}
        request_rows = conn.execute(
            select(outbox_events).where(outbox_events.c.topic == "llm.review.requested")
        ).all()
    assert len(request_rows) == 1

    with engine.begin() as conn:
        write_outbox(
            conn,
            source_event_id="audit-signal-2",
            topic="signal.persisted",
            aggregate_type="Signal",
            aggregate_id="signal-1",
            occurred_at_utc=base + timedelta(seconds=10),
            payload={"signal_id": "signal-1", "session_id": SESSION, "version": 2},
            created_at_utc=base + timedelta(seconds=10),
        )
    assert (
        ingest_intraday_trigger_events(engine, now=base + timedelta(seconds=10), debounce_seconds=5)
        == 1
    )
    assert (
        enqueue_intraday_review(
            engine,
            now=base + timedelta(seconds=15),
            rule_version="rules-test",
            min_interval_seconds=60,
        )
        is None
    )
    second = enqueue_intraday_review(
        engine,
        now=base + timedelta(seconds=66),
        rule_version="rules-test",
        min_interval_seconds=60,
    )
    assert second is not None
    assert second.request_id != first.request_id


def test_intraday_trigger_queue_collapses_duplicate_state_payloads() -> None:
    engine = _engine()
    _seed_session(engine, closed=False)
    _seed_signal(engine)
    base = datetime(2026, 7, 22, 15, 0, tzinfo=UTC)
    payload = {"signal_id": "signal-1", "session_id": SESSION, "state_version": 1}
    with engine.begin() as conn:
        for suffix in ("first", "duplicate"):
            write_outbox(
                conn,
                source_event_id=f"audit-signal-{suffix}",
                topic="signal.persisted",
                aggregate_type="Signal",
                aggregate_id="signal-1",
                occurred_at_utc=base,
                payload=payload,
                created_at_utc=base,
            )
    assert ingest_intraday_trigger_events(engine, now=base, debounce_seconds=5) == 1
    with engine.connect() as conn:
        trigger_ids = conn.execute(select(llm_trigger_events.c.id)).scalars().all()
    assert len(trigger_ids) == 1


@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL is required")
def test_postgresql_intraday_enqueue_has_one_global_worker() -> None:
    raw_url = os.environ["DATABASE_URL"]
    engine = create_engine(
        raw_url.replace("postgresql://", "postgresql+psycopg://", 1), pool_size=5
    )
    suffix = uuid4().hex
    seed = int(suffix[:8], 16)
    trading_day = date(2050 + seed % 40, 1 + (seed // 40) % 12, 1 + (seed // 480) % 28)
    session_id = f"pg-intraday-{suffix}"
    now = datetime.combine(trading_day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=15)
    source_event_ids: list[str] = []
    run_outbox_event_id: str | None = None
    barrier = Barrier(2)
    try:
        with engine.begin() as conn:
            current_max = int(
                conn.execute(select(func.coalesce(func.max(outbox_events.c.id), 0))).scalar_one()
            )
            conn.execute(
                update(llm_event_cursors)
                .where(llm_event_cursors.c.cursor_name == "intraday-deterministic-state-v1")
                .values(last_outbox_id=current_max, updated_at_utc=now)
            )
            conn.execute(
                trading_sessions.insert().values(
                    session_id=session_id,
                    trading_date=trading_day,
                    status="OPEN",
                    opened_at_utc=now - timedelta(hours=1),
                    closed_at_utc=None,
                    created_at_utc=now - timedelta(hours=1),
                )
            )
            for version in (1, 2):
                source_event_ids.append(
                    write_outbox(
                        conn,
                        source_event_id=f"pg-signal-{suffix}-{version}",
                        topic="signal.persisted",
                        aggregate_type="Signal",
                        aggregate_id=f"signal-{suffix}",
                        occurred_at_utc=now,
                        payload={"session_id": session_id, "state_version": version},
                        created_at_utc=now,
                    )
                )
        assert ingest_intraday_trigger_events(engine, now=now, debounce_seconds=1) == 2

        def enqueue() -> AutomationRun | None:
            barrier.wait(timeout=3)
            return enqueue_intraday_review(
                engine,
                now=now + timedelta(seconds=1),
                rule_version="rules-pg-concurrency",
                min_interval_seconds=60,
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _item: enqueue(), range(2)))
        runs = [result for result in results if result is not None]
        assert len(runs) == 1
        run_outbox_event_id = runs[0].outbox_event_id
        with engine.connect() as conn:
            assert (
                conn.execute(
                    select(func.count())
                    .select_from(llm_automation_runs)
                    .where(llm_automation_runs.c.session_id == session_id)
                ).scalar_one()
                == 1
            )
            assert set(
                conn.execute(
                    select(llm_trigger_events.c.state).where(
                        llm_trigger_events.c.session_id == session_id
                    )
                ).scalars()
            ) == {"MERGED"}
    finally:
        with engine.begin() as conn:
            conn.execute(
                delete(llm_trigger_events).where(llm_trigger_events.c.session_id == session_id)
            )
            conn.execute(
                delete(llm_automation_runs).where(llm_automation_runs.c.session_id == session_id)
            )
            event_ids = source_event_ids + (
                [run_outbox_event_id] if run_outbox_event_id is not None else []
            )
            if event_ids:
                conn.execute(delete(outbox_events).where(outbox_events.c.event_id.in_(event_ids)))
            conn.execute(
                delete(trading_sessions).where(trading_sessions.c.session_id == session_id)
            )
        engine.dispose()


def test_outbox_worker_persists_inert_review_and_acks_without_provider() -> None:
    engine = _engine()
    _seed_session(engine, closed=True)
    _seed_signal(engine)
    _seed_event(engine)
    now = CLOSE + timedelta(minutes=2)
    run = schedule_post_market_review(
        engine, DAY, now=now, rule_version="rules-test", grace_seconds=60
    )
    settings = LLMAutomationSettings(
        enabled=True,
        poll_seconds=1,
        post_market_grace_seconds=60,
        intraday_debounce_seconds=5,
        intraday_min_interval_seconds=60,
        max_attempts=3,
    )
    supervisor = LLMAutomationSupervisor(
        engine,
        LLMReviewService(LLMSettings.from_env({}), now=lambda: now),
        settings,
        rule_version="rules-test",
        now=lambda: now,
        worker_id="automation-test",
    )
    assert asyncio.run(supervisor.run_once()) == 1
    with engine.connect() as conn:
        review = conn.execute(select(llm_reviews)).mappings().one()
        stored_run = (
            conn.execute(
                select(llm_automation_runs).where(llm_automation_runs.c.run_id == run.run_id)
            )
            .mappings()
            .one()
        )
        queued = (
            conn.execute(
                select(outbox_events).where(outbox_events.c.event_id == run.outbox_event_id)
            )
            .mappings()
            .one()
        )
    assert review["review_status"] == "UNAVAILABLE"
    assert review["unavailable_reason_code"] == "CONFIG_MISSING"
    assert stored_run["state"] == "COMPLETED"
    assert queued["published_at_utc"] is not None


def test_successful_post_market_restart_does_not_call_provider_twice() -> None:
    engine = _engine()
    _seed_session(engine, closed=True)
    _seed_signal(engine)
    _seed_event(engine)
    now = CLOSE + timedelta(minutes=2)
    schedule_post_market_review(engine, DAY, now=now, rule_version="rules-test", grace_seconds=60)
    provider = _PostMarketProvider()
    settings = LLMAutomationSettings(True, 1, 60, 5, 60, 3)
    service = LLMReviewService(_configured_llm_settings(), provider, now=lambda: now)
    first = LLMAutomationSupervisor(
        engine,
        service,
        settings,
        rule_version="rules-test",
        now=lambda: now,
        worker_id="automation-first",
    )
    second = LLMAutomationSupervisor(
        engine,
        service,
        settings,
        rule_version="rules-test",
        now=lambda: now,
        worker_id="automation-restarted",
    )
    assert asyncio.run(first.run_once()) == 1
    assert asyncio.run(second.run_once()) == 0
    assert provider.calls == 1
    with engine.connect() as conn:
        assert conn.execute(select(llm_reviews.c.review_id)).scalars().all()
        assert conn.execute(select(daily_reviews.c.review_id)).scalars().all()


def test_malformed_automation_outbox_dead_letters_without_provider_call() -> None:
    engine = _engine()
    _seed_session(engine, closed=True)
    _seed_signal(engine)
    _seed_event(engine)
    now = CLOSE + timedelta(minutes=2)
    run = schedule_post_market_review(
        engine, DAY, now=now, rule_version="rules-test", grace_seconds=60
    )
    with engine.begin() as conn:
        conn.execute(
            outbox_events.update()
            .where(outbox_events.c.event_id == run.outbox_event_id)
            .values(payload={"run_id": run.run_id, "request": {"bad": "shape"}})
        )
    provider = _PostMarketProvider()
    supervisor = LLMAutomationSupervisor(
        engine,
        LLMReviewService(_configured_llm_settings(), provider, now=lambda: now),
        LLMAutomationSettings(True, 1, 60, 5, 60, 1),
        rule_version="rules-test",
        now=lambda: now,
        worker_id="automation-dead-letter",
    )
    assert asyncio.run(supervisor.run_once()) == 0
    with engine.connect() as conn:
        queued = (
            conn.execute(
                select(outbox_events).where(outbox_events.c.event_id == run.outbox_event_id)
            )
            .mappings()
            .one()
        )
        stored_run = (
            conn.execute(
                select(llm_automation_runs).where(llm_automation_runs.c.run_id == run.run_id)
            )
            .mappings()
            .one()
        )
    assert queued["dead_lettered_at_utc"] is not None
    assert queued["last_error_code"] == "AUTOMATION_PAYLOAD_INVALID"
    assert stored_run["state"] == "DEAD_LETTERED"
    assert provider.calls == 0


def test_tampered_valid_automation_payload_dead_letters_without_provider_call() -> None:
    engine = _engine()
    _seed_session(engine, closed=True)
    _seed_signal(engine)
    _seed_event(engine)
    now = CLOSE + timedelta(minutes=2)
    run = schedule_post_market_review(
        engine, DAY, now=now, rule_version="rules-test", grace_seconds=60
    )
    with engine.begin() as conn:
        payload = conn.execute(
            select(outbox_events.c.payload).where(outbox_events.c.event_id == run.outbox_event_id)
        ).scalar_one()
        payload["request"]["rule_version"] = "tampered-rules"
        conn.execute(
            outbox_events.update()
            .where(outbox_events.c.event_id == run.outbox_event_id)
            .values(payload=payload)
        )
    provider = _PostMarketProvider()
    supervisor = LLMAutomationSupervisor(
        engine,
        LLMReviewService(_configured_llm_settings(), provider, now=lambda: now),
        LLMAutomationSettings(True, 1, 60, 5, 60, 1),
        rule_version="rules-test",
        now=lambda: now,
        worker_id="automation-tampered",
    )
    assert asyncio.run(supervisor.run_once()) == 0
    with engine.connect() as conn:
        queued = (
            conn.execute(
                select(outbox_events).where(outbox_events.c.event_id == run.outbox_event_id)
            )
            .mappings()
            .one()
        )
    assert queued["dead_lettered_at_utc"] is not None
    assert queued["last_error_code"] == "AUTOMATION_PAYLOAD_INVALID"
    assert provider.calls == 0


def test_automation_supervisor_cancels_without_draining_queue() -> None:
    engine = _engine()
    settings = LLMAutomationSettings(True, 60, 60, 5, 60, 3)
    supervisor = LLMAutomationSupervisor(
        engine,
        LLMReviewService(LLMSettings.from_env({})),
        settings,
        rule_version="rules-test",
        worker_id="automation-cancel",
    )

    async def cancel() -> None:
        task = asyncio.create_task(supervisor.serve())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=0.5)
        except asyncio.CancelledError:
            pass
        assert supervisor.status().running is False

    asyncio.run(cancel())
