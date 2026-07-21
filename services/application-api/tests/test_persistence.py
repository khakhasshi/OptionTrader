"""P1-7: signal + No-Trade reason persistence to review/audit.

Serialization is tested purely; the transactional write-path runs against an
in-memory SQLite whose ``trading``/``audit`` schemas are ATTACHed, so the
schema-qualified inserts and the single-transaction guarantee are exercised
without a live Postgres.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
from threading import Barrier, Event, current_thread
from typing import Any, cast
from uuid import uuid4

from jsonschema import Draft202012Validator
from cryptography.fernet import Fernet
import pytest
from referencing import Registry, Resource
from sqlalchemy import create_engine, event, select, text, update
from sqlalchemy.engine import Engine

from app.persistence import (
    SignalContext,
    audit_events,
    broker_snapshots,
    candidate_trade_plans,
    claim_outbox_batch,
    claim_confirmation_intent,
    confirmation_capabilities,
    daily_reviews,
    build_signal_contract,
    build_signal_rows,
    event_contexts,
    fills,
    get_llm_review,
    get_llm_review_by_request_id,
    latest_daily_review,
    llm_reviews,
    list_rule_hypotheses,
    metadata,
    mark_outbox_published,
    order_events,
    orders,
    outbox_events,
    position_snapshots,
    persist_broker_reconciliation,
    persist_order_projection,
    persist_signal,
    persist_event_context,
    persist_llm_review,
    persist_staged_candidate,
    restorable_execution_workflow,
    reschedule_outbox_message,
    risk_decisions,
    rule_hypotheses,
    rotate_confirmation_capabilities,
    signals,
    verified_initial_risk_context,
)
from app.llm.models import (
    DailyReviewDetail,
    LLMReview,
    LLMReviewRequest,
    ProviderMetadata,
    ReviewContext,
    RuleHypothesis,
)
from app.grpc_gen import broker_pb2, execution_pb2
from app.persistence import repository
from app.events import unavailable_event_context
from app.regime import CHAOS, EVENT, NO_TRADE as REGIME_NO_TRADE, RANGE, TREND, RegimeState
from app.strategy import (
    EVENT_VOL_CRUSH,
    LONG_GAMMA,
    NO_TRADE,
    SHORT_PREMIUM,
    StrategyDecision,
)
from app.vol import IV_CHEAP, IV_RICH, VolState
from app.trading.models import (
    CandidateTradePlan,
    ExecutionOrder,
    RiskDecision,
    StageCandidateResult,
)
from app.trading.capability import ConfirmationCipher

UTC = timezone.utc
_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture()
def confirmation_cipher() -> ConfirmationCipher:
    return ConfirmationCipher(Fernet.generate_key().decode("ascii"))


def _signal_validator() -> Draft202012Validator:
    schema_dir = _ROOT / "packages/contracts/jsonschema"
    resources = {
        path.name: Resource.from_contents(json.loads(path.read_text()))
        for path in schema_dir.glob("*.json")
    }
    registry = Registry().with_resources(list(resources.items()))
    return Draft202012Validator(resources["signal.json"].contents, registry=registry)


def _regime(kind: str = TREND) -> RegimeState:
    return RegimeState(
        regime=kind,
        trend_score=6,
        range_score=1,
        components={"vwap_side": 2, "adx": 2},
        unavailable=["volume_vs_20d"],
    )


def _vol(state: str = IV_CHEAP) -> VolState:
    return VolState(
        iv_hv_state=state,
        interpretation="Long Vol",
        atm_iv=0.18,
        hv_20=0.12,
        hv_60=0.14,
        iv_hv_ratio=1.5,
        implied_move=0.01,
        realized_move=0.015,
        realized_implied_ratio=1.5,
        straddle_mark=5.0,
        unavailable=[],
    )


def _decision(playbook: str = LONG_GAMMA) -> StrategyDecision:
    return StrategyDecision(
        playbook=playbook,
        reason="Trend + IV cheap/fair + breakout in allowed window",
        risk_status="PASS_READONLY",
        risk_notes=["risk limits UNCONFIRMED (ASSUMPTIONS Q3): placeholder only"],
        limits_unconfirmed=True,
    )


def _ctx(signal_id: str = "sig-1") -> SignalContext:
    return SignalContext(
        signal_id=signal_id,
        session_id="2026-07-09",
        occurred_at_utc=datetime(2026, 7, 9, 13, 45, tzinfo=UTC),
        rule_version="rules_p1_1.0.0",
    )


# ------------------------------- serialization -------------------------------


def test_serialize_traded_signal_has_no_no_trade_reason() -> None:
    sig, audit = build_signal_rows(_ctx(), _regime(), _vol(), _decision(LONG_GAMMA))
    assert sig["strategy_kind"] == "LongGamma"
    assert sig["no_trade_reason"] is None
    assert sig["regime"] == "Trend"
    assert sig["vol_state"] == IV_CHEAP
    payload = cast(dict[str, Any], sig["payload"])
    assert payload["regime"]["trend_score"] == 6
    assert payload["vol"]["hv_60"] == 0.14
    assert list(_signal_validator().iter_errors(payload["signal"])) == []
    assert audit["action"] == "SIGNAL_EMITTED"
    assert audit["to_status"] == "LongGamma"
    assert audit["entity_id"] == "sig-1"


def test_serialize_no_trade_records_reason() -> None:
    decision = StrategyDecision(
        playbook=NO_TRADE,
        reason="Trend but no confirmed opening-range breakout",
        risk_status="PASS_READONLY",
        risk_notes=[],
    )
    sig, _ = build_signal_rows(_ctx(), _regime(), _vol(), decision)
    assert sig["strategy_kind"] == "NoTrade"
    assert sig["no_trade_reason"] == "Trend but no confirmed opening-range breakout"


def test_serialize_captures_unavailable_inputs() -> None:
    sig, _ = build_signal_rows(_ctx(), _regime(), _vol(), _decision())
    payload = cast(dict[str, Any], sig["payload"])
    assert payload["regime"]["unavailable"] == ["volume_vs_20d"]


@pytest.mark.parametrize(
    ("label", "contract_value"),
    [
        (TREND, "Trend"),
        (RANGE, "Range"),
        (EVENT, "Event"),
        (CHAOS, "Chaos"),
        (REGIME_NO_TRADE, "NoTrade"),
    ],
)
def test_all_regime_labels_map_to_contract(label: str, contract_value: str) -> None:
    signal = build_signal_contract(_ctx(), _regime(label), _decision())
    assert signal["regime"] == contract_value


@pytest.mark.parametrize(
    ("label", "contract_value"),
    [
        (LONG_GAMMA, "LongGamma"),
        (SHORT_PREMIUM, "ShortPremium"),
        (EVENT_VOL_CRUSH, "EventVolCrush"),
        (NO_TRADE, "NoTrade"),
    ],
)
def test_all_strategy_labels_map_to_contract(label: str, contract_value: str) -> None:
    signal = build_signal_contract(_ctx(), _regime(), _decision(label))
    assert signal["strategy"] == contract_value


@pytest.mark.parametrize(
    ("risk_status", "contract_value"),
    [
        ("PASS_READONLY", "PASSED"),
        ("BLOCKED", "REJECTED"),
        ("NOT_EVALUATED", "NOT_EVALUATED"),
    ],
)
def test_all_initial_risk_labels_map_to_contract(risk_status: str, contract_value: str) -> None:
    decision = StrategyDecision(
        playbook=LONG_GAMMA,
        reason="contract mapping",
        risk_status=risk_status,
        risk_notes=[],
    )
    signal = build_signal_contract(_ctx(), _regime(), decision)
    assert signal["initial_risk_status"] == contract_value


def test_signal_contract_has_required_shape_and_validates_schema() -> None:
    signal = build_signal_contract(_ctx(), _regime(), _decision())
    assert set(signal) == {
        "schema_version",
        "signal_id",
        "session_id",
        "occurred_at_utc",
        "regime",
        "strategy",
        "initial_risk_status",
        "reason",
        "rule_version",
    }
    assert list(_signal_validator().iter_errors(signal)) == []


def test_unmapped_contract_labels_fail_closed() -> None:
    with pytest.raises(ValueError, match="unmapped regime"):
        build_signal_contract(_ctx(), _regime("Trendish"), _decision())
    with pytest.raises(ValueError, match="unmapped strategy"):
        build_signal_contract(_ctx(), _regime(), _decision("Gamma Maybe"))
    unknown_risk = StrategyDecision(
        playbook=LONG_GAMMA,
        reason="bad risk status",
        risk_status="MAYBE",
        risk_notes=[],
    )
    with pytest.raises(ValueError, match="unmapped initial risk status"):
        build_signal_contract(_ctx(), _regime(), unknown_risk)


def test_signal_contract_requires_rule_version() -> None:
    ctx = SignalContext("sig-1", "session-1", datetime(2026, 7, 9, tzinfo=UTC), "")
    with pytest.raises(ValueError, match="rule_version"):
        build_signal_contract(ctx, _regime(), _decision())


def test_serialize_rejects_naive_timestamp() -> None:
    ctx = SignalContext("sig-1", "2026-07-09", datetime(2026, 7, 9, 13, 45), "rules-test")
    with pytest.raises(ValueError, match="timezone-aware"):
        build_signal_rows(ctx, _regime(), _vol(), _decision())


def test_serialize_rejects_non_utc_timestamp() -> None:
    from datetime import timedelta

    est = timezone(timedelta(hours=-5))
    ctx = SignalContext(
        "sig-1", "2026-07-09", datetime(2026, 7, 9, 8, 45, tzinfo=est), "rules-test"
    )
    with pytest.raises(ValueError, match="must be UTC"):
        build_signal_rows(ctx, _regime(), _vol(), _decision())


# ------------------------------- write-path ----------------------------------


@pytest.fixture()
def engine() -> Engine:
    """In-memory SQLite with trading/audit schemas attached, mirror tables built.

    SQLAlchemy renders ``trading.signals`` as a schema reference; SQLite treats
    ATTACHed databases as schemas, so this exercises the real qualified inserts.
    """
    eng = create_engine("sqlite://")

    @event.listens_for(eng, "connect")
    def _attach(dbapi_conn: Any, _rec: Any) -> None:
        cur = dbapi_conn.cursor()
        cur.execute("ATTACH DATABASE ':memory:' AS trading")
        cur.execute("ATTACH DATABASE ':memory:' AS audit")
        cur.execute("ATTACH DATABASE ':memory:' AS events")
        cur.execute("ATTACH DATABASE ':memory:' AS risk")
        cur.execute("ATTACH DATABASE ':memory:' AS review")
        cur.close()

    metadata.create_all(eng)
    return eng


def test_persist_event_context_writes_context_and_audit_idempotently(engine: Engine) -> None:
    context = unavailable_event_context(
        datetime(2026, 7, 20, 13, 45, tzinfo=UTC), "calendar not loaded"
    )
    assert persist_event_context(engine, "sess-events", context) is True
    assert persist_event_context(engine, "sess-events", context) is False

    with engine.connect() as conn:
        event_rows = conn.execute(select(event_contexts)).mappings().all()
        audits = (
            conn.execute(select(audit_events).where(audit_events.c.action == "EVENT_CONTEXT_BUILT"))
            .mappings()
            .all()
        )
    assert len(event_rows) == 1
    assert event_rows[0]["category"] == "HighRisk"
    assert event_rows[0]["payload"]["available"] is False
    assert len(audits) == 1
    assert audits[0]["to_status"] == "UNAVAILABLE"


def _staged_models() -> tuple[CandidateTradePlan, StageCandidateResult]:
    fixture_dir = _ROOT / "packages/contracts/fixtures"
    plan = CandidateTradePlan.model_validate(
        json.loads((fixture_dir / "candidate_trade_plan.sample.json").read_text())
    )
    decision = RiskDecision.model_validate(
        json.loads((fixture_dir / "risk_decision.sample.json").read_text())
    )
    order = ExecutionOrder(
        schema_version="1.1",
        order_id="order_demo_001",
        plan_id=plan.plan_id,
        plan_hash=plan.plan_hash,
        idempotency_key=plan.idempotency_key,
        session_id=plan.session_id,
        broker_id=plan.broker_id,
        execution_mode=plan.execution_mode,
        state="AWAITING_CONFIRMATION",
        total_quantity=1,
        filled_quantity=0,
        broker_order_id=None,
        expires_at_utc=plan.expires_at_utc,
        updated_at_utc="2026-07-20T14:30:01Z",
        state_version=1,
        broker_child_order_ids=[],
        broker_child_orders=[],
        residual_exposure=False,
        risk_reason_codes=[],
    )
    return plan, StageCandidateResult(
        initial_risk_decision=decision,
        order=order,
        confirmation_token="never-persist-this",
    )


def test_execution_persistence_is_atomic_idempotent_and_encrypts_token(
    engine: Engine, confirmation_cipher: ConfirmationCipher
) -> None:
    plan, result = _staged_models()
    assert persist_staged_candidate(engine, plan, result, confirmation_cipher) is True
    assert persist_staged_candidate(engine, plan, result, confirmation_cipher) is False

    with engine.connect() as conn:
        plan_row = conn.execute(select(candidate_trade_plans)).mappings().one()
        risk_row = conn.execute(select(risk_decisions)).mappings().one()
        order_row = conn.execute(select(orders)).mappings().one()
        capability_row = conn.execute(select(confirmation_capabilities)).mappings().one()
        event_row = conn.execute(select(order_events)).mappings().one()
        audit_rows = conn.execute(select(audit_events)).mappings().all()
        outbox_topics = set(conn.execute(select(outbox_events.c.topic)).scalars())
    serialized = json.dumps(
        [
            plan_row["payload"],
            risk_row["payload"],
            event_row["payload"],
            [row["payload"] for row in audit_rows],
        ]
    )
    assert "never-persist-this" not in serialized
    assert capability_row["token_ciphertext"] != "never-persist-this"
    assert confirmation_cipher.decrypt(capability_row["token_ciphertext"]) == "never-persist-this"
    assert order_row["status"] == "AWAITING_CONFIRMATION"
    assert order_row["state_version"] == 1
    assert outbox_topics == {"candidate.staged", "risk.decision_recorded", "order.staged"}


def _llm_request() -> LLMReviewRequest:
    return LLMReviewRequest.model_validate(
        json.loads(
            (_ROOT / "packages/contracts/fixtures/llm_review_request.sample.json").read_text()
        )
    )


def _llm_review() -> LLMReview:
    return LLMReview.model_validate(
        json.loads((_ROOT / "packages/contracts/fixtures/llm_review.completed.json").read_text())
    )


def test_llm_review_persistence_is_atomic_idempotent_and_audited(engine: Engine) -> None:
    request = _llm_request()
    review = _llm_review()
    assert persist_llm_review(engine, request, review) is True
    assert persist_llm_review(engine, request, review) is False

    with engine.connect() as conn:
        stored = conn.execute(select(llm_reviews)).mappings().one()
        audit = (
            conn.execute(select(audit_events).where(audit_events.c.entity_id == review.review_id))
            .mappings()
            .one()
        )
        outbox = (
            conn.execute(
                select(outbox_events).where(outbox_events.c.aggregate_id == review.review_id)
            )
            .mappings()
            .one()
        )
    assert stored["review_status"] == "COMPLETED"
    assert stored["input_hash"] == review.provider.input_hash
    assert audit["payload"]["input_summary"]["prompt_version"] == "phase4-review-v3"
    assert outbox["topic"] == "llm.review_recorded"
    assert get_llm_review(engine, review.review_id) == review
    assert get_llm_review_by_request_id(engine, review.request_id) == review


def test_pre_execution_review_reads_authoritative_initial_risk(
    engine: Engine, confirmation_cipher: ConfirmationCipher
) -> None:
    plan, staged = _staged_models()
    assert persist_staged_candidate(engine, plan, staged, confirmation_cipher)
    authoritative = verified_initial_risk_context(engine, plan.plan_id, plan.plan_hash)
    assert authoritative == (plan, staged.initial_risk_decision)
    assert verified_initial_risk_context(engine, plan.plan_id, "f" * 64) is None


def test_post_market_review_creates_daily_review_and_research_only_queue(engine: Engine) -> None:
    request = LLMReviewRequest(
        schema_version="1.0",
        request_id="post_market_request_001",
        correlation_id="session_2026-07-20",
        causation_id=None,
        session_id="session_2026-07-20",
        occurred_at_utc="2026-07-20T20:01:00Z",
        received_at_utc="2026-07-20T20:01:01Z",
        source="application-service",
        source_sequence=99,
        rule_version="rules_p3_1.0.0",
        stage="POST_MARKET",
        trading_date="2026-07-20",
        plan_id=None,
        plan_hash=None,
        context=ReviewContext(
            session_metrics={"trades": 1, "realized_pnl": "-10.00"},
            deterministic_summary="One fully audited paper trade.",
        ),
        source_refs=[],
    )
    hypothesis = RuleHypothesis(
        title="Test a narrower entry window",
        rationale="One audited observation suggests a research question.",
        validation_plan="Run cost-aware walk-forward and out-of-sample replay.",
        evidence_ids=[],
        status="RESEARCH_ONLY",
        activation_allowed=False,
    )
    review = LLMReview(
        schema_version="1.0",
        review_id="post_market_review_001",
        request_id=request.request_id,
        correlation_id=request.correlation_id,
        causation_id=None,
        session_id=request.session_id,
        occurred_at_utc=request.occurred_at_utc,
        received_at_utc="2026-07-20T20:01:03Z",
        source="llm-intelligence-layer",
        source_sequence=request.source_sequence,
        rule_version=request.rule_version,
        stage="POST_MARKET",
        trading_date=request.trading_date,
        plan_id=None,
        plan_hash=None,
        review_status="COMPLETED",
        summary="The loss respected the deterministic stop.",
        decision_support="Review only.",
        sop_alignment="Aligned",
        risk_notes=[],
        invalidations=[],
        recommended_action="Review Only",
        confidence=0.6,
        rule_references=[],
        evidence_citations=[],
        daily_review=DailyReviewDetail(
            best_trade=None,
            worst_trade="One stopped paper trade.",
            good_losses=["The stop was respected."],
            bad_losses=[],
            sop_violations=[],
            loss_attribution=[],
            one_change_tomorrow="Do not change production rules from one sample.",
        ),
        rule_hypotheses=[hypothesis],
        unavailable_reason_code=None,
        provider=ProviderMetadata(
            provider="deepseek-openai",
            model="deepseek-v4-flash",
            provider_request_id="provider-post-1",
            prompt_version="phase4-review-v3",
            input_hash="d" * 64,
            latency_ms=50,
            attempts=1,
            cache_hit=False,
            input_tokens=500,
            output_tokens=200,
            estimated_cost_usd="0.000126",
        ),
        source_refs=[],
    )
    assert persist_llm_review(engine, request, review)
    assert latest_daily_review(engine, date(2026, 7, 20)) == review
    hypotheses = list_rule_hypotheses(engine)
    assert len(hypotheses) == 1
    assert hypotheses[0].status == "PENDING_RESEARCH"
    assert hypotheses[0].activation_allowed is False
    with engine.connect() as conn:
        assert conn.execute(select(daily_reviews)).mappings().one()["review_id"] == review.review_id
        assert conn.execute(select(rule_hypotheses)).mappings().one()["activation_allowed"] is False


def test_outbox_is_transactional_leased_and_at_least_once(engine: Engine) -> None:
    assert persist_signal(engine, _ctx(), _regime(), _vol(), _decision())

    claimed_at = datetime.now(UTC) + timedelta(seconds=1)
    first = claim_outbox_batch(engine, "worker-a", limit=1, lease_seconds=30, now=claimed_at)
    assert [message.topic for message in first] == ["signal.persisted"]
    assert first[0].attempts == 1
    assert claim_outbox_batch(engine, "worker-b", now=claimed_at) == []
    assert not mark_outbox_published(
        engine, first[0].event_id, "worker-b", now=claimed_at + timedelta(seconds=1)
    )
    assert reschedule_outbox_message(
        engine,
        first[0].event_id,
        "worker-a",
        "DOWNSTREAM_UNAVAILABLE",
        retry_delay_seconds=5,
        now=claimed_at + timedelta(seconds=1),
    )
    assert claim_outbox_batch(engine, "worker-b", now=claimed_at + timedelta(seconds=5)) == []
    second = claim_outbox_batch(engine, "worker-b", now=claimed_at + timedelta(seconds=6))
    assert [message.event_id for message in second] == [first[0].event_id]
    assert second[0].attempts == 2
    assert mark_outbox_published(
        engine, second[0].event_id, "worker-b", now=claimed_at + timedelta(seconds=7)
    )
    assert claim_outbox_batch(engine, "worker-c", now=claimed_at + timedelta(minutes=1)) == []


def test_outbox_dead_letters_after_bounded_attempts(engine: Engine) -> None:
    assert persist_signal(engine, _ctx(), _regime(), _vol(), _decision())
    now = datetime.now(UTC) + timedelta(seconds=1)
    message = claim_outbox_batch(engine, "worker", limit=1, now=now)[0]
    assert reschedule_outbox_message(
        engine,
        message.event_id,
        "worker",
        "PERMANENT_FAILURE",
        max_attempts=1,
        now=now + timedelta(seconds=1),
    )
    with engine.connect() as conn:
        row = (
            conn.execute(select(outbox_events).where(outbox_events.c.event_id == message.event_id))
            .mappings()
            .one()
        )
    assert row["dead_lettered_at_utc"] is not None
    assert row["last_error_code"] == "PERMANENT_FAILURE"
    assert claim_outbox_batch(engine, "other", now=now + timedelta(minutes=1)) == []


def test_repeated_reconciliation_failures_have_distinct_outbox_identity(engine: Engine) -> None:
    repository.persist_broker_reconciliation_failure(engine, "ibkr", "BROKER_RPC_FAILURE")
    repository.persist_broker_reconciliation_failure(engine, "ibkr", "BROKER_RPC_FAILURE")
    with engine.connect() as conn:
        rows = (
            conn.execute(
                select(outbox_events).where(outbox_events.c.topic == "broker.reconciliation_failed")
            )
            .mappings()
            .all()
        )
    assert len(rows) == 2
    assert len({row["event_id"] for row in rows}) == 2


def test_confirmation_capabilities_rotate_atomically_to_primary_key(engine: Engine) -> None:
    old_key = Fernet.generate_key().decode("ascii")
    new_key = Fernet.generate_key().decode("ascii")
    old_cipher = ConfirmationCipher(old_key)
    primary_cipher = ConfirmationCipher(new_key)
    ring = ConfirmationCipher(f"{new_key},{old_key}")
    plan, result = _staged_models()
    assert persist_staged_candidate(engine, plan, result, old_cipher)

    assert rotate_confirmation_capabilities(engine, ring) == 1
    with engine.connect() as conn:
        ciphertext = conn.execute(select(confirmation_capabilities.c.token_ciphertext)).scalar_one()
    assert primary_cipher.decrypt(ciphertext) == "never-persist-this"
    with pytest.raises(ValueError, match="cannot be decrypted"):
        old_cipher.decrypt(ciphertext)
    assert rotate_confirmation_capabilities(engine, ring) == 0
    with engine.connect() as conn:
        assert (
            conn.execute(select(confirmation_capabilities.c.token_ciphertext)).scalar_one()
            == ciphertext
        )


def test_confirmation_rotation_rejects_corrupt_ciphertext_without_partial_success(
    engine: Engine,
) -> None:
    old_key = Fernet.generate_key().decode("ascii")
    new_key = Fernet.generate_key().decode("ascii")
    plan, result = _staged_models()
    assert persist_staged_candidate(engine, plan, result, ConfirmationCipher(old_key))
    with engine.begin() as conn:
        conn.execute(
            update(confirmation_capabilities).values(token_ciphertext="corrupt-ciphertext")
        )
    with pytest.raises(ValueError, match="cannot be rotated"):
        rotate_confirmation_capabilities(engine, ConfirmationCipher(f"{new_key},{old_key}"))
    with engine.connect() as conn:
        assert (
            conn.execute(select(confirmation_capabilities.c.token_ciphertext)).scalar_one()
            == "corrupt-ciphertext"
        )


def _broker_batch(snapshot: Any) -> Any:
    raw = snapshot.SerializeToString(deterministic=True)
    return execution_pb2.BrokerReconciliationBatch(
        schema_version="1.0",
        broker_id=execution_pb2.BROKER_ID_IBKR,
        snapshot_sequence=snapshot.snapshot_sequence,
        snapshot_hash=sha256(raw).hexdigest(),
        snapshot_protobuf=raw,
        expires_at_utc="2099-07-21T14:31:00Z",
    )


def test_broker_fact_ledger_persists_exact_snapshot_and_is_idempotent(
    engine: Engine, confirmation_cipher: ConfirmationCipher
) -> None:
    plan, result = _staged_models()
    persist_staged_candidate(engine, plan, result, confirmation_cipher)
    assert result.order is not None
    working = result.order.model_copy(
        update={
            "state": "WORKING",
            "state_version": 4,
            "broker_order_id": "900",
            "updated_at_utc": "2026-07-21T14:30:00Z",
            "residual_exposure": True,
        }
    )
    persist_order_projection(engine, working, action="WORKING", actor="gateway")
    snapshot = broker_pb2.BrokerSnapshot(
        schema_version="1.0",
        snapshot_sequence=12,
        account=broker_pb2.AccountSnapshot(
            broker_id=broker_pb2.BROKER_ID_IBKR,
            occurred_at_utc="2026-07-21T14:30:01Z",
            health=broker_pb2.BROKER_HEALTH_HEALTHY,
            reconciled=True,
            buying_power="10000",
            net_liquidation="25000",
            currency="USD",
        ),
        positions=[
            broker_pb2.PositionSnapshot(contract_id="101", quantity=1, average_price="1.25")
        ],
        orders=[
            broker_pb2.BrokerOrderSnapshot(
                broker_order_id="900",
                idempotency_key=plan.idempotency_key,
                plan_hash=plan.plan_hash,
                status=broker_pb2.BROKER_ORDER_STATUS_WORKING,
                total_quantity=1,
                filled_quantity=0,
                submitted_price=plan.limit_price,
                side=broker_pb2.ORDER_SIDE_BUY,
                order_type=broker_pb2.BROKER_ORDER_TYPE_LIMIT,
                residual_exposure=True,
            )
        ],
        fills=[
            broker_pb2.FillSnapshot(
                fill_id="exec-1",
                broker_order_id="900",
                contract_id="101",
                side=broker_pb2.ORDER_SIDE_BUY,
                quantity=1,
                price="1.25",
                occurred_at_utc="2026-07-21T14:30:00Z",
            )
        ],
    )
    batch = _broker_batch(snapshot)
    assert persist_broker_reconciliation(engine, batch) == []
    assert persist_broker_reconciliation(engine, batch) == []

    with engine.connect() as conn:
        broker_row = conn.execute(select(broker_snapshots)).mappings().one()
        position_rows = conn.execute(select(position_snapshots)).mappings().all()
        fill_rows = conn.execute(select(fills)).mappings().all()
    assert broker_row["snapshot_hash"] == batch.snapshot_hash
    assert broker_row["reconciled"] is True
    assert len(position_rows) == len(fill_rows) == 1
    assert fill_rows[0]["order_id"] == working.order_id

    changed = broker_pb2.BrokerSnapshot()
    changed.CopyFrom(snapshot)
    changed.snapshot_sequence = 14
    changed.fills[0].price = "1.30"
    assert persist_broker_reconciliation(engine, _broker_batch(changed)) == [
        "BROKER_FILL_IDENTITY_CONFLICT"
    ]


def test_broker_fact_ledger_detects_unknown_order_and_fill(engine: Engine) -> None:
    snapshot = broker_pb2.BrokerSnapshot(
        schema_version="1.0",
        snapshot_sequence=13,
        account=broker_pb2.AccountSnapshot(
            broker_id=broker_pb2.BROKER_ID_IBKR,
            occurred_at_utc="2026-07-21T14:30:01Z",
            health=broker_pb2.BROKER_HEALTH_HEALTHY,
            reconciled=True,
            buying_power="10000",
            net_liquidation="25000",
            currency="USD",
        ),
        orders=[
            broker_pb2.BrokerOrderSnapshot(
                broker_order_id="external-1",
                status=broker_pb2.BROKER_ORDER_STATUS_WORKING,
            )
        ],
        fills=[
            broker_pb2.FillSnapshot(
                fill_id="exec-external",
                broker_order_id="historical-external",
                contract_id="101",
                side=broker_pb2.ORDER_SIDE_BUY,
                quantity=1,
                price="1.25",
                occurred_at_utc="2026-07-21T14:30:00Z",
            )
        ],
    )
    assert persist_broker_reconciliation(engine, _broker_batch(snapshot)) == [
        "UNKNOWN_ACTIVE_BROKER_ORDER",
        "UNKNOWN_BROKER_FILL",
    ]


def test_workflow_restore_only_decrypts_unclaimed_unexpired_capability(
    engine: Engine,
    confirmation_cipher: ConfirmationCipher,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(repository, "_now_utc", lambda: datetime(2026, 7, 20, 14, 30, tzinfo=UTC))
    plan, result = _staged_models()
    assert result.order is not None
    persist_staged_candidate(engine, plan, result, confirmation_cipher)

    restored = restorable_execution_workflow(engine, confirmation_cipher)
    assert len(restored) == 1
    assert restored[0][0].plan_hash == plan.plan_hash
    assert restored[0][1].order_id == result.order.order_id
    assert restored[0][2] == "never-persist-this"

    with engine.begin() as conn:
        conn.execute(
            update(confirmation_capabilities).values(
                claimed_at_utc=datetime(2026, 7, 20, 14, 30, 1, tzinfo=UTC)
            )
        )
    assert restorable_execution_workflow(engine, confirmation_cipher)[0][2] == ""


def test_execution_persistence_rejects_combo_quantity_mismatch(
    engine: Engine, confirmation_cipher: ConfirmationCipher
) -> None:
    plan, result = _staged_models()
    assert result.order is not None
    mismatched = result.model_copy(
        update={"order": result.order.model_copy(update={"total_quantity": 2})}
    )
    with pytest.raises(ValueError, match="does not match candidate plan"):
        persist_staged_candidate(engine, plan, mismatched, confirmation_cipher)


def test_confirmation_intent_claim_is_shared_one_time_and_precedes_projection(
    engine: Engine,
    confirmation_cipher: ConfirmationCipher,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        repository,
        "_now_utc",
        lambda: datetime(2026, 7, 20, 14, 30, 10, tzinfo=UTC),
    )
    plan, result = _staged_models()
    persist_staged_candidate(engine, plan, result, confirmation_cipher)
    assert (
        claim_confirmation_intent(
            engine,
            "order_demo_001",
            plan.plan_hash,
            "local-operator",
            confirmation_cipher,
        )
        == "never-persist-this"
    )
    assert (
        claim_confirmation_intent(
            engine,
            "order_demo_001",
            plan.plan_hash,
            "local-operator",
            confirmation_cipher,
        )
        is None
    )
    assert result.order is not None
    working = result.order.model_copy(
        update={
            "state": "WORKING",
            "state_version": 4,
            "broker_order_id": "paper-order-1",
            "updated_at_utc": "2026-07-20T14:30:02Z",
        }
    )
    assert persist_order_projection(
        engine, working, action="ORDER_CONFIRMED", actor="rust-execution-gateway"
    )
    assert not persist_order_projection(
        engine, working, action="ORDER_CONFIRMED", actor="rust-execution-gateway"
    )
    with engine.connect() as conn:
        assert conn.execute(select(orders.c.status)).scalar_one() == "WORKING"
        assert conn.execute(select(confirmation_capabilities)).first() is None
        actions = conn.execute(select(audit_events.c.action)).scalars().all()
    assert "CONFIRMATION_REQUESTED" in actions
    assert "ORDER_CONFIRMED" in actions


def test_order_projection_rejects_stale_or_conflicting_versions(
    engine: Engine, confirmation_cipher: ConfirmationCipher
) -> None:
    plan, result = _staged_models()
    assert result.order is not None
    plan = plan.model_copy(
        update={"legs": [leg.model_copy(update={"quantity": 3}) for leg in plan.legs]}
    )
    staged_order = result.order.model_copy(update={"total_quantity": 3})
    result = result.model_copy(update={"order": staged_order})
    persist_staged_candidate(engine, plan, result, confirmation_cipher)
    working = staged_order.model_copy(
        update={
            "state": "WORKING",
            "state_version": 4,
            "broker_order_id": "paper-order-1",
            "updated_at_utc": "2026-07-20T14:30:04Z",
        }
    )
    assert persist_order_projection(engine, working, action="WORKING", actor="gateway")

    assert not persist_order_projection(
        engine,
        staged_order,
        action="STALE_STAGE",
        actor="gateway",
    )
    conflict = working.model_copy(update={"state": "FILLED"})
    with pytest.raises(ValueError, match="state_version"):
        persist_order_projection(engine, conflict, action="CONFLICT", actor="gateway")

    partial = working.model_copy(
        update={
            "state": "PARTIAL_FILL",
            "state_version": 5,
            "filled_quantity": 1,
            "residual_exposure": True,
            "updated_at_utc": "2026-07-20T14:30:05Z",
        }
    )
    assert persist_order_projection(engine, partial, action="PARTIAL", actor="gateway")
    second_partial = partial.model_copy(
        update={
            "state_version": 6,
            "filled_quantity": 2,
            "updated_at_utc": "2026-07-20T14:30:06Z",
        }
    )
    assert persist_order_projection(
        engine, second_partial, action="PARTIAL_PROGRESS", actor="gateway"
    )
    with engine.connect() as conn:
        row = conn.execute(select(orders)).mappings().one()
    assert row["status"] == "PARTIAL_FILL"
    assert row["filled_quantity"] == 2
    assert row["state_version"] == 6
    assert row["payload"]["state_version"] == 6


def test_order_projection_cannot_clear_unreconciled_residual_exposure(
    engine: Engine, confirmation_cipher: ConfirmationCipher
) -> None:
    plan, result = _staged_models()
    persist_staged_candidate(engine, plan, result, confirmation_cipher)
    assert result.order is not None
    uncertain = result.order.model_copy(
        update={
            "state": "RECONCILE_PENDING",
            "state_version": 4,
            "broker_order_id": "lb-split-1",
            "residual_exposure": True,
            "updated_at_utc": "2026-07-20T14:30:04Z",
        }
    )
    assert persist_order_projection(engine, uncertain, action="BROKER_UNKNOWN", actor="gateway")
    falsely_cleared = uncertain.model_copy(
        update={
            "state": "CANCEL_PENDING",
            "state_version": 5,
            "residual_exposure": False,
            "updated_at_utc": "2026-07-20T14:30:05Z",
        }
    )
    with pytest.raises(ValueError, match="terminal flat proof"):
        persist_order_projection(engine, falsely_cleared, action="FALSE_CLEAR", actor="gateway")


def test_parent_fill_proof_clears_single_or_native_combo_residual(
    engine: Engine, confirmation_cipher: ConfirmationCipher
) -> None:
    plan, result = _staged_models()
    persist_staged_candidate(engine, plan, result, confirmation_cipher)
    assert result.order is not None
    uncertain = result.order.model_copy(
        update={
            "state": "RECONCILE_PENDING",
            "state_version": 4,
            "broker_order_id": "native-order-1",
            "residual_exposure": True,
            "updated_at_utc": "2026-07-20T14:30:04Z",
        }
    )
    assert persist_order_projection(engine, uncertain, action="BROKER_UNKNOWN", actor="gateway")
    filled = uncertain.model_copy(
        update={
            "state": "FILLED",
            "state_version": 5,
            "filled_quantity": uncertain.total_quantity,
            "residual_exposure": False,
            "updated_at_utc": "2026-07-20T14:30:05Z",
        }
    )

    assert persist_order_projection(engine, filled, action="BROKER_FILLED", actor="gateway")
    with engine.connect() as conn:
        persisted = conn.execute(select(orders.c.payload)).scalar_one()
    assert persisted["state"] == "FILLED"
    assert persisted["residual_exposure"] is False


def test_persist_writes_signal_and_audit(engine: Engine) -> None:
    wrote = persist_signal(engine, _ctx(), _regime(), _vol(), _decision())
    assert wrote is True
    with engine.connect() as conn:
        srows = conn.execute(select(signals)).mappings().all()
        arows = conn.execute(select(audit_events)).mappings().all()
    assert len(srows) == 1
    assert len(arows) == 1
    assert srows[0]["strategy_kind"] == "LongGamma"
    assert arows[0]["entity_id"] == "sig-1"
    assert srows[0]["created_at_utc"] is not None


def test_persist_is_idempotent(engine: Engine) -> None:
    assert persist_signal(engine, _ctx(), _regime(), _vol(), _decision()) is True
    assert persist_signal(engine, _ctx(), _regime(), _vol(), _decision()) is False
    with engine.connect() as conn:
        assert conn.execute(select(signals)).mappings().all().__len__() == 1
        assert conn.execute(select(audit_events)).mappings().all().__len__() == 1


def test_persist_no_trade_reason_persisted(engine: Engine) -> None:
    decision = StrategyDecision(
        playbook=NO_TRADE,
        reason="regime=Chaos: conflicting trend/range signals",
        risk_status="PASS_READONLY",
        risk_notes=[],
    )
    persist_signal(engine, _ctx("sig-nt"), _regime(RANGE), _vol(IV_RICH), decision)
    with engine.connect() as conn:
        row = conn.execute(select(signals)).mappings().one()
    assert row["strategy_kind"] == "NoTrade"
    assert row["no_trade_reason"] == "regime=Chaos: conflicting trend/range signals"


def test_transaction_atomic_on_audit_failure(engine: Engine) -> None:
    """If the audit insert fails, the signal insert must roll back too."""
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE audit.audit_events"))
        conn.commit()
    with pytest.raises(Exception):
        persist_signal(engine, _ctx(), _regime(), _vol(), _decision())
    with engine.connect() as conn:
        assert conn.execute(select(signals)).mappings().all() == []


@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL is required")
def test_postgresql_migration_and_concurrent_idempotency() -> None:
    """Exercise real FK/JSONB/timestamptz and atomic ON CONFLICT behavior."""
    raw_url = os.environ["DATABASE_URL"]
    url = raw_url.replace("postgresql://", "postgresql+psycopg://", 1)
    pg_engine = create_engine(url, pool_size=5)
    suffix = uuid4().hex
    session_id = f"review-{suffix}"
    signal_id = f"sig-{suffix}"
    ctx = SignalContext(
        signal_id=signal_id,
        session_id=session_id,
        occurred_at_utc=datetime.now(UTC),
        rule_version="rules_p1_1.0.0",
    )
    try:
        with pg_engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO trading.trading_sessions "
                    "(session_id, trading_date, status) VALUES (:id, CURRENT_DATE, 'REPLAY')"
                ),
                {"id": session_id},
            )

        def write_once() -> bool:
            return persist_signal(pg_engine, ctx, _regime(), _vol(), _decision())

        with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(lambda _: write_once(), range(4)))
        assert results.count(True) == 1
        assert results.count(False) == 3

        with pg_engine.connect() as conn:
            assert (
                conn.execute(
                    text("SELECT count(*) FROM trading.signals WHERE signal_id=:id"),
                    {"id": signal_id},
                ).scalar_one()
                == 1
            )
            assert (
                conn.execute(
                    text("SELECT count(*) FROM audit.audit_events WHERE entity_id=:id"),
                    {"id": signal_id},
                ).scalar_one()
                == 1
            )
            assert (
                conn.execute(
                    text("SELECT count(*) FROM audit.outbox_events WHERE aggregate_id=:id"),
                    {"id": signal_id},
                ).scalar_one()
                == 1
            )
    finally:
        with pg_engine.begin() as conn:
            conn.execute(
                text("DELETE FROM audit.outbox_events WHERE aggregate_id=:id"),
                {"id": signal_id},
            )
            conn.execute(
                text("DELETE FROM audit.audit_events WHERE entity_id=:id"), {"id": signal_id}
            )
            conn.execute(text("DELETE FROM trading.signals WHERE signal_id=:id"), {"id": signal_id})
            conn.execute(
                text("DELETE FROM trading.trading_sessions WHERE session_id=:id"),
                {"id": session_id},
            )


@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL is required")
def test_postgresql_outbox_skip_locked_never_leases_same_event_twice() -> None:
    raw_url = os.environ["DATABASE_URL"]
    pg_engine = create_engine(
        raw_url.replace("postgresql://", "postgresql+psycopg://", 1), pool_size=3
    )
    suffix = uuid4().hex
    session_id = f"outbox-{suffix}"
    signal_id = f"sig-{suffix}"
    barrier = Barrier(2)
    try:
        with pg_engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO trading.trading_sessions "
                    "(session_id, trading_date, status) VALUES (:id, CURRENT_DATE, 'REPLAY')"
                ),
                {"id": session_id},
            )
        assert persist_signal(
            pg_engine,
            SignalContext(signal_id, session_id, datetime.now(UTC), "rules-p3-test"),
            _regime(),
            _vol(),
            _decision(),
        )
        with pg_engine.begin() as conn:
            target_event_id = str(
                conn.execute(
                    select(outbox_events.c.event_id).where(
                        outbox_events.c.aggregate_id == signal_id
                    )
                ).scalar_one()
            )
            conn.execute(
                update(outbox_events)
                .where(outbox_events.c.event_id == target_event_id)
                .values(available_at_utc=datetime(2000, 1, 1, tzinfo=UTC))
            )

        def claim(worker: str) -> list[str]:
            barrier.wait(timeout=2)
            return [message.event_id for message in claim_outbox_batch(pg_engine, worker, limit=1)]

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(claim, ("worker-a", "worker-b")))
        claimed_ids = [event_id for result in results for event_id in result]
        assert claimed_ids.count(target_event_id) == 1
        with pg_engine.connect() as conn:
            lease_owner = conn.execute(
                select(outbox_events.c.lease_owner).where(
                    outbox_events.c.event_id == target_event_id
                )
            ).scalar_one()
        assert lease_owner in {"worker-a", "worker-b"}
    finally:
        with pg_engine.begin() as conn:
            conn.execute(
                text("DELETE FROM audit.outbox_events WHERE aggregate_id=:id"),
                {"id": signal_id},
            )
            conn.execute(
                text("DELETE FROM audit.audit_events WHERE entity_id=:id"), {"id": signal_id}
            )
            conn.execute(text("DELETE FROM trading.signals WHERE signal_id=:id"), {"id": signal_id})
            conn.execute(
                text("DELETE FROM trading.trading_sessions WHERE session_id=:id"),
                {"id": session_id},
            )
        pg_engine.dispose()


@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL is required")
def test_postgresql_order_projection_cannot_regress_during_concurrent_write() -> None:
    """A stale writer must re-read the row after the competing transaction commits."""
    raw_url = os.environ["DATABASE_URL"]
    pg_engine = create_engine(raw_url.replace("postgresql://", "postgresql+psycopg://", 1))
    suffix = uuid4().hex
    now = datetime.now(UTC).replace(microsecond=0)
    created = now.isoformat().replace("+00:00", "Z")
    expires = (now + timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
    plan_base, result_base = _staged_models()
    assert result_base.order is not None
    plan = plan_base.model_copy(
        update={
            "plan_id": f"plan-{suffix}",
            "plan_hash": suffix * 2,
            "idempotency_key": f"idem-{suffix}",
            "session_id": f"session-{suffix}",
            "signal_id": f"signal-{suffix}",
            "created_at_utc": created,
            "expires_at_utc": expires,
        }
    )
    decision = result_base.initial_risk_decision.model_copy(
        update={
            "decision_id": f"decision-{suffix}",
            "plan_id": plan.plan_id,
            "plan_hash": plan.plan_hash,
            "session_id": plan.session_id,
            "occurred_at_utc": created,
        }
    )
    staged_order = result_base.order.model_copy(
        update={
            "order_id": f"order-{suffix}",
            "plan_id": plan.plan_id,
            "plan_hash": plan.plan_hash,
            "idempotency_key": plan.idempotency_key,
            "session_id": plan.session_id,
            "expires_at_utc": expires,
            "updated_at_utc": created,
        }
    )
    staged = StageCandidateResult(
        initial_risk_decision=decision,
        order=staged_order,
        confirmation_token="concurrent-secret",
    )
    cipher = ConfirmationCipher(Fernet.generate_key().decode("ascii"))
    try:
        with pg_engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO trading.trading_sessions "
                    "(session_id, trading_date, status) VALUES (:id, CURRENT_DATE, 'PAPER')"
                ),
                {"id": plan.session_id},
            )
        persist_signal(
            pg_engine,
            SignalContext(plan.signal_id, plan.session_id, now, "rules-p3-test"),
            _regime(),
            _vol(),
            _decision(),
        )
        assert persist_staged_candidate(pg_engine, plan, staged, cipher)
        with ThreadPoolExecutor(max_workers=2) as executor:
            claims = list(
                executor.map(
                    lambda _: claim_confirmation_intent(
                        pg_engine,
                        staged_order.order_id,
                        plan.plan_hash,
                        "concurrent-operator",
                        cipher,
                    ),
                    range(2),
                )
            )
        assert claims.count("concurrent-secret") == 1
        assert claims.count(None) == 1

        stale = staged_order.model_copy(
            update={
                "state": "WORKING",
                "state_version": 4,
                "broker_order_id": "paper-stale",
                "updated_at_utc": (now + timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
            }
        )
        authoritative = stale.model_copy(
            update={
                "state": "PARTIAL_FILL",
                "state_version": 5,
                "filled_quantity": 1,
                "broker_order_id": "paper-authoritative",
                "residual_exposure": True,
                "updated_at_utc": (now + timedelta(seconds=2)).isoformat().replace("+00:00", "Z"),
            }
        )
        stale_select_started = Event()

        def observe_stale_select(
            _conn: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _executemany: bool,
        ) -> None:
            if (
                current_thread().name.startswith("stale-writer")
                and "FOR UPDATE" in statement
                and staged_order.order_id in str(_parameters)
            ):
                stale_select_started.set()

        event.listen(pg_engine, "before_cursor_execute", observe_stale_select)
        try:
            with pg_engine.connect() as blocker:
                transaction = blocker.begin()
                blocker.execute(
                    select(orders)
                    .where(orders.c.order_id == staged_order.order_id)
                    .with_for_update()
                )
                with ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix="stale-writer"
                ) as executor:
                    future = executor.submit(
                        persist_order_projection,
                        pg_engine,
                        stale,
                        action="STALE_CONCURRENT_WRITE",
                        actor="test",
                    )
                    assert stale_select_started.wait(timeout=2)
                    blocker.execute(
                        update(orders)
                        .where(orders.c.order_id == staged_order.order_id)
                        .values(
                            status=authoritative.state,
                            filled_quantity=authoritative.filled_quantity,
                            state_version=authoritative.state_version,
                            broker_order_id=authoritative.broker_order_id,
                            payload=authoritative.model_dump(mode="json"),
                            updated_at_utc=now + timedelta(seconds=2),
                        )
                    )
                    transaction.commit()
                    assert future.result(timeout=5) is False
        finally:
            event.remove(pg_engine, "before_cursor_execute", observe_stale_select)

        with pg_engine.connect() as conn:
            row = (
                conn.execute(select(orders).where(orders.c.order_id == staged_order.order_id))
                .mappings()
                .one()
            )
        assert row["state_version"] == 5
        assert row["filled_quantity"] == 1
        assert row["payload"]["state"] == "PARTIAL_FILL"
    finally:
        with pg_engine.begin() as conn:
            conn.execute(
                text(
                    "DELETE FROM audit.outbox_events WHERE aggregate_id IN (:plan_id, :signal_id)"
                ),
                {"plan_id": plan.plan_id, "signal_id": plan.signal_id},
            )
            conn.execute(
                text("DELETE FROM audit.audit_events WHERE session_id=:session_id"),
                {"session_id": plan.session_id},
            )
            conn.execute(
                text("DELETE FROM trading.order_events WHERE order_id=:id"),
                {"id": staged_order.order_id},
            )
            conn.execute(
                text("DELETE FROM risk.confirmation_capabilities WHERE order_id=:id"),
                {"id": staged_order.order_id},
            )
            conn.execute(
                text("DELETE FROM risk.risk_decisions WHERE plan_id=:id"), {"id": plan.plan_id}
            )
            conn.execute(
                text("DELETE FROM trading.orders WHERE order_id=:id"),
                {"id": staged_order.order_id},
            )
            conn.execute(
                text("DELETE FROM trading.candidate_trade_plans WHERE plan_id=:id"),
                {"id": plan.plan_id},
            )
            conn.execute(
                text("DELETE FROM trading.signals WHERE signal_id=:id"), {"id": plan.signal_id}
            )
            conn.execute(
                text("DELETE FROM trading.trading_sessions WHERE session_id=:id"),
                {"id": plan.session_id},
            )
        pg_engine.dispose()
