"""P1-7: signal + No-Trade reason persistence to review/audit.

Serialization is tested purely; the transactional write-path runs against an
in-memory SQLite whose ``trading``/``audit`` schemas are ATTACHed, so the
schema-qualified inserts and the single-transaction guarantee are exercised
without a live Postgres.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from jsonschema import Draft202012Validator
import pytest
from referencing import Registry, Resource
from sqlalchemy import create_engine, event, select, text
from sqlalchemy.engine import Engine

from app.persistence import (
    SignalContext,
    audit_events,
    candidate_trade_plans,
    build_signal_contract,
    build_signal_rows,
    event_contexts,
    metadata,
    order_events,
    orders,
    persist_confirmation_intent,
    persist_order_projection,
    persist_signal,
    persist_event_context,
    persist_staged_candidate,
    risk_decisions,
    signals,
)
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

UTC = timezone.utc
_ROOT = Path(__file__).resolve().parents[3]


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
        schema_version="1.0",
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
        risk_reason_codes=[],
    )
    return plan, StageCandidateResult(
        initial_risk_decision=decision,
        order=order,
        confirmation_token="never-persist-this",
    )


def test_execution_persistence_is_atomic_idempotent_and_omits_token(engine: Engine) -> None:
    plan, result = _staged_models()
    assert persist_staged_candidate(engine, plan, result) is True
    assert persist_staged_candidate(engine, plan, result) is False

    with engine.connect() as conn:
        plan_row = conn.execute(select(candidate_trade_plans)).mappings().one()
        risk_row = conn.execute(select(risk_decisions)).mappings().one()
        order_row = conn.execute(select(orders)).mappings().one()
        event_row = conn.execute(select(order_events)).mappings().one()
        audit_rows = conn.execute(select(audit_events)).mappings().all()
    serialized = json.dumps(
        [
            plan_row["payload"],
            risk_row["payload"],
            event_row["payload"],
            [row["payload"] for row in audit_rows],
        ]
    )
    assert "never-persist-this" not in serialized
    assert order_row["status"] == "AWAITING_CONFIRMATION"


def test_confirmation_intent_precedes_order_projection_and_is_idempotent(engine: Engine) -> None:
    plan, result = _staged_models()
    persist_staged_candidate(engine, plan, result)
    assert persist_confirmation_intent(engine, "order_demo_001", plan.plan_hash, "local-operator")
    assert not persist_confirmation_intent(
        engine, "order_demo_001", plan.plan_hash, "local-operator"
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
        actions = conn.execute(select(audit_events.c.action)).scalars().all()
    assert "CONFIRMATION_REQUESTED" in actions
    assert "ORDER_CONFIRMED" in actions


def test_order_projection_rejects_stale_or_conflicting_versions(engine: Engine) -> None:
    plan, result = _staged_models()
    persist_staged_candidate(engine, plan, result)
    assert result.order is not None
    working = result.order.model_copy(
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
        result.order,
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
    assert row["payload"]["state_version"] == 6


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
    finally:
        with pg_engine.begin() as conn:
            conn.execute(
                text("DELETE FROM audit.audit_events WHERE entity_id=:id"), {"id": signal_id}
            )
            conn.execute(text("DELETE FROM trading.signals WHERE signal_id=:id"), {"id": signal_id})
            conn.execute(
                text("DELETE FROM trading.trading_sessions WHERE session_id=:id"),
                {"id": session_id},
            )
        pg_engine.dispose()
