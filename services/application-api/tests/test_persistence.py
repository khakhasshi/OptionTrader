"""P1-7: signal + No-Trade reason persistence to review/audit.

Serialization is tested purely; the transactional write-path runs against an
in-memory SQLite whose ``trading``/``audit`` schemas are ATTACHed, so the
schema-qualified inserts and the single-transaction guarantee are exercised
without a live Postgres.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, event, select, text

from app.persistence import (
    SignalContext,
    audit_events,
    build_signal_rows,
    metadata,
    persist_signal,
    signals,
)
from app.regime import RANGE, TREND, RegimeState
from app.strategy import LONG_GAMMA, NO_TRADE, StrategyDecision
from app.vol import IV_CHEAP, IV_RICH, VolState

UTC = timezone.utc


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
    )


# ------------------------------- serialization -------------------------------


def test_serialize_traded_signal_has_no_no_trade_reason() -> None:
    sig, audit = build_signal_rows(_ctx(), _regime(), _vol(), _decision(LONG_GAMMA))
    assert sig["strategy_kind"] == LONG_GAMMA
    assert sig["no_trade_reason"] is None
    assert sig["regime"] == TREND
    assert sig["vol_state"] == IV_CHEAP
    assert sig["payload"]["regime"]["trend_score"] == 6
    assert audit["action"] == "SIGNAL_EMITTED"
    assert audit["to_status"] == LONG_GAMMA
    assert audit["entity_id"] == "sig-1"


def test_serialize_no_trade_records_reason() -> None:
    decision = StrategyDecision(
        playbook=NO_TRADE,
        reason="Trend but no confirmed opening-range breakout",
        risk_status="PASS_READONLY",
        risk_notes=[],
    )
    sig, _ = build_signal_rows(_ctx(), _regime(), _vol(), decision)
    assert sig["strategy_kind"] == NO_TRADE
    assert sig["no_trade_reason"] == "Trend but no confirmed opening-range breakout"


def test_serialize_captures_unavailable_inputs() -> None:
    sig, _ = build_signal_rows(_ctx(), _regime(), _vol(), _decision())
    assert sig["payload"]["regime"]["unavailable"] == ["volume_vs_20d"]


def test_serialize_rejects_naive_timestamp() -> None:
    ctx = SignalContext("sig-1", "2026-07-09", datetime(2026, 7, 9, 13, 45))
    with pytest.raises(ValueError, match="timezone-aware"):
        build_signal_rows(ctx, _regime(), _vol(), _decision())


def test_serialize_rejects_non_utc_timestamp() -> None:
    from datetime import timedelta

    est = timezone(timedelta(hours=-5))
    ctx = SignalContext("sig-1", "2026-07-09", datetime(2026, 7, 9, 8, 45, tzinfo=est))
    with pytest.raises(ValueError, match="must be UTC"):
        build_signal_rows(ctx, _regime(), _vol(), _decision())


# ------------------------------- write-path ----------------------------------


@pytest.fixture()
def engine():
    """In-memory SQLite with trading/audit schemas attached, mirror tables built.

    SQLAlchemy renders ``trading.signals`` as a schema reference; SQLite treats
    ATTACHed databases as schemas, so this exercises the real qualified inserts.
    """
    eng = create_engine("sqlite://")

    @event.listens_for(eng, "connect")
    def _attach(dbapi_conn, _rec):
        cur = dbapi_conn.cursor()
        cur.execute("ATTACH DATABASE ':memory:' AS trading")
        cur.execute("ATTACH DATABASE ':memory:' AS audit")
        cur.close()

    metadata.create_all(eng)
    return eng


def test_persist_writes_signal_and_audit(engine) -> None:
    wrote = persist_signal(engine, _ctx(), _regime(), _vol(), _decision())
    assert wrote is True
    with engine.connect() as conn:
        srows = conn.execute(select(signals)).mappings().all()
        arows = conn.execute(select(audit_events)).mappings().all()
    assert len(srows) == 1
    assert len(arows) == 1
    assert srows[0]["strategy_kind"] == LONG_GAMMA
    assert arows[0]["entity_id"] == "sig-1"
    assert srows[0]["created_at_utc"] is not None


def test_persist_is_idempotent(engine) -> None:
    assert persist_signal(engine, _ctx(), _regime(), _vol(), _decision()) is True
    assert persist_signal(engine, _ctx(), _regime(), _vol(), _decision()) is False
    with engine.connect() as conn:
        assert conn.execute(select(signals)).mappings().all().__len__() == 1
        assert conn.execute(select(audit_events)).mappings().all().__len__() == 1


def test_persist_no_trade_reason_persisted(engine) -> None:
    decision = StrategyDecision(
        playbook=NO_TRADE,
        reason="regime=Chaos: conflicting trend/range signals",
        risk_status="PASS_READONLY",
        risk_notes=[],
    )
    persist_signal(engine, _ctx("sig-nt"), _regime(RANGE), _vol(IV_RICH), decision)
    with engine.connect() as conn:
        row = conn.execute(select(signals)).mappings().one()
    assert row["strategy_kind"] == NO_TRADE
    assert row["no_trade_reason"] == "regime=Chaos: conflicting trend/range signals"


def test_transaction_atomic_on_audit_failure(engine) -> None:
    """If the audit insert fails, the signal insert must roll back too."""
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE audit.audit_events"))
        conn.commit()
    with pytest.raises(Exception):
        persist_signal(engine, _ctx(), _regime(), _vol(), _decision())
    with engine.connect() as conn:
        assert conn.execute(select(signals)).mappings().all() == []
