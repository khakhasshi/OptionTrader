"""Strategy Engine tests (DESIGN 4.5): selection + read-only risk pre-check."""

from __future__ import annotations

from datetime import time

from app.regime import CHAOS, EVENT, RANGE, TREND, RegimeState
from app.strategy import (
    EVENT_VOL_CRUSH,
    LONG_GAMMA,
    NO_TRADE,
    SHORT_PREMIUM,
    RiskLimits,
    StrategyInputs,
    decide,
)
from app.vol import (
    IV_CHEAP,
    IV_FAIR,
    IV_RICH,
    IV_UNKNOWN,
    IV_VERY_RICH,
    SHORT_VOL,
    VolState,
)


def _regime(label: str) -> RegimeState:
    return RegimeState(regime=label, trend_score=0, range_score=0, components={})


def _vol(iv_state: str, *, interpretation: str = "Undecided", complete: bool = False) -> VolState:
    return VolState(
        iv_hv_state=iv_state,
        interpretation=interpretation,
        atm_iv=0.30 if complete else None,
        hv_20=0.20 if complete else None,
        hv_60=0.18 if complete else None,
        iv_hv_ratio=1.5 if complete else None,
        implied_move=0.02 if complete else None,
        realized_move=0.0,
        realized_implied_ratio=0.2 if complete else None,
        straddle_mark=10.0 if complete else None,
        unavailable=[],
    )


def test_long_gamma_selected_in_window() -> None:
    d = decide(
        _regime(TREND),
        _vol(IV_CHEAP),
        StrategyInputs(now_et=time(10, 0), breakout=True, spread_ok=True),
    )
    assert d.playbook == LONG_GAMMA
    assert d.risk_status == "PASS_READONLY"


def test_long_gamma_blocked_outside_window() -> None:
    d = decide(
        _regime(TREND),
        _vol(IV_FAIR),
        StrategyInputs(now_et=time(12, 30), breakout=True),
    )
    assert d.playbook == NO_TRADE
    assert "window" in d.reason


def test_trend_needs_cheap_iv() -> None:
    d = decide(
        _regime(TREND),
        _vol(IV_RICH),
        StrategyInputs(now_et=time(10, 0), breakout=True),
    )
    assert d.playbook == NO_TRADE


def test_short_premium_selected() -> None:
    d = decide(
        _regime(RANGE),
        _vol(IV_VERY_RICH, interpretation=SHORT_VOL, complete=True),
        StrategyInputs(now_et=time(11, 0), breakout=False, minutes_to_major_event=60),
    )
    assert d.playbook == SHORT_PREMIUM


def test_short_premium_too_early() -> None:
    d = decide(
        _regime(RANGE),
        _vol(IV_RICH, interpretation=SHORT_VOL, complete=True),
        StrategyInputs(now_et=time(9, 45), breakout=False, minutes_to_major_event=60),
    )
    assert d.playbook == NO_TRADE
    assert "window" in d.reason


def test_short_premium_requires_decay_event_data_and_window() -> None:
    regime = _regime(RANGE)
    assert (
        decide(
            regime,
            _vol(IV_RICH, complete=True),
            StrategyInputs(now_et=time(11, 0), minutes_to_major_event=60),
        ).playbook
        == NO_TRADE
    )
    assert (
        decide(
            regime,
            _vol(IV_RICH, interpretation=SHORT_VOL, complete=True),
            StrategyInputs(now_et=time(11, 0), minutes_to_major_event=None),
        ).playbook
        == NO_TRADE
    )
    assert (
        decide(
            regime,
            _vol(IV_RICH, interpretation=SHORT_VOL, complete=True),
            StrategyInputs(now_et=time(11, 31), minutes_to_major_event=60),
        ).playbook
        == NO_TRADE
    )
    assert (
        decide(
            regime,
            _vol(IV_RICH, interpretation=SHORT_VOL, complete=True),
            StrategyInputs(now_et=time(11, 0), minutes_to_major_event=15),
        ).playbook
        == NO_TRADE
    )


def test_short_premium_window_boundaries_are_inclusive() -> None:
    vol = _vol(IV_RICH, interpretation=SHORT_VOL, complete=True)
    for now_et in (time(10, 0), time(11, 30)):
        decision = decide(
            _regime(RANGE),
            vol,
            StrategyInputs(now_et=now_et, minutes_to_major_event=60),
        )
        assert decision.playbook == SHORT_PREMIUM


def test_event_takes_precedence() -> None:
    d = decide(
        _regime(EVENT),
        _vol(IV_RICH),
        StrategyInputs(now_et=time(10, 0), event_released=True),
    )
    assert d.playbook == EVENT_VOL_CRUSH


def test_event_before_release_is_no_trade() -> None:
    d = decide(_regime(EVENT), _vol(IV_UNKNOWN), StrategyInputs(now_et=time(10, 0)))
    assert d.playbook == NO_TRADE


def test_chaos_is_no_trade() -> None:
    d = decide(_regime(CHAOS), _vol(IV_CHEAP), StrategyInputs(now_et=time(10, 0)))
    assert d.playbook == NO_TRADE


def test_risk_precheck_blocks_wide_spread() -> None:
    d = decide(
        _regime(TREND),
        _vol(IV_CHEAP),
        StrategyInputs(now_et=time(10, 0), breakout=True, spread_ok=False),
    )
    assert d.playbook == NO_TRADE
    assert d.risk_status == "BLOCKED"
    assert any("spread" in n for n in d.risk_notes)


def test_risk_precheck_fails_closed_on_unhealthy_data() -> None:
    d = decide(
        _regime(TREND),
        _vol(IV_CHEAP),
        StrategyInputs(now_et=time(10, 0), breakout=True, data_healthy=False),
    )
    assert d.playbook == NO_TRADE
    assert d.risk_status == "BLOCKED"


def test_limits_unconfirmed_flag_surfaced() -> None:
    d = decide(
        _regime(TREND),
        _vol(IV_CHEAP),
        StrategyInputs(now_et=time(10, 0), breakout=True),
    )
    assert d.limits_unconfirmed is True
    assert any("UNCONFIRMED" in n for n in d.risk_notes)


def test_confirmed_limits_drop_unconfirmed_note() -> None:
    d = decide(
        _regime(TREND),
        _vol(IV_CHEAP),
        StrategyInputs(now_et=time(10, 0), breakout=True),
        limits=RiskLimits(max_spread_pct=0.08, unconfirmed=False),
    )
    assert d.limits_unconfirmed is False
    assert not any("UNCONFIRMED" in n for n in d.risk_notes)
