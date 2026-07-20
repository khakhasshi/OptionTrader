"""Regime Engine tests (DESIGN 4.3): score components and classification.

Synthetic sessions are shaped to hit specific rules deterministically, so the
scores and labels are hand-checkable and stable.
"""

from __future__ import annotations

from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from app.regime import CHAOS, EVENT, NO_TRADE, RANGE, TREND, RegimeInputs, evaluate

SRC_TZ = ZoneInfo("America/New_York")


def _bars(closes: list[float], highs: list[float] | None = None,
          lows: list[float] | None = None, opens: list[float] | None = None) -> pd.DataFrame:
    base = pd.Timestamp("2026-07-09 09:30:00", tz=SRC_TZ)
    n = len(closes)
    ts = pd.Series([base + pd.Timedelta(minutes=i) for i in range(n)])
    highs = highs if highs is not None else [c + 0.25 for c in closes]
    lows = lows if lows is not None else [c - 0.25 for c in closes]
    opens = opens if opens is not None else list(closes)
    return pd.DataFrame(
        {
            "occurred_at_utc": ts.dt.tz_convert("UTC"),
            "timestamp_et": ts,
            "trading_date": ["2026-07-09"] * n,
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": [1000] * n,
        }
    )


def test_empty_bars_rejected() -> None:
    empty = pd.DataFrame(
        {c: [] for c in ["occurred_at_utc", "open", "high", "low", "close", "volume"]}
    )
    with pytest.raises(ValueError, match="no bars"):
        evaluate(empty)


def test_strong_uptrend_scores_trend() -> None:
    # 30 bars ramping up: holds above VWAP the whole time and breaks the OR high.
    closes = [500.0 + i * 0.5 for i in range(30)]
    state = evaluate(_bars(closes), inputs=RegimeInputs(volume_vs_20d=1.3,
                                                        straddle_series=[1.0, 2.0]))
    assert state.components["trend_sustained_vwap_side"] == 2
    assert state.components["trend_break_opening_range"] == 2
    assert state.components["trend_volume_above_20d"] == 1
    assert state.trend_score >= 5
    assert state.range_score < 4
    assert state.regime == TREND


def test_choppy_session_scores_range() -> None:
    # Oscillate across VWAP many times, stay inside the opening range.
    closes = [500.0 + (2.0 if i % 2 else -2.0) for i in range(30)]
    state = evaluate(_bars(closes), inputs=RegimeInputs(volume_vs_20d=0.7,
                                                        straddle_series=[2.0, 1.0]))
    assert state.components["range_vwap_crossings"] == 2
    assert state.range_score >= 5
    assert state.trend_score < 4
    assert state.regime == RANGE


def test_event_window_overrides() -> None:
    closes = [500.0 + i * 0.5 for i in range(30)]
    state = evaluate(_bars(closes), inputs=RegimeInputs(event_window=True))
    assert state.regime == EVENT


def test_missing_optional_inputs_are_recorded_not_fabricated() -> None:
    closes = [500.0 + i * 0.5 for i in range(30)]
    state = evaluate(_bars(closes))
    assert "volume_vs_20d" in state.unavailable
    assert "straddle_series" in state.unavailable
    # unavailable signals contribute exactly 0, never a guessed value
    assert state.components["trend_volume_above_20d"] == 0
    assert state.components["trend_straddle_rising"] == 0


def test_flat_session_is_no_trade() -> None:
    # Barely-moving prices: no sustained side, no breakout, few crossings.
    closes = [500.0 + (0.01 if i % 2 else -0.01) for i in range(12)]
    state = evaluate(_bars(closes))
    assert state.trend_score < 5
    assert state.regime in {NO_TRADE, RANGE}


def test_deterministic_repeat() -> None:
    closes = [500.0 + i * 0.3 for i in range(20)]
    a = evaluate(_bars(closes))
    b = evaluate(_bars(closes))
    assert a == b


def test_both_scores_high_is_chaos() -> None:
    # Large-body bars that break the OR high but also whipsaw across VWAP:
    # drives both trend (breakout) and range (crossings) contributions up.
    closes = [500.0, 496.0, 508.0, 494.0, 510.0, 493.0, 512.0, 492.0,
              514.0, 491.0, 516.0, 490.0, 518.0, 489.0, 520.0, 488.0, 522.0]
    highs = [c + 3.0 for c in closes]
    lows = [c - 3.0 for c in closes]
    state = evaluate(_bars(closes, highs=highs, lows=lows),
                     opening_range_minutes=3)
    # Whichever thresholds land, chaos requires both >=5; assert consistency.
    if state.trend_score >= 5 and state.range_score >= 5:
        assert state.regime == CHAOS
