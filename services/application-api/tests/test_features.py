"""Tests for deterministic features (P1-2): underlying + option quote math."""

from __future__ import annotations

import math
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from app.features import historical_volatility, opening_range, session_vwap
from app.features.options import atm_straddle, bid_ask_spread

SRC_TZ = ZoneInfo("Etc/GMT+4")


def _bars(prices: list[float], vols: list[int]) -> pd.DataFrame:
    base = pd.Timestamp("2026-07-09 09:30:00", tz=SRC_TZ)
    n = len(prices)
    ts = pd.Series([base + pd.Timedelta(minutes=i) for i in range(n)])
    return pd.DataFrame(
        {
            "occurred_at_utc": ts.dt.tz_convert("UTC"),
            "high": [p + 0.5 for p in prices],
            "low": [p - 0.5 for p in prices],
            "close": prices,
            "volume": vols,
        }
    )


def test_session_vwap_is_volume_weighted():
    bars = _bars([100.0, 200.0], [1, 3])
    # (100*1 + 200*3) / 4 = 175
    assert session_vwap(bars) == pytest.approx(175.0)


def test_session_vwap_zero_volume_falls_back_to_mean():
    bars = _bars([100.0, 200.0], [0, 0])
    assert session_vwap(bars) == pytest.approx(150.0)


def test_opening_range_uses_first_n_bars():
    bars = _bars([100.0, 105.0, 90.0, 110.0], [1, 1, 1, 1])
    orange = opening_range(bars, minutes=2)
    # first 2 bars: highs 100.5/105.5, lows 99.5/104.5
    assert orange.high == pytest.approx(105.5)
    assert orange.low == pytest.approx(99.5)
    assert orange.width == pytest.approx(6.0)


def test_historical_volatility_matches_manual():
    # constant daily up-move => zero return variance => zero HV
    closes = pd.Series([100.0 * (1.01**i) for i in range(30)])
    assert historical_volatility(closes, window=20) == pytest.approx(0.0, abs=1e-9)


def test_historical_volatility_known_two_state():
    # alternating returns give a computable stdev; check annualization factor
    closes = pd.Series([100, 110, 100, 110, 100, 110, 100], dtype="float64")
    hv = historical_volatility(closes, window=6)
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
    sigma = pd.Series(rets).std(ddof=1) * math.sqrt(252)
    assert hv == pytest.approx(sigma)


def test_historical_volatility_needs_enough_data():
    with pytest.raises(ValueError, match="need >="):
        historical_volatility(pd.Series([100.0, 101.0]), window=20)


def _quotes() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "strike": [495.0, 500.0, 505.0, 495.0, 500.0, 505.0],
            "option_type": ["C", "C", "C", "P", "P", "P"],
            "bid": [8.0, 4.0, 1.5, 1.0, 3.5, 7.0],
            "ask": [8.4, 4.2, 1.7, 1.2, 3.7, 7.4],
        }
    )


def test_bid_ask_spread_per_row():
    spreads = bid_ask_spread(_quotes())
    assert spreads.iloc[0] == pytest.approx(0.4)
    assert spreads.iloc[1] == pytest.approx(0.2)


def test_atm_straddle_picks_nearest_strike():
    s = atm_straddle(_quotes(), spot=501.0)
    assert s.strike == pytest.approx(500.0)
    # call mid (4.0+4.2)/2=4.1, put mid (3.5+3.7)/2=3.6
    assert s.call_mid == pytest.approx(4.1)
    assert s.put_mid == pytest.approx(3.6)
    assert s.mark == pytest.approx(7.7)


def test_atm_straddle_requires_both_legs():
    calls_only = _quotes()[_quotes()["option_type"] == "C"]
    with pytest.raises(ValueError, match="missing a call or put"):
        atm_straddle(calls_only, spot=500.0)
