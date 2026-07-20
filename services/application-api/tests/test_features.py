"""Tests for deterministic features (P1-2): underlying + option quote math."""

from __future__ import annotations

import json
import math
from datetime import date
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from app.features import historical_volatility, opening_range, session_vwap
from app.features.options import atm_straddle, bid_ask_spread

SRC_TZ = ZoneInfo("Etc/GMT+4")
_ROOT = Path(__file__).resolve().parents[3]


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
            "vwap": prices,
            "volume": vols,
        }
    )


def test_session_vwap_is_volume_weighted() -> None:
    bars = _bars([100.0, 200.0], [1, 3])
    # (100*1 + 200*3) / 4 = 175
    assert session_vwap(bars) == pytest.approx(175.0)


def test_session_vwap_prefers_provider_bar_vwap() -> None:
    bars = _bars([100.0, 100.0], [1, 3])
    bars["vwap"] = [90.0, 110.0]
    assert session_vwap(bars) == pytest.approx(105.0)


def test_session_vwap_zero_volume_falls_back_to_mean() -> None:
    bars = _bars([100.0, 200.0], [0, 0])
    assert session_vwap(bars) == pytest.approx(150.0)


def test_opening_range_uses_first_n_bars() -> None:
    bars = _bars([100.0, 105.0, 90.0, 110.0], [1, 1, 1, 1])
    orange = opening_range(bars, minutes=2)
    # first 2 bars: highs 100.5/105.5, lows 99.5/104.5
    assert orange.high == pytest.approx(105.5)
    assert orange.low == pytest.approx(99.5)
    assert orange.width == pytest.approx(6.0)


def test_opening_range_rejects_late_or_gapped_data() -> None:
    late = _bars([100.0, 101.0], [1, 1])
    late["occurred_at_utc"] += pd.Timedelta(minutes=30)
    with pytest.raises(ValueError, match="incomplete"):
        opening_range(late, minutes=2)

    gapped = _bars([100.0, 101.0], [1, 1])
    gapped.loc[1, "occurred_at_utc"] += pd.Timedelta(minutes=1)
    with pytest.raises(ValueError, match="incomplete"):
        opening_range(gapped, minutes=2)


def test_historical_volatility_matches_manual() -> None:
    # constant daily up-move => zero return variance => zero HV
    closes = pd.Series([100.0 * (1.01**i) for i in range(30)])
    assert historical_volatility(closes, window=20) == pytest.approx(0.0, abs=1e-9)


def test_historical_volatility_known_two_state() -> None:
    # alternating returns give a computable stdev; check annualization factor
    closes = pd.Series([100, 110, 100, 110, 100, 110, 100], dtype="float64")
    hv = historical_volatility(closes, window=6)
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
    sigma = pd.Series(rets).std(ddof=1) * math.sqrt(252)
    assert hv == pytest.approx(sigma)


def test_historical_volatility_needs_enough_data() -> None:
    with pytest.raises(ValueError, match="need >="):
        historical_volatility(pd.Series([100.0, 101.0]), window=20)


def test_shared_cross_language_feature_fixture() -> None:
    fixture = json.loads(
        (_ROOT / "packages/contracts/fixtures/market_features.sample.json").read_text()
    )
    bars = pd.DataFrame(fixture["bars"])
    session_open = pd.Timestamp("2026-07-09T09:30:00", tz="America/New_York")
    bars["occurred_at_utc"] = [
        (session_open + pd.Timedelta(minutes=int(minute) - 570)).tz_convert("UTC")
        for minute in bars["minute_et"]
    ]
    expected = fixture["expected"]
    assert session_vwap(bars) == pytest.approx(expected["session_vwap"])
    orange = opening_range(bars, minutes=2)
    assert orange.high == pytest.approx(expected["opening_range_high"])
    assert orange.low == pytest.approx(expected["opening_range_low"])
    closes = pd.Series(fixture["daily_closes"], dtype="float64")
    assert historical_volatility(closes, 20) == pytest.approx(expected["hv20"])
    assert historical_volatility(closes, 60) == pytest.approx(expected["hv60"])


def _quotes() -> pd.DataFrame:
    occurred = pd.Timestamp("2026-07-09T14:00:00Z")
    return pd.DataFrame(
        {
            "underlying": ["QQQ"] * 6,
            "expiry": [date(2026, 7, 9)] * 6,
            "strike": [495.0, 500.0, 505.0, 495.0, 500.0, 505.0],
            "option_type": ["C", "C", "C", "P", "P", "P"],
            "occurred_at_utc": [occurred] * 6,
            "bid": [8.0, 4.0, 1.5, 1.0, 3.5, 7.0],
            "ask": [8.4, 4.2, 1.7, 1.2, 3.7, 7.4],
        }
    )


def test_bid_ask_spread_per_row() -> None:
    spreads = bid_ask_spread(_quotes())
    assert spreads.iloc[0] == pytest.approx(0.4)
    assert spreads.iloc[1] == pytest.approx(0.2)


def test_atm_straddle_picks_nearest_strike() -> None:
    s = atm_straddle(
        _quotes(),
        spot=501.0,
        underlying="QQQ",
        expiry="2026-07-09",
        as_of=pd.Timestamp("2026-07-09T14:00:00Z"),
    )
    assert s.strike == pytest.approx(500.0)
    # call mid (4.0+4.2)/2=4.1, put mid (3.5+3.7)/2=3.6
    assert s.call_mid == pytest.approx(4.1)
    assert s.put_mid == pytest.approx(3.6)
    assert s.mark == pytest.approx(7.7)


def test_atm_straddle_requires_both_legs() -> None:
    calls_only = _quotes()[_quotes()["option_type"] == "C"]
    with pytest.raises(ValueError, match="missing a call or put"):
        atm_straddle(
            calls_only,
            spot=500.0,
            underlying="QQQ",
            expiry="2026-07-09",
            as_of=pd.Timestamp("2026-07-09T14:00:00Z"),
        )


def test_atm_straddle_never_pairs_different_expiries() -> None:
    quotes = _quotes()
    quotes.loc[quotes["option_type"] == "P", "expiry"] = date(2026, 7, 10)
    with pytest.raises(ValueError, match="missing a call or put"):
        atm_straddle(
            quotes,
            spot=500.0,
            underlying="QQQ",
            expiry="2026-07-09",
            as_of=pd.Timestamp("2026-07-09T14:00:00Z"),
        )


def test_atm_straddle_rejects_mismatched_snapshot_times() -> None:
    quotes = _quotes()
    quotes.loc[quotes["option_type"] == "P", "occurred_at_utc"] = pd.Timestamp(
        "2026-07-09T13:59:59.500Z"
    )
    with pytest.raises(ValueError, match="timestamps do not match"):
        atm_straddle(
            quotes,
            spot=500.0,
            underlying="QQQ",
            expiry="2026-07-09",
            as_of=pd.Timestamp("2026-07-09T14:00:00Z"),
        )


def test_atm_straddle_rejects_stale_or_crossed_quotes() -> None:
    stale = _quotes()
    with pytest.raises(ValueError, match="no fresh quotes"):
        atm_straddle(
            stale,
            spot=500.0,
            underlying="QQQ",
            expiry="2026-07-09",
            as_of=pd.Timestamp("2026-07-09T14:00:02Z"),
        )

    crossed = _quotes()
    crossed.loc[0, "ask"] = crossed.loc[0, "bid"] - 0.01
    with pytest.raises(ValueError, match="crossed option market"):
        bid_ask_spread(crossed)
