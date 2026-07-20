"""Vol Engine tests (DESIGN 4.4): moves, IV/HV state, intraday interpretation."""

from __future__ import annotations

from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from app.vol import (
    IV_CHEAP,
    IV_FAIR,
    IV_RICH,
    IV_UNKNOWN,
    IV_VERY_RICH,
    LONG_VOL,
    NO_CHASE,
    SHORT_VOL,
    UNDECIDED,
    VolInputs,
    evaluate,
)

SRC_TZ = ZoneInfo("America/New_York")


def _bars(closes: list[float]) -> pd.DataFrame:
    base = pd.Timestamp("2026-07-09 09:30:00", tz=SRC_TZ)
    n = len(closes)
    ts = pd.Series([base + pd.Timedelta(minutes=i) for i in range(n)])
    return pd.DataFrame(
        {
            "occurred_at_utc": ts.dt.tz_convert("UTC"),
            "open": [closes[0]] * n,
            "high": [c + 0.25 for c in closes],
            "low": [c - 0.25 for c in closes],
            "close": closes,
            "volume": [1000] * n,
        }
    )


def test_empty_bars_rejected() -> None:
    empty = pd.DataFrame({c: [] for c in ["occurred_at_utc", "open", "high", "low", "close"]})
    with pytest.raises(ValueError, match="no bars"):
        evaluate(empty)


def test_realized_move_from_bars_only() -> None:
    # open 500, last close 505; session low 499.75 (first bar low). realized
    # move = max(|505-500|, |505-high|, |505-499.75|)/505 = 5.25/505.
    state = evaluate(_bars([500.0 + i for i in range(6)]))
    assert state.realized_move == pytest.approx(5.25 / 505.0, rel=1e-6)
    # no option inputs -> implied move unknown, IV/HV unknown, undecided
    assert state.implied_move is None
    assert state.iv_hv_state == IV_UNKNOWN
    assert state.interpretation == UNDECIDED
    assert set(state.unavailable) >= {
        "straddle_mark",
        "atm_iv",
        "hv_20",
        "hv_60",
        "straddle_series",
    }


def test_implied_move_from_straddle() -> None:
    state = evaluate(_bars([500.0] * 6), inputs=VolInputs(straddle_mark=5.0))
    # spot 500, straddle 5 -> implied move 0.01
    assert state.implied_move == pytest.approx(0.01)


def test_iv_hv_state_thresholds() -> None:
    bars = _bars([500.0 + (0.5 if i % 2 else -0.5) for i in range(30)])
    hv = 0.20
    # HV is supplied from daily history; session minute closes are not reused.
    assert (
        evaluate(bars, inputs=VolInputs(atm_iv=hv * 0.7, hv_20=hv, hv_60=0.18)).iv_hv_state
        == IV_CHEAP
    )
    assert evaluate(bars, inputs=VolInputs(atm_iv=hv, hv_20=hv, hv_60=0.18)).iv_hv_state == IV_FAIR
    assert (
        evaluate(bars, inputs=VolInputs(atm_iv=hv * 1.3, hv_20=hv, hv_60=0.18)).iv_hv_state
        == IV_RICH
    )
    assert (
        evaluate(bars, inputs=VolInputs(atm_iv=hv * 1.6, hv_20=hv, hv_60=0.18)).iv_hv_state
        == IV_VERY_RICH
    )


def test_intraday_closes_are_not_used_as_daily_hv() -> None:
    state = evaluate(_bars([500.0 + i for i in range(30)]), inputs=VolInputs(atm_iv=0.2))
    assert state.hv_20 is None
    assert state.hv_60 is None
    assert state.iv_hv_state == IV_UNKNOWN


@pytest.mark.parametrize(
    "inputs",
    [
        VolInputs(atm_iv=-0.1),
        VolInputs(hv_20=float("nan")),
        VolInputs(straddle_mark=0.0),
        VolInputs(straddle_series=[5.0, -1.0]),
    ],
)
def test_invalid_vol_inputs_fail_closed(inputs: VolInputs) -> None:
    with pytest.raises(ValueError):
        evaluate(_bars([500.0] * 6), inputs=inputs)


def test_interpretation_short_vol() -> None:
    # tiny realized move vs large implied, straddle decaying -> Short Vol
    bars = _bars([500.0, 500.1, 499.9, 500.0, 500.05, 500.0])
    state = evaluate(
        bars,
        inputs=VolInputs(straddle_mark=10.0, straddle_series=[10.0, 8.0]),
    )
    assert state.realized_implied_ratio is not None
    assert state.realized_implied_ratio < 0.4
    assert state.interpretation == SHORT_VOL


def test_interpretation_long_vol() -> None:
    # big realized move vs small implied, straddle expanding -> Long Vol
    bars = _bars([500.0 + i * 1.0 for i in range(8)])
    state = evaluate(
        bars,
        inputs=VolInputs(straddle_mark=3.0, straddle_series=[3.0, 5.0]),
    )
    assert state.realized_implied_ratio is not None
    assert state.realized_implied_ratio > 0.6
    assert state.interpretation == LONG_VOL


def test_interpretation_no_chase() -> None:
    # realized ~ implied and straddle not expanding -> No Chase
    bars = _bars([500.0 + i * 0.5 for i in range(6)])  # moves ~2.5 over session
    spot = 502.5
    # realized move = 2.5/502.5 ~= 0.004975; set implied equal so ratio ~1
    implied_target = 2.5 / spot
    state = evaluate(
        bars,
        inputs=VolInputs(straddle_mark=implied_target * spot, straddle_series=[5.0, 4.0]),
    )
    assert state.realized_implied_ratio is not None
    assert 0.9 <= state.realized_implied_ratio <= 1.1
    assert state.interpretation == NO_CHASE


def test_deterministic_repeat() -> None:
    bars = _bars([500.0 + i * 0.3 for i in range(20)])
    inp = VolInputs(atm_iv=0.2, straddle_mark=4.0, straddle_series=[4.0, 4.5])
    assert evaluate(bars, inputs=inp) == evaluate(bars, inputs=inp)
