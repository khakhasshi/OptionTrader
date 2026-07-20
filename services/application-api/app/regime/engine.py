"""Regime Engine (DESIGN 4.3): classify intraday market state from bars.

Consumes a standardized session-so-far bar frame (columns from
:mod:`app.ingestion.standardize`) and emits a :class:`RegimeState` with a
Trend score, a Range score, per-rule component contributions, and the final
regime label (Trend / Range / Event / Chaos / No Trade).

The scoring rules are the single Python authority for regime detection (see
CLAUDE.md §3): Rust computes only the deterministic underlying features (VWAP,
opening range, ...); regime decisions live here and nowhere else.

Two sub-signals depend on data blocked by ASSUMPTIONS Q1 (option quotes /
multi-day history): "volume above the 20-day same-minute average" and the ATM
straddle trend. They are optional inputs. When absent they contribute 0 and are
listed in ``unavailable`` — never fabricated — so a score is always explainable
and reproducible from whatever data was actually present.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

# Minimum consecutive 1-minute bars on one side of VWAP to count as "sustained".
_SUSTAINED_MINUTES = 10
# Number of VWAP crossings above which the session looks range-bound.
_RANGE_CROSSINGS = 3

TREND = "Trend"
RANGE = "Range"
EVENT = "Event"
CHAOS = "Chaos"
NO_TRADE = "No Trade"


@dataclass(frozen=True)
class RegimeState:
    """Outcome of one regime evaluation, as of the last bar in the frame."""

    regime: str
    trend_score: int
    range_score: int
    components: dict[str, int]
    unavailable: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RegimeInputs:
    """Optional external signals not derivable from the bar frame alone.

    Both are blocked by ASSUMPTIONS Q1; leave ``None`` until wired to real data.

    * ``volume_vs_20d`` — ratio of current same-minute volume to its 20-day
      average (>1 means above average).
    * ``straddle_series`` — chronological ATM straddle marks for the session so
      far; its slope decides the straddle rising/decaying sub-signals.
    """

    volume_vs_20d: float | None = None
    straddle_series: list[float] | None = None
    event_window: bool = False


def _running_vwap(bars: pd.DataFrame) -> pd.Series:
    price = bars["close"].astype("float64")
    vol = bars["volume"].astype("float64")
    cum_pv = (price * vol).cumsum()
    cum_v = vol.cumsum()
    # Fall back to price where cumulative volume is zero (mirrors ReplayClock).
    return (cum_pv / cum_v).where(cum_v > 0, price)


def _sustained_side_minutes(close: pd.Series, vwap: pd.Series) -> int:
    """Consecutive trailing bars price has held one side of VWAP."""
    side = (close > vwap).astype(int) - (close < vwap).astype(int)
    last = side.iloc[-1]
    if last == 0:
        return 0
    run = 0
    for value in reversed(side.tolist()):
        if value == last:
            run += 1
        else:
            break
    return run


def _vwap_crossings(close: pd.Series, vwap: pd.Series) -> int:
    """Number of times close-vs-VWAP sign flips (ignoring exact touches)."""
    sign = (close - vwap).apply(lambda d: 1 if d > 0 else (-1 if d < 0 else 0))
    nonzero = sign[sign != 0]
    return int((nonzero.diff().fillna(0) != 0).sum() - 1) if len(nonzero) > 1 else 0


def _body_wick_ratio(bars: pd.DataFrame) -> float:
    """Mean |close-open| / (high-low); smaller means small bodies, more wick."""
    rng = (bars["high"] - bars["low"]).astype("float64")
    body = (bars["close"] - bars["open"]).abs().astype("float64")
    ratio = (body / rng).where(rng > 0, 1.0)
    return float(ratio.mean())
def _straddle_slope(series: list[float] | None) -> int:
    """+1 rising, -1 decaying, 0 flat/unknown, from first-to-last comparison."""
    if not series or len(series) < 2:
        return 0
    if series[-1] > series[0]:
        return 1
    if series[-1] < series[0]:
        return -1
    return 0


def evaluate(
    bars: pd.DataFrame,
    opening_range_minutes: int = 15,
    inputs: RegimeInputs | None = None,
) -> RegimeState:
    """Classify the regime as of the last bar in ``bars`` (DESIGN 4.3).

    ``bars`` is the standardized session-so-far frame, ordered or unordered on
    ``occurred_at_utc``. Trend/Range scores are summed from the design's
    per-rule contributions; the label follows the design's threshold table.
    """
    if bars.empty:
        raise ValueError("no bars")
    if opening_range_minutes <= 0:
        raise ValueError("opening_range_minutes must be positive")
    inp = inputs or RegimeInputs()

    ordered = bars.sort_values("occurred_at_utc").reset_index(drop=True)
    close = ordered["close"].astype("float64")
    vwap = _running_vwap(ordered)

    first_ts = ordered["occurred_at_utc"].iloc[0]
    or_deadline = first_ts + pd.Timedelta(minutes=opening_range_minutes)
    or_mask = ordered["occurred_at_utc"] < or_deadline
    or_window = ordered[or_mask]
    or_high = float(or_window["high"].max()) if not or_window.empty else float("nan")
    or_low = float(or_window["low"].min()) if not or_window.empty else float("nan")
    last_close = float(close.iloc[-1])

    unavailable: list[str] = []
    c: dict[str, int] = {}

    # --- Trend score ---
    c["trend_sustained_vwap_side"] = (
        2 if _sustained_side_minutes(close, vwap) >= _SUSTAINED_MINUTES else 0
    )
    broke_or = bool(last_close > or_high or last_close < or_low) if or_window.size else False
    c["trend_break_opening_range"] = 2 if broke_or else 0

    if inp.volume_vs_20d is None:
        unavailable.append("volume_vs_20d")
        c["trend_volume_above_20d"] = 0
    else:
        c["trend_volume_above_20d"] = 1 if inp.volume_vs_20d > 1.0 else 0

    # "retest of VWAP/breakout then continues same direction": approximate as
    # price on the breakout side while still above VWAP on that same side.
    continues = broke_or and _sustained_side_minutes(close, vwap) > 0
    c["trend_retest_continues"] = 1 if continues else 0

    slope = _straddle_slope(inp.straddle_series)
    if inp.straddle_series is None:
        unavailable.append("straddle_series")
    c["trend_straddle_rising"] = 1 if slope > 0 else 0

    trend_score = sum(v for k, v in c.items() if k.startswith("trend_"))

    # --- Range score ---
    c["range_vwap_crossings"] = 2 if _vwap_crossings(close, vwap) > _RANGE_CROSSINGS else 0
    no_continuation = (
        bool(or_low <= last_close <= or_high) if or_window.size and not broke_or else False
    )
    c["range_opening_range_fails"] = 2 if no_continuation else 0

    if inp.volume_vs_20d is None:
        c["range_volume_falling"] = 0
    else:
        c["range_volume_falling"] = 1 if inp.volume_vs_20d < 1.0 else 0

    c["range_straddle_decaying"] = 1 if slope < 0 else 0
    c["range_small_body_more_wick"] = 1 if _body_wick_ratio(ordered) < 0.5 else 0

    range_score = sum(v for k, v in c.items() if k.startswith("range_"))

    regime = _classify(trend_score, range_score, inp.event_window)
    return RegimeState(
        regime=regime,
        trend_score=trend_score,
        range_score=range_score,
        components=c,
        unavailable=unavailable,
    )


def _classify(trend_score: int, range_score: int, event_window: bool) -> str:
    """Apply the DESIGN 4.3 threshold table.

    Event takes precedence when a timed-event window is active; Chaos when both
    scores are high; then Trend or Range; otherwise No Trade.
    """
    if event_window:
        return EVENT
    if trend_score >= 5 and range_score >= 5:
        return CHAOS
    if trend_score >= 5 and range_score < 4:
        return TREND
    if range_score >= 5 and trend_score < 4:
        return RANGE
    return NO_TRADE

