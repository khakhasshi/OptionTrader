"""Underlying-only features: session VWAP, opening range, historical volatility.

All functions are deterministic and operate on standardized bar frames
(columns from :mod:`app.ingestion.standardize`) or plain close series. Prices
are returned as floats here; string/decimal rendering for the MarketSnapshot
contract happens at the replay boundary.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd

# Annualization for a 252-trading-day year, applied to daily log returns.
_TRADING_DAYS = 252


@dataclass(frozen=True)
class OpeningRange:
    """High/low over the first ``minutes`` of the session."""

    high: float
    low: float
    minutes: int

    @property
    def width(self) -> float:
        return self.high - self.low


def session_vwap(bars: pd.DataFrame) -> float:
    """Volume-weighted average price over the whole frame.

    Uses ``close`` as the per-bar price (1-minute bars; close is the standard
    proxy). Falls back to a simple mean if total volume is zero.
    """
    if bars.empty:
        raise ValueError("no bars")
    vol = bars["volume"].astype("float64")
    price = bars["close"].astype("float64")
    total = float(vol.sum())
    if total <= 0:
        return float(price.mean())
    return float((price * vol).sum() / total)


def opening_range(bars: pd.DataFrame, minutes: int = 15) -> OpeningRange:
    """High/low of the first ``minutes`` bars, ordered by ``occurred_at_utc``.

    Assumes 1-minute bars; the first ``minutes`` rows define the window.
    """
    if bars.empty:
        raise ValueError("no bars")
    if minutes <= 0:
        raise ValueError("minutes must be positive")
    ordered = bars.sort_values("occurred_at_utc")
    window = ordered.head(minutes)
    return OpeningRange(
        high=float(window["high"].max()),
        low=float(window["low"].min()),
        minutes=minutes,
    )


def historical_volatility(closes: pd.Series, window: int) -> float:
    """Annualized close-to-close historical volatility over ``window`` returns.

    Computes the sample standard deviation of daily log returns across the most
    recent ``window`` returns, annualized by ``sqrt(252)``. Requires at least
    ``window + 1`` closes.
    """
    if window < 2:
        raise ValueError("window must be >= 2")
    closes = closes.astype("float64").dropna()
    if len(closes) < window + 1:
        raise ValueError(f"need >= {window + 1} closes, got {len(closes)}")
    log_ret = (closes / closes.shift(1)).apply(lambda x: math.log(x)).dropna()
    recent = log_ret.tail(window)
    daily_sigma = float(recent.std(ddof=1))
    return daily_sigma * math.sqrt(_TRADING_DAYS)
