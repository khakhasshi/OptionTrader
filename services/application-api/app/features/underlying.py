"""Underlying-only research features.

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
_SESSION_OPEN_HOUR = 9
_SESSION_OPEN_MINUTE = 30


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

    Uses provider bar VWAP when available and valid. A missing bar VWAP falls
    back to that bar's close; callers responsible for trading authorization
    must separately downgrade data health when a fallback was required.
    """
    if bars.empty:
        raise ValueError("no bars")
    vol = bars["volume"].astype("float64")
    close = bars["close"].astype("float64")
    if "vwap" in bars:
        provider_vwap = bars["vwap"].astype("float64")
        price = provider_vwap.where(provider_vwap.notna() & (provider_vwap > 0), close)
    else:
        price = close
    total = float(vol.sum())
    if total <= 0:
        return float(price.mean())
    return float((price * vol).sum() / total)


def opening_range(bars: pd.DataFrame, minutes: int = 15) -> OpeningRange:
    """High/low over the fixed 09:30 ET session opening window.

    The complete one-minute sequence must be present. Treating the first row
    in a partial dataset as the market open would turn a data outage into a
    valid trading signal, so incomplete windows fail closed.
    """
    if bars.empty:
        raise ValueError("no bars")
    if minutes <= 0:
        raise ValueError("minutes must be positive")
    ordered = bars.sort_values("occurred_at_utc").copy()
    if "timestamp_et" in ordered:
        timestamps_et = pd.to_datetime(ordered["timestamp_et"], utc=True).dt.tz_convert(
            "America/New_York"
        )
    else:
        timestamps_et = pd.to_datetime(ordered["occurred_at_utc"], utc=True).dt.tz_convert(
            "America/New_York"
        )
    trading_dates = timestamps_et.dt.date.unique()
    if len(trading_dates) != 1:
        raise ValueError("opening range requires exactly one trading date")
    session_open = pd.Timestamp(
        year=trading_dates[0].year,
        month=trading_dates[0].month,
        day=trading_dates[0].day,
        hour=_SESSION_OPEN_HOUR,
        minute=_SESSION_OPEN_MINUTE,
        tz="America/New_York",
    )
    deadline = session_open + pd.Timedelta(minutes=minutes)
    window = ordered[(timestamps_et >= session_open) & (timestamps_et < deadline)]
    expected = pd.date_range(session_open, periods=minutes, freq="min")
    actual = pd.DatetimeIndex(timestamps_et[window.index]).drop_duplicates().sort_values()
    if not actual.equals(expected):
        raise ValueError("opening range is incomplete or contains duplicate minutes")
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
