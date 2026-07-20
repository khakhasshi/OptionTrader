"""Offline reference features for Rust Market Core fixture comparison.

Split by data dependency:

* :mod:`app.features.underlying` — features derivable from QQQ/VIX bars alone
  (session VWAP, opening range, realized/historical volatility). Fully testable
  offline against the on-disk 1-minute data.
* :mod:`app.features.options` — strict same-expiry/snapshot ATM straddle and
  bid/ask spread over normalized option quotes.

Everything here is a pure function of its inputs: no wall-clock, no RNG. These
functions are not a paper/live trading authority.
"""

from app.features.underlying import (
    historical_volatility,
    opening_range,
    session_vwap,
)

__all__ = [
    "historical_volatility",
    "opening_range",
    "session_vwap",
]
