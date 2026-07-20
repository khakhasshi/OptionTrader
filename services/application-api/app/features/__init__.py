"""Deterministic feature computation over standardized bars and snapshots.

Split by data dependency:

* :mod:`app.features.underlying` — features derivable from QQQ/VIX bars alone
  (session VWAP, opening range, realized/historical volatility). Fully testable
  offline against the on-disk 1-minute data.
* :mod:`app.features.options` — ATM straddle mark and bid/ask spread. These
  require an option-quote source not yet ingested; the functions define the
  contract and math and are unit-tested against synthetic quote frames, ready
  to wire to real option data in a later Phase 1 step.

Everything here is a pure function of its inputs: no wall-clock, no RNG.
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
