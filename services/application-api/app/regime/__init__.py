"""Regime Engine: intraday market-state classification (DESIGN 4.3).

Pure, deterministic scoring over standardized bar frames. The Trend/Range
scoring rules are the single Python authority for regime detection (CLAUDE.md
§3); no wall-clock, no RNG, so a given session-so-far frame always yields the
same :class:`RegimeState`.
"""

from app.regime.engine import (
    CHAOS,
    EVENT,
    NO_TRADE,
    RANGE,
    TREND,
    RegimeInputs,
    RegimeState,
    evaluate,
)

__all__ = [
    "CHAOS",
    "EVENT",
    "NO_TRADE",
    "RANGE",
    "TREND",
    "RegimeInputs",
    "RegimeState",
    "evaluate",
]
