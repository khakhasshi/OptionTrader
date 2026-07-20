"""Strategy Engine: playbook selection + read-only risk pre-check (DESIGN 4.5).

Proposes a candidate playbook from regime + vol state. Never authorizes a
trade — the Rust Gateway is the sole authority (CLAUDE.md §2).
"""

from app.strategy.engine import (
    EVENT_VOL_CRUSH,
    LONG_GAMMA,
    NO_TRADE,
    SHORT_PREMIUM,
    RiskLimits,
    StrategyDecision,
    StrategyInputs,
    decide,
)

__all__ = [
    "EVENT_VOL_CRUSH",
    "LONG_GAMMA",
    "NO_TRADE",
    "SHORT_PREMIUM",
    "RiskLimits",
    "StrategyDecision",
    "StrategyInputs",
    "decide",
]
