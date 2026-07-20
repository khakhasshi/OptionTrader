"""Vol Engine: implied vs realized volatility state (DESIGN 4.4).

Pure, deterministic functions over standardized bars plus optional option
inputs. IV/HV state, implied/realized moves, and the intraday interpretation
are the single Python authority (CLAUDE.md §3). Missing option data degrades to
``Unknown``/``None`` rather than being fabricated.
"""

from app.vol.engine import (
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
    VolState,
    evaluate,
)

__all__ = [
    "IV_CHEAP",
    "IV_FAIR",
    "IV_RICH",
    "IV_UNKNOWN",
    "IV_VERY_RICH",
    "LONG_VOL",
    "NO_CHASE",
    "SHORT_VOL",
    "UNDECIDED",
    "VolInputs",
    "VolState",
    "evaluate",
]
