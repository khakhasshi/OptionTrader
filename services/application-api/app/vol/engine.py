"""Vol Engine (DESIGN 4.4): implied vs realized volatility relationship.

Given the session-so-far bars plus (when available) the ATM straddle mark and
ATM implied volatility, computes:

  * Implied intraday move  = ATM straddle price / spot
  * Realized intraday move = max(|now-open|, |now-high|, |now-low|) / spot
  * Realized/Implied ratio
  * IV/HV ratio (needs ATM IV + historical vol)

and derives a vol-state label (IV Cheap/Fair/Rich/Very Rich) and an intraday
interpretation (Short Vol / Long Vol / No Chase).

ATM IV and the straddle mark come from an option source blocked by ASSUMPTIONS
Q1. They are optional. When IV is absent the IV/HV state is ``UNKNOWN`` and when
the straddle is absent the implied move is ``None`` — fail closed, never
fabricated, so every derived value is traceable to a real input.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from app.features import historical_volatility

# IV/HV thresholds (DESIGN 4.4). Ordered high-to-low for classification.
_VERY_RICH = 1.5
_RICH = 1.2
_CHEAP = 0.8

IV_CHEAP = "IV Cheap"
IV_FAIR = "IV Fair"
IV_RICH = "IV Rich"
IV_VERY_RICH = "IV Very Rich"
IV_UNKNOWN = "Unknown"

# Intraday interpretation labels.
SHORT_VOL = "Short Vol"
LONG_VOL = "Long Vol"
NO_CHASE = "No Chase"
UNDECIDED = "Undecided"

# Realized/Implied thresholds (DESIGN 4.4).
_RI_LOW = 0.4
_RI_HIGH = 0.6


@dataclass(frozen=True)
class VolState:
    """Outcome of one Vol Engine evaluation, as of the last bar."""

    iv_hv_state: str
    interpretation: str
    atm_iv: float | None
    hv_20: float | None
    iv_hv_ratio: float | None
    implied_move: float | None
    realized_move: float
    realized_implied_ratio: float | None
    straddle_mark: float | None
    unavailable: list[str]


@dataclass(frozen=True)
class VolInputs:
    """Optional option-derived inputs (blocked by ASSUMPTIONS Q1).

    * ``atm_iv`` — ATM implied volatility as an annualized decimal (0.18 = 18%).
    * ``straddle_mark`` — current 0DTE ATM straddle price.
    * ``straddle_series`` — chronological straddle marks; its slope decides
      expansion vs decay for the intraday interpretation.
    """

    atm_iv: float | None = None
    straddle_mark: float | None = None
    straddle_series: list[float] | None = None


def _classify_iv_hv(ratio: float | None) -> str:
    """Map IV/HV ratio to a vol-state label (DESIGN 4.4)."""
    if ratio is None:
        return IV_UNKNOWN
    if ratio > _VERY_RICH:
        return IV_VERY_RICH
    if ratio > _RICH:
        return IV_RICH
    if ratio < _CHEAP:
        return IV_CHEAP
    return IV_FAIR


def _straddle_expanding(series: list[float] | None) -> bool | None:
    """True if last > first, False if last < first, None if unknown/flat."""
    if not series or len(series) < 2:
        return None
    if series[-1] > series[0]:
        return True
    if series[-1] < series[0]:
        return False
    return None


def _interpret(ri_ratio: float | None, expanding: bool | None) -> str:
    """Intraday interpretation (DESIGN 4.4).

    * Realized/Implied < 0.4 and straddle decaying  -> Short Vol
    * Realized/Implied > 0.6 and (still expanding)   -> Long Vol
    * Realized/Implied ~1 and IV not expanding       -> No Chase
    Otherwise Undecided (including when the ratio is unknown).
    """
    if ri_ratio is None:
        return UNDECIDED
    if ri_ratio < _RI_LOW and expanding is False:
        return SHORT_VOL
    if 0.9 <= ri_ratio <= 1.1 and not expanding:
        return NO_CHASE
    if ri_ratio > _RI_HIGH and expanding is not False:
        return LONG_VOL
    return UNDECIDED


def evaluate(
    bars: pd.DataFrame,
    hv_window: int = 20,
    inputs: VolInputs | None = None,
) -> VolState:
    """Evaluate the vol state as of the last bar in ``bars`` (DESIGN 4.4).

    ``bars`` is the standardized session-so-far frame. Realized move uses the
    session open/high/low/last from these bars; implied move and IV/HV need the
    optional option inputs and degrade to ``None``/``Unknown`` when absent.
    """
    if bars.empty:
        raise ValueError("no bars")
    inp = inputs or VolInputs()
    ordered = bars.sort_values("occurred_at_utc").reset_index(drop=True)

    spot = float(ordered["close"].iloc[-1])
    if spot <= 0:
        raise ValueError("spot must be positive")
    session_open = float(ordered["open"].iloc[0])
    session_high = float(ordered["high"].max())
    session_low = float(ordered["low"].min())
    realized_move = (
        max(abs(spot - session_open), abs(spot - session_high), abs(spot - session_low))
        / spot
    )

    unavailable: list[str] = []

    implied_move: float | None = None
    if inp.straddle_mark is not None:
        implied_move = inp.straddle_mark / spot
    else:
        unavailable.append("straddle_mark")

    ri_ratio = (
        realized_move / implied_move if implied_move and implied_move > 0 else None
    )

    hv_20: float | None
    try:
        hv_20 = historical_volatility(ordered["close"].astype("float64"), hv_window)
    except ValueError:
        hv_20 = None
        unavailable.append("hv_20")

    iv_hv_ratio: float | None = None
    if inp.atm_iv is None:
        unavailable.append("atm_iv")
    elif hv_20 is not None and hv_20 > 0:
        iv_hv_ratio = inp.atm_iv / hv_20

    expanding = _straddle_expanding(inp.straddle_series)
    if inp.straddle_series is None:
        unavailable.append("straddle_series")

    return VolState(
        iv_hv_state=_classify_iv_hv(iv_hv_ratio),
        interpretation=_interpret(ri_ratio, expanding),
        atm_iv=inp.atm_iv,
        hv_20=hv_20,
        iv_hv_ratio=iv_hv_ratio,
        implied_move=implied_move,
        realized_move=realized_move,
        realized_implied_ratio=ri_ratio,
        straddle_mark=inp.straddle_mark,
        unavailable=unavailable,
    )
