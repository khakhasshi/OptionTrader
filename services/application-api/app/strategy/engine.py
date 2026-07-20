"""Strategy Engine (DESIGN 4.5): pick a playbook from regime + vol state.

Given a :class:`~app.regime.RegimeState`, a :class:`~app.vol.VolState`, the
current Eastern time, and a spread-quality flag, selects one of:

  * Long Gamma      — Trend + IV Cheap/Fair, breakout, in an allowed window
  * Short Premium    — Range + IV Rich/Very Rich, no breakout, decaying straddle
  * Event Vol Crush  — an active timed-event window
  * No Trade         — anything else, or a failed read-only risk pre-check

Authority boundary (CLAUDE.md §2): this engine only *proposes* a candidate. The
read-only Initial Risk pre-check here can only downgrade a proposal to No Trade;
it can never authorize a trade. The Rust Risk & Execution Gateway remains the
sole authority and re-checks everything (Final Risk Check) before any submit.

Hard risk numbers (max loss, position size) are blocked by ASSUMPTIONS Q3, so
:class:`RiskLimits` ships conservative placeholders flagged ``UNCONFIRMED``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import time

from app.regime import CHAOS, EVENT, RANGE, TREND, RegimeState
from app.vol import IV_CHEAP, IV_FAIR, IV_RICH, IV_VERY_RICH, SHORT_VOL, VolState

LONG_GAMMA = "Long Gamma"
SHORT_PREMIUM = "Short Premium"
EVENT_VOL_CRUSH = "Event Vol Crush"
NO_TRADE = "No Trade"

# Allowed Long Gamma windows (ET), DESIGN 4.5.
_LG_WINDOWS = ((time(9, 45), time(11, 0)), (time(14, 0), time(15, 30)))
# Short Premium may not open in the first 30 minutes (DESIGN: 30-45m after open).
_SP_WINDOW = (time(10, 0), time(11, 30))
_EVENT_GUARD_MINUTES = 15


@dataclass(frozen=True)
class RiskLimits:
    """Read-only risk limits for the Initial pre-check.

    UNCONFIRMED (ASSUMPTIONS Q3): these are conservative placeholders, not
    approved production numbers. They must be replaced and dual-approved before
    paper/live. ``max_spread_pct`` is the option bid/ask ceiling from DESIGN 4.5
    (reject spreads wider than 10%).
    """

    max_spread_pct: float = 0.10
    unconfirmed: bool = True


@dataclass(frozen=True)
class StrategyInputs:
    """Context needed to select and pre-check a playbook.

    * ``now_et`` — current Eastern wall-clock time (decision clock).
    * ``spread_ok`` — option bid/ask spread within limits (from Rust features).
    * ``breakout`` — QQQ has broken the opening range with volume.
    * ``data_healthy`` — DataHealth == HEALTHY and reconciled; else fail closed.
    """

    now_et: time
    spread_ok: bool = True
    breakout: bool = False
    data_healthy: bool = True
    minutes_to_major_event: int | None = None
    event_released: bool = False


@dataclass(frozen=True)
class StrategyDecision:
    """Selected playbook plus the audit trail of why (and why not others)."""

    playbook: str
    reason: str
    risk_status: str  # PASS_READONLY | BLOCKED
    risk_notes: list[str] = field(default_factory=list)
    limits_unconfirmed: bool = True


def _in_long_gamma_window(now: time) -> bool:
    return any(start <= now <= end for start, end in _LG_WINDOWS)


def _in_short_premium_window(now: time) -> bool:
    return _SP_WINDOW[0] <= now <= _SP_WINDOW[1]


def _select(regime: RegimeState, vol: VolState, inp: StrategyInputs) -> tuple[str, str]:
    """Pick a playbook and a human-readable reason (no risk check yet).

    Event takes precedence; then the Long Gamma and Short Premium condition
    sets from DESIGN 4.5; Chaos and everything unmatched fall to No Trade.
    """
    if regime.regime == EVENT:
        if not inp.event_released:
            return NO_TRADE, "regime=Event but release not confirmed: wait"
        if vol.iv_hv_state not in (IV_RICH, IV_VERY_RICH):
            return NO_TRADE, "post-event but IV is not rich enough for vol crush"
        return EVENT_VOL_CRUSH, "event released + IV rich: event vol-crush candidate"
    if regime.regime == CHAOS:
        return NO_TRADE, "regime=Chaos: conflicting trend/range signals"
    if (
        inp.minutes_to_major_event is not None
        and 0 <= inp.minutes_to_major_event <= _EVENT_GUARD_MINUTES
    ):
        return NO_TRADE, "major event within 15 minutes"

    if regime.regime == TREND:
        if vol.iv_hv_state not in (IV_CHEAP, IV_FAIR):
            return NO_TRADE, f"Trend but IV not cheap/fair (state={vol.iv_hv_state})"
        if not inp.breakout:
            return NO_TRADE, "Trend but no confirmed opening-range breakout"
        if not _in_long_gamma_window(inp.now_et):
            return NO_TRADE, f"Trend setup outside allowed window ({inp.now_et:%H:%M} ET)"
        return LONG_GAMMA, "Trend + IV cheap/fair + breakout in allowed window"

    if regime.regime == RANGE:
        if vol.iv_hv_state not in (IV_RICH, IV_VERY_RICH):
            return NO_TRADE, f"Range but IV not rich (state={vol.iv_hv_state})"
        if inp.breakout:
            return NO_TRADE, "Range but opening range broke: no premium sale"
        if not _in_short_premium_window(inp.now_et):
            return NO_TRADE, f"Range setup outside 10:00-11:30 window ({inp.now_et:%H:%M} ET)"
        if inp.minutes_to_major_event is None:
            return NO_TRADE, "Range but event proximity is unknown"
        if vol.interpretation != SHORT_VOL:
            return NO_TRADE, "Range but straddle decay/realized-implied confirmation is absent"
        required_vol = (vol.atm_iv, vol.hv_20, vol.straddle_mark, vol.realized_implied_ratio)
        if any(value is None for value in required_vol):
            return NO_TRADE, "Range but required volatility inputs are unavailable"
        return SHORT_PREMIUM, "Range + IV rich + confirmed decay + no event/breakout"

    return NO_TRADE, f"regime={regime.regime}: no matching playbook"


def _risk_precheck(
    playbook: str, vol: VolState, inp: StrategyInputs, limits: RiskLimits
) -> tuple[str, list[str]]:
    """Read-only Initial Risk pre-check. Can only downgrade, never authorize.

    Returns ``(status, notes)`` where status is ``BLOCKED`` if any hard gate
    fails. No Trade proposals still run the check so the audit trail is uniform.
    """
    notes: list[str] = []
    if playbook == NO_TRADE:
        return "PASS_READONLY", notes

    blocked = False
    if not inp.data_healthy:
        notes.append("data not HEALTHY/reconciled: fail closed")
        blocked = True
    if not inp.spread_ok:
        notes.append(f"option spread exceeds {limits.max_spread_pct:.0%} ceiling")
        blocked = True
    if limits.unconfirmed:
        notes.append("risk limits UNCONFIRMED (ASSUMPTIONS Q3): placeholder only")

    return ("BLOCKED" if blocked else "PASS_READONLY"), notes


def decide(
    regime: RegimeState,
    vol: VolState,
    inp: StrategyInputs,
    limits: RiskLimits | None = None,
) -> StrategyDecision:
    """Select a playbook and attach a read-only risk verdict (DESIGN 4.5).

    A ``BLOCKED`` pre-check forces the playbook to No Trade while preserving the
    original selection reason plus the blocking notes, so review can see both
    what was proposed and why it was stopped.
    """
    limits = limits or RiskLimits()
    playbook, reason = _select(regime, vol, inp)
    status, notes = _risk_precheck(playbook, vol, inp, limits)

    if status == "BLOCKED":
        return StrategyDecision(
            playbook=NO_TRADE,
            reason=f"{playbook} proposed ({reason}) but blocked by risk pre-check",
            risk_status=status,
            risk_notes=notes,
            limits_unconfirmed=limits.unconfirmed,
        )
    return StrategyDecision(
        playbook=playbook,
        reason=reason,
        risk_status=status,
        risk_notes=notes,
        limits_unconfirmed=limits.unconfirmed,
    )
