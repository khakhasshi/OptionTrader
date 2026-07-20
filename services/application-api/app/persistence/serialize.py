"""Turn engine outputs into review/audit rows (P1-7).

Pure, side-effect-free: given the regime/vol/strategy results for one tick plus
its identifying context, produce the ``trading.signals`` and
``audit.audit_events`` row dicts. Deterministic — no clock, no RNG, no I/O — so
the same inputs always serialize to the same rows (testable, replayable).

The ``payload`` JSON captures the full engine context (scores, sub-signals,
unavailable inputs, risk notes) so the review layer can reconstruct *why* a
signal was emitted without re-running the engines.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from app.regime import RegimeState
from app.strategy import NO_TRADE, StrategyDecision
from app.vol import VolState


@dataclass(frozen=True)
class SignalContext:
    """Identity + timing for one persisted signal.

    ``occurred_at_utc`` is the decision instant from the replay/live clock — the
    authoritative UTC instant, never a wall-clock ``now()``.
    """

    signal_id: str
    session_id: str
    occurred_at_utc: datetime


def _require_utc(ts: datetime) -> datetime:
    """Reject naive or non-UTC instants: the audit trail must be unambiguous."""
    if ts.tzinfo is None:
        raise ValueError("occurred_at_utc must be timezone-aware (UTC)")
    if ts.utcoffset() != timezone.utc.utcoffset(None):
        raise ValueError("occurred_at_utc must be UTC")
    return ts


def _regime_payload(regime: RegimeState) -> dict[str, object]:
    return {
        "regime": regime.regime,
        "trend_score": regime.trend_score,
        "range_score": regime.range_score,
        "components": dict(regime.components),
        "unavailable": list(regime.unavailable),
    }


def _vol_payload(vol: VolState) -> dict[str, object]:
    return {
        "iv_hv_state": vol.iv_hv_state,
        "interpretation": vol.interpretation,
        "atm_iv": vol.atm_iv,
        "hv_20": vol.hv_20,
        "iv_hv_ratio": vol.iv_hv_ratio,
        "implied_move": vol.implied_move,
        "realized_move": vol.realized_move,
        "realized_implied_ratio": vol.realized_implied_ratio,
        "straddle_mark": vol.straddle_mark,
        "unavailable": list(vol.unavailable),
    }


def _strategy_payload(decision: StrategyDecision) -> dict[str, object]:
    return {
        "playbook": decision.playbook,
        "reason": decision.reason,
        "risk_status": decision.risk_status,
        "risk_notes": list(decision.risk_notes),
        "limits_unconfirmed": decision.limits_unconfirmed,
    }


def build_signal_rows(
    ctx: SignalContext,
    regime: RegimeState,
    vol: VolState,
    decision: StrategyDecision,
) -> tuple[dict[str, object], dict[str, object]]:
    """Serialize one tick into ``(signals_row, audit_row)``.

    The No-Trade reason is recorded whenever the selected playbook is No Trade,
    capturing exactly why nothing was traded — the core review requirement.
    """
    _require_utc(ctx.occurred_at_utc)

    no_trade_reason = decision.reason if decision.playbook == NO_TRADE else None

    payload = {
        "regime": _regime_payload(regime),
        "vol": _vol_payload(vol),
        "strategy": _strategy_payload(decision),
    }

    signal_row = {
        "signal_id": ctx.signal_id,
        "session_id": ctx.session_id,
        "occurred_at_utc": ctx.occurred_at_utc,
        "regime": regime.regime,
        "vol_state": vol.iv_hv_state,
        "strategy_kind": decision.playbook,
        "no_trade_reason": no_trade_reason,
        "payload": payload,
    }

    audit_row = {
        "event_id": f"sig:{ctx.signal_id}",
        "session_id": ctx.session_id,
        "occurred_at_utc": ctx.occurred_at_utc,
        "actor": "strategy-engine",
        "action": "SIGNAL_EMITTED",
        "entity_type": "signal",
        "entity_id": ctx.signal_id,
        "from_status": None,
        "to_status": decision.playbook,
        "payload": {"reason": decision.reason, "risk_status": decision.risk_status},
    }

    return signal_row, audit_row


__all__ = ["SignalContext", "build_signal_rows"]
