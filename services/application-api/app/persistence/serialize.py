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

from app.regime import CHAOS, EVENT, NO_TRADE as REGIME_NO_TRADE, RANGE, TREND, RegimeState
from app.strategy import (
    EVENT_VOL_CRUSH,
    LONG_GAMMA,
    NO_TRADE as STRATEGY_NO_TRADE,
    SHORT_PREMIUM,
    StrategyDecision,
)
from app.vol import VolState

_REGIME_CONTRACT = {
    TREND: "Trend",
    RANGE: "Range",
    EVENT: "Event",
    CHAOS: "Chaos",
    REGIME_NO_TRADE: "NoTrade",
}
_STRATEGY_CONTRACT = {
    LONG_GAMMA: "LongGamma",
    SHORT_PREMIUM: "ShortPremium",
    EVENT_VOL_CRUSH: "EventVolCrush",
    STRATEGY_NO_TRADE: "NoTrade",
}
_INITIAL_RISK_CONTRACT = {
    "PASS_READONLY": "PASSED",
    "BLOCKED": "REJECTED",
    "NOT_EVALUATED": "NOT_EVALUATED",
}


@dataclass(frozen=True)
class SignalContext:
    """Identity + timing for one persisted signal.

    ``occurred_at_utc`` is the decision instant from the replay/live clock — the
    authoritative UTC instant, never a wall-clock ``now()``.
    """

    signal_id: str
    session_id: str
    occurred_at_utc: datetime
    rule_version: str


def _require_utc(ts: datetime) -> datetime:
    """Reject naive or non-UTC instants: the audit trail must be unambiguous."""
    if ts.tzinfo is None:
        raise ValueError("occurred_at_utc must be timezone-aware (UTC)")
    if ts.utcoffset() != timezone.utc.utcoffset(None):
        raise ValueError("occurred_at_utc must be UTC")
    return ts


def _contract_enum(mapping: dict[str, str], value: str, field: str) -> str:
    try:
        return mapping[value]
    except KeyError as exc:
        raise ValueError(f"unmapped {field} label: {value!r}") from exc


def build_signal_contract(
    ctx: SignalContext,
    regime: RegimeState,
    decision: StrategyDecision,
) -> dict[str, object]:
    """Build the schema-facing Signal object using contract enum values only."""
    occurred_at_utc = _require_utc(ctx.occurred_at_utc)
    if not ctx.signal_id or not ctx.session_id or not ctx.rule_version:
        raise ValueError("signal_id, session_id and rule_version must be non-empty")
    reason = [decision.reason, *decision.risk_notes]
    if not all(reason):
        raise ValueError("signal reasons must be non-empty")
    return {
        "schema_version": "1.0",
        "signal_id": ctx.signal_id,
        "session_id": ctx.session_id,
        "occurred_at_utc": occurred_at_utc.isoformat().replace("+00:00", "Z"),
        "regime": _contract_enum(_REGIME_CONTRACT, regime.regime, "regime"),
        "strategy": _contract_enum(_STRATEGY_CONTRACT, decision.playbook, "strategy"),
        "initial_risk_status": _contract_enum(
            _INITIAL_RISK_CONTRACT, decision.risk_status, "initial risk status"
        ),
        "reason": reason,
        "rule_version": ctx.rule_version,
    }


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
        "hv_60": vol.hv_60,
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
    signal_contract = build_signal_contract(ctx, regime, decision)
    no_trade_reason = decision.reason if decision.playbook == STRATEGY_NO_TRADE else None

    payload = {
        "signal": signal_contract,
        "regime": _regime_payload(regime),
        "vol": _vol_payload(vol),
        "strategy": _strategy_payload(decision),
    }

    signal_row = {
        "signal_id": ctx.signal_id,
        "session_id": ctx.session_id,
        "occurred_at_utc": ctx.occurred_at_utc,
        "regime": signal_contract["regime"],
        "vol_state": vol.iv_hv_state,
        "strategy_kind": signal_contract["strategy"],
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
        "to_status": signal_contract["strategy"],
        "payload": {
            "reason": signal_contract["reason"],
            "risk_status": signal_contract["initial_risk_status"],
            "rule_version": signal_contract["rule_version"],
        },
    }

    return signal_row, audit_row


__all__ = ["SignalContext", "build_signal_contract", "build_signal_rows"]
