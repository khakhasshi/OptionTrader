"""Pure projection: MarketTick stream -> CockpitState frames.

The projector accumulates per-minute bars into a session DataFrame and, on each
tick, runs the Phase 1 engines (Regime -> Vol -> Strategy -> Signal) exactly as
offline replay does, then assembles a ``cockpit_state.json`` frame. It holds no
I/O and no wall clock: the same tick sequence always yields the same frames, so
it is fully unit-testable and replay-reproducible.

Fail closed: a bad tick or an engine error produces a No-Trade frame
(``new_position_allowed=false``) with the reason in ``risk_flags`` — never a
fabricated tradable state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time, timezone
from typing import Any

import pandas as pd

from app.persistence.serialize import SignalContext, build_signal_contract
from app.regime import RegimeInputs, RegimeState
from app.regime import evaluate as regime_evaluate
from app.strategy import StrategyDecision, StrategyInputs
from app.strategy import decide as strategy_decide
from app.vol import VolInputs, VolState
from app.vol import evaluate as vol_evaluate

# DataHealth values that permit the stream to be considered LIVE (flowing).
_LIVE_HEALTH = frozenset({"HEALTHY", "DEGRADED", "RECONCILING"})


@dataclass(frozen=True)
class ProjectorConfig:
    """Session identity and replay knobs for one cockpit stream."""

    session_id: str
    rule_version: str
    opening_range_minutes: int = 15


@dataclass
class CockpitProjector:
    """Stateful per-session projector. Feed it ticks in order via ``apply``."""

    config: ProjectorConfig
    _bars: list[dict[str, Any]] = field(default_factory=list)
    _seq: int = 0
    # Last CONTIGUOUSLY-consumed MarketSnapshot.sequence_number (0 = none yet).
    # The next accepted record must be exactly this + 1; anything else is a gap,
    # reorder, or duplicate and forces RECONCILING (fail closed) until backfill
    # restores continuity.
    _last_market_seq: int = 0

    def _connection(self, data_health: str) -> str:
        if data_health == "STALE":
            return "STALE"
        if data_health == "DISCONNECTED":
            return "DISCONNECTED"
        return "LIVE"

    def _next_seq(self) -> int:
        seq = self._seq
        self._seq += 1
        return seq

    def _base_frame(self, server_time_utc: str) -> dict[str, Any]:
        return {
            "schema_version": "1.0",
            "seq": self._next_seq(),
            "session_id": self.config.session_id,
            "server_time_utc": server_time_utc,
            "connection": "DISCONNECTED",
            "new_position_allowed": False,
            "snapshot": None,
            "regime": None,
            "vol": None,
            "signal": None,
            "risk_flags": [],
        }

    def disconnected_frame(self, server_time_utc: str, reason: str) -> dict[str, Any]:
        """A terminal fail-closed frame: stream lost, nothing tradable."""
        frame = self._base_frame(server_time_utc)
        frame["risk_flags"] = [reason]
        return frame

    def apply(self, tick: dict[str, Any]) -> dict[str, Any]:
        """Project one tick (``{"snapshot": {...}, "bar": {...}}``) to a frame.

        The snapshot is the Rust-authoritative aggregate (drives data_health and
        display); the bar is the raw per-minute OHLCV appended to the session
        frame the engines consume.
        """
        snapshot = tick.get("snapshot")
        bar = tick.get("bar")
        server_time = (snapshot or {}).get("occurred_at_utc") or _utc_now_iso()

        if not snapshot or not bar:
            return self.disconnected_frame(server_time, "tick missing snapshot or bar")

        data_health = str(snapshot.get("data_health", "DISCONNECTED"))
        connection = self._connection(data_health)

        # Sequence-continuity guard (fail closed on gap/reorder/dup). Even a
        # HEALTHY snapshot must NOT unlock trading if records were skipped — the
        # engines would run on incomplete bar history. The next accepted record
        # must be exactly _last_market_seq + 1; a fresh projector expects seq 1,
        # so a first record with seq > 1 (e.g. after an app restart mid-session)
        # also blocks until backfilled to session open.
        market_seq = snapshot.get("sequence_number")
        expected = self._last_market_seq + 1
        if not isinstance(market_seq, int) or market_seq != expected:
            frame = self._base_frame(server_time)
            frame["connection"] = "STALE"  # data exists but is not trustworthy
            frame["snapshot"] = snapshot
            frame["risk_flags"] = [
                f"sequence discontinuity: expected {expected}, got {market_seq}; "
                "reconciling — new positions blocked until missing records backfill"
            ]
            # Do NOT advance _last_market_seq and do NOT append the bar: a later
            # backfilled record carrying `expected` will pass this guard and the
            # session history stays gap-free.
            return frame

        try:
            self._append_bar(bar)
            frame_bars = self._frame()
            regime = regime_evaluate(
                frame_bars,
                opening_range_minutes=self.config.opening_range_minutes,
                inputs=RegimeInputs(),
            )
            vol = vol_evaluate(frame_bars, inputs=VolInputs())
            now_et = _parse_et_time(bar["timestamp_et"])
            decision = strategy_decide(
                regime,
                vol,
                StrategyInputs(now_et=now_et, data_healthy=data_health == "HEALTHY"),
            )
            signal = self._signal(snapshot, regime, decision)
        except (ValueError, KeyError) as exc:
            # Engine/data fault: fail closed, keep the raw snapshot for context.
            frame = self._base_frame(server_time)
            frame["connection"] = connection
            frame["snapshot"] = snapshot
            frame["risk_flags"] = [f"projection error: {exc}"]
            return frame

        # Record accepted and contiguous: advance the consumed-sequence marker.
        self._last_market_seq = market_seq

        allowed = data_health == "HEALTHY" and connection == "LIVE"
        risk_flags = list(decision.risk_notes)
        if not allowed:
            risk_flags.append(f"new positions blocked: data_health={data_health}")

        frame = self._base_frame(server_time)
        frame["connection"] = connection
        frame["new_position_allowed"] = allowed
        frame["snapshot"] = snapshot
        frame["regime"] = _regime_dict(regime)
        frame["vol"] = _vol_dict(vol)
        frame["signal"] = signal
        frame["risk_flags"] = risk_flags
        return frame

    def _append_bar(self, bar: dict[str, Any]) -> None:
        self._bars.append(
            {
                "occurred_at_utc": pd.Timestamp(bar["occurred_at_utc"]).tz_convert("UTC"),
                "timestamp_et": bar["timestamp_et"],
                "open": float(bar["open"]),
                "high": float(bar["high"]),
                "low": float(bar["low"]),
                "close": float(bar["close"]),
                "volume": int(bar["volume"]),
                "vwap": float(bar["vwap"]) if bar.get("vwap") else None,
            }
        )

    def _frame(self) -> pd.DataFrame:
        return pd.DataFrame(self._bars)

    def _signal(
        self, snapshot: dict[str, Any], regime: RegimeState, decision: StrategyDecision
    ) -> dict[str, Any]:
        occurred = pd.Timestamp(snapshot["occurred_at_utc"]).tz_convert("UTC").to_pydatetime()
        ctx = SignalContext(
            signal_id=f"sig_{snapshot['snapshot_id']}",
            session_id=self.config.session_id,
            occurred_at_utc=occurred,
            rule_version=self.config.rule_version,
        )
        return build_signal_contract(ctx, regime, decision)


def _utc_now_iso() -> str:
    """UTC RFC3339 ending in Z (contract utcTimestamp), never a local offset."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_et_time(timestamp_et: str) -> time:
    result: time = pd.Timestamp(timestamp_et).time()
    return result


def _regime_dict(regime: RegimeState) -> dict[str, Any]:
    return {
        "regime": _REGIME_CONTRACT.get(regime.regime, regime.regime),
        "trend_score": regime.trend_score,
        "range_score": regime.range_score,
        "components": dict(regime.components),
        "unavailable": list(regime.unavailable),
    }


def _vol_dict(vol: VolState) -> dict[str, Any]:
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


# Engine label -> cockpit_state.json regime enum (mirrors serialize._REGIME_CONTRACT).
_REGIME_CONTRACT = {
    "Trend": "Trend",
    "Range": "Range",
    "Event": "Event",
    "Chaos": "Chaos",
    "No Trade": "NoTrade",
}
