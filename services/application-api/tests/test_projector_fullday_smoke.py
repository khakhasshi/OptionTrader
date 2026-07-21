"""Committed PROJECTOR full-day smoke (Phase 2 acceptance, projector layer only).

Scope, stated honestly: this exercises the pure CockpitProjector over a full
regular session (390 one-minute frames, 09:30–15:59 ET). It is NOT a transport
or cross-language smoke — for that see tests/test_integration_smoke.py, which
runs the real Rust producer -> gRPC -> SessionHub -> CockpitState path.

Asserts, at the projector layer:
* every emitted CockpitState validates against cockpit_state.json;
* a REAL data gap (non-contiguous minute_et, not just a health flag) and a STALE
  window both force new_position_allowed=False for their whole span;
* health stays non-tradable across the degraded span (not just one frame);
* seq is exactly range(N) — no gaps, no duplicates;
* the run completes with no unhandled exception.

Cross-language properties from the review checklist live in their own tests:
* producer survives subscriber cancel; 2nd subscriber doesn't pollute health;
  snapshot health == health_states -> trading-core-bin grpc.rs unit tests;
* WS reconnect seq continuity, multi-client single projection, upstream
  reconnect after end -> tests/test_realtime_session.py;
* REST recovery vs newer WS frame arbitration -> apps/web Cockpit.test.tsx;
* end-to-end transport (Rust->gRPC->hub->CockpitState) -> test_integration_smoke.py.
"""

from __future__ import annotations

import glob
import json
import os
from datetime import datetime
from typing import Any

from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from app.realtime.projector import CockpitProjector, ProjectorConfig

_SCHEMA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
    "packages",
    "contracts",
    "jsonschema",
)

_OPEN_MINUTE_ET = 9 * 60 + 30  # 570
_SESSION_MINUTES = 390  # 09:30..15:59 inclusive
# Inject a degraded STALE span in the middle of the day.
_STALE_START = 200
_STALE_END = 205
# Inject a REAL data gap: skip this many minutes at the gap point (so minute_et
# is genuinely non-contiguous, not just a health flag). The frames around it are
# marked STALE, as the Rust DataHealthMachine would on a gap.
_GAP_AT = 260
_GAP_MINUTES = 3


def _validator() -> Draft202012Validator:
    res = {
        os.path.basename(f): Resource.from_contents(json.load(open(f)))
        for f in glob.glob(os.path.join(_SCHEMA_DIR, "*.json"))
    }
    reg = Registry().with_resources(list(res.items()))
    return Draft202012Validator(res["cockpit_state.json"].contents, registry=reg)


def _tick(i: int, close: float, health: str, minute_offset: int = 0) -> dict[str, Any]:
    # minute_offset advances the wall-clock minute WITHOUT advancing the frame
    # index i, so a gap produces genuinely non-contiguous minute_et / timestamps.
    minute_et = _OPEN_MINUTE_ET + i + minute_offset
    hh, mm = minute_et // 60, minute_et % 60
    uhh = hh + 4  # EDT: ET + 4 = UTC
    ts_utc = f"2026-07-09T{uhh:02d}:{mm:02d}:00Z"
    ts_et = f"2026-07-09T{hh:02d}:{mm:02d}:00-04:00"
    snap = {
        "schema_version": "1.0",
        "snapshot_id": f"mkt_{minute_et}_{i + 1:06d}",
        "occurred_at_utc": ts_utc,
        "timestamp_et": ts_et,
        "symbol": "QQQ.US",
        "price": f"{close:.2f}",
        "open": "500.00",
        "high": f"{close + 1:.2f}",
        "low": "498.00",
        "vwap": f"{close:.2f}",
        "volume": 1_000_000,
        "sequence_number": i + 1,
        "quote_age_ms": 0,
        "data_health": health,
    }
    bar = {
        "occurred_at_utc": ts_utc,
        "timestamp_et": ts_et,
        "minute_et": minute_et,
        "open": f"{close:.2f}",
        "high": f"{close + 0.5:.2f}",
        "low": f"{close - 0.5:.2f}",
        "close": f"{close:.2f}",
        "volume": 1000 + i,
        "vwap": f"{close:.2f}",
    }
    return {"snapshot": snap, "bar": bar}


def test_fullday_replay_is_schema_valid_and_fail_closed() -> None:
    validator = _validator()
    proj = CockpitProjector(
        config=ProjectorConfig(session_id="sess_20260709", rule_version="smoke-1")
    )
    close = 500.0
    invalid = 0
    stale_frames_tradable = 0
    gap_frames_tradable = 0
    healthy_tradable = 0
    gap_minute_delta: float | None = None
    prev_dt: datetime | None = None

    for i in range(_SESSION_MINUTES):
        close += 0.1 if i % 3 else -0.15
        # A real gap: after _GAP_AT, wall-clock minutes jump by _GAP_MINUTES while
        # the frame index keeps stepping by 1 -> non-contiguous minute_et. The
        # gap window is marked STALE, as the Rust DataHealthMachine would report.
        in_stale = _STALE_START <= i < _STALE_END
        in_gap = _GAP_AT <= i < _GAP_AT + _GAP_MINUTES
        offset = _GAP_MINUTES if i >= _GAP_AT else 0
        health = "STALE" if (in_stale or in_gap) else "HEALTHY"
        frame = proj.apply(_tick(i, close, health, minute_offset=offset))

        ts = frame["snapshot"]["occurred_at_utc"] if frame["snapshot"] else None
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00")) if ts else None
        if dt is not None and prev_dt is not None and i == _GAP_AT:
            # Measure the ACTUAL minute jump at the gap boundary; a broken gap
            # injection would show 1 min here and fail the assertion below.
            gap_minute_delta = (dt - prev_dt).total_seconds() / 60.0
        prev_dt = dt

        if list(validator.iter_errors(frame)):
            invalid += 1
        if in_stale and frame["new_position_allowed"]:
            stale_frames_tradable += 1
        if in_gap and frame["new_position_allowed"]:
            gap_frames_tradable += 1
        if not in_stale and not in_gap and frame["new_position_allowed"]:
            healthy_tradable += 1

    assert invalid == 0, f"{invalid} frames failed cockpit_state.json validation"
    assert stale_frames_tradable == 0, "new positions must be blocked throughout STALE"
    assert gap_frames_tradable == 0, "new positions must be blocked throughout the gap"
    # The gap must be a REAL time jump: normal step is 1 min, so the boundary
    # delta is (1 + _GAP_MINUTES). Asserting the exact value means a future
    # broken gap injection (contiguous minutes) fails this test instead of
    # passing silently.
    assert gap_minute_delta == float(1 + _GAP_MINUTES), (
        f"expected a {1 + _GAP_MINUTES}-minute jump at the gap, got {gap_minute_delta}"
    )
    assert healthy_tradable > 0


def test_fullday_frame_count_and_seq_is_exact_range() -> None:
    proj = CockpitProjector(config=ProjectorConfig(session_id="sess_seq", rule_version="smoke-1"))
    seqs = []
    close = 500.0
    for i in range(_SESSION_MINUTES):
        close += 0.05
        frame = proj.apply(_tick(i, close, "HEALTHY"))
        seqs.append(frame["seq"])
    # Exact range() rules out gaps AND duplicates (sorted() alone would not).
    assert seqs == list(range(_SESSION_MINUTES))
