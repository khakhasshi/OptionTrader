"""CockpitProjector: MarketTick stream -> schema-valid CockpitState frames."""

from __future__ import annotations

import glob
import json
import os
from typing import Any

from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from app.realtime.projector import CockpitProjector, ProjectorConfig

_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
_SCHEMA_DIR = os.path.join(_ROOT, "packages", "contracts", "jsonschema")


def _validator() -> Draft202012Validator:
    resources = {
        os.path.basename(f): Resource.from_contents(json.load(open(f)))
        for f in glob.glob(os.path.join(_SCHEMA_DIR, "*.json"))
    }
    registry = Registry().with_resources(list(resources.items()))
    return Draft202012Validator(resources["cockpit_state.json"].contents, registry=registry)


def _bar(minute_et: int, close: float, volume: int) -> dict[str, Any]:
    hh, mm = minute_et // 60, minute_et % 60
    uhh = hh + 4  # EDT: ET + 4 = UTC
    return {
        "occurred_at_utc": f"2026-07-20T{uhh:02d}:{mm:02d}:00Z",
        "timestamp_et": f"2026-07-20T{hh:02d}:{mm:02d}:00-04:00",
        "minute_et": minute_et,
        "open": f"{close:.2f}",
        "high": f"{close + 0.5:.2f}",
        "low": f"{close - 0.5:.2f}",
        "close": f"{close:.2f}",
        "volume": volume,
        "vwap": f"{close:.2f}",
    }


def _snapshot(minute_et: int, close: float, seq: int, health: str = "HEALTHY") -> dict[str, Any]:
    hh, mm = minute_et // 60, minute_et % 60
    uhh = hh + 4
    return {
        "schema_version": "1.0",
        "snapshot_id": f"mkt_{minute_et}_{seq:06d}",
        "occurred_at_utc": f"2026-07-20T{uhh:02d}:{mm:02d}:00Z",
        "timestamp_et": f"2026-07-20T{hh:02d}:{mm:02d}:00-04:00",
        "symbol": "QQQ.US",
        "price": f"{close:.2f}",
        "open": "498.50",
        "high": f"{close + 1:.2f}",
        "low": "497.90",
        "previous_close": "497.20",
        "vwap": f"{close:.2f}",
        "volume": 1_000_000,
        "premarket_high": None,
        "premarket_low": None,
        "sequence_number": seq,
        "quote_age_ms": 0,
        "data_health": health,
    }


def _tick(minute_et: int, close: float, seq: int, health: str = "HEALTHY") -> dict[str, Any]:
    return {
        "snapshot": _snapshot(minute_et, close, seq, health),
        "bar": _bar(minute_et, close, 1000 + seq),
    }


def _config() -> ProjectorConfig:
    return ProjectorConfig(session_id="sess_test", rule_version="test-1", opening_range_minutes=3)


def test_frames_are_schema_valid_and_seq_monotonic() -> None:
    proj = CockpitProjector(config=_config())
    validator = _validator()
    frames = [proj.apply(_tick(570 + i, 500.0 + i * 0.1, i + 1)) for i in range(6)]
    for frame in frames:
        assert list(validator.iter_errors(frame)) == []
    assert [f["seq"] for f in frames] == [0, 1, 2, 3, 4, 5]


def test_healthy_tick_produces_signal_and_derivations() -> None:
    proj = CockpitProjector(config=_config())
    for i in range(4):
        frame = proj.apply(_tick(570 + i, 500.0 + i * 0.1, i + 1))
    assert frame["connection"] == "LIVE"
    assert frame["snapshot"] is not None
    assert frame["regime"] is not None
    assert frame["vol"] is not None
    assert frame["signal"] is not None
    # Signal is contract-shaped (enum, not engine label).
    assert frame["signal"]["regime"] in {"Trend", "Range", "Event", "Chaos", "NoTrade"}
    assert frame["signal"]["strategy"] in {"LongGamma", "ShortPremium", "EventVolCrush", "NoTrade"}


def test_stale_health_blocks_new_positions() -> None:
    proj = CockpitProjector(config=_config())
    frame = proj.apply(_tick(570, 500.0, 1, health="STALE"))
    assert frame["connection"] == "STALE"
    assert frame["new_position_allowed"] is False
    assert any("data_health=STALE" in flag for flag in frame["risk_flags"])


def test_disconnected_health_blocks_and_reports() -> None:
    proj = CockpitProjector(config=_config())
    frame = proj.apply(_tick(570, 500.0, 1, health="DISCONNECTED"))
    assert frame["connection"] == "DISCONNECTED"
    assert frame["new_position_allowed"] is False


def test_degraded_is_live_but_not_tradable() -> None:
    proj = CockpitProjector(config=_config())
    frame = proj.apply(_tick(570, 500.0, 1, health="DEGRADED"))
    assert frame["connection"] == "LIVE"
    assert frame["new_position_allowed"] is False


def test_missing_snapshot_or_bar_fails_closed() -> None:
    proj = CockpitProjector(config=_config())
    frame = proj.apply({"snapshot": None, "bar": None})
    assert frame["new_position_allowed"] is False
    assert frame["connection"] == "DISCONNECTED"
    assert frame["snapshot"] is None
    # The fail-closed frame must itself be schema-valid, incl. Z-suffixed time.
    assert list(_validator().iter_errors(frame)) == []
    assert str(frame["server_time_utc"]).endswith("Z")


def test_bad_bar_values_fail_closed_with_reason() -> None:
    proj = CockpitProjector(config=_config())
    bad = _tick(570, 500.0, 1)
    bad["bar"]["close"] = "not-a-number"
    frame = proj.apply(bad)
    assert frame["new_position_allowed"] is False
    assert any("projection error" in flag for flag in frame["risk_flags"])


def test_disconnected_frame_is_schema_valid() -> None:
    proj = CockpitProjector(config=_config())
    frame = proj.disconnected_frame("2026-07-20T13:45:00Z", "stream ended")
    assert list(_validator().iter_errors(frame)) == []
    assert frame["new_position_allowed"] is False


def test_sequence_gap_blocks_until_backfilled() -> None:
    """Review P0: seq 1 -> (disconnect) -> seq 4 must NOT unlock trading; only
    after 2 and 3 backfill (contiguous) does HEALTHY 4 become tradable."""
    proj = CockpitProjector(config=_config())
    validator = _validator()

    f1 = proj.apply(_tick(570, 500.0, 1))
    assert f1["connection"] == "LIVE"
    assert f1["new_position_allowed"] is True

    # A HEALTHY snapshot at seq 4 while 2,3 are missing -> reconciling, blocked.
    f4 = proj.apply(_tick(573, 500.3, 4))
    assert f4["new_position_allowed"] is False
    assert f4["connection"] == "STALE"
    assert any("sequence discontinuity" in flag for flag in f4["risk_flags"])
    assert list(validator.iter_errors(f4)) == []

    # Backfill 2 and 3 (contiguous): still blocked at 2, blocked at 3...
    f2 = proj.apply(_tick(571, 500.1, 2))
    assert f2["new_position_allowed"] is True and f2["connection"] == "LIVE"
    f3 = proj.apply(_tick(572, 500.2, 3))
    assert f3["new_position_allowed"] is True

    # ...now the previously-skipped 4 arrives again in order -> tradable.
    f4b = proj.apply(_tick(573, 500.3, 4))
    assert f4b["connection"] == "LIVE"
    assert f4b["new_position_allowed"] is True


def test_first_record_after_restart_midsession_blocks() -> None:
    """A fresh projector whose first record is seq > 1 (app restarted
    mid-session) must block until backfilled to session open."""
    proj = CockpitProjector(config=_config())
    frame = proj.apply(_tick(575, 500.5, 6))  # first-ever record is seq 6
    assert frame["new_position_allowed"] is False
    assert frame["connection"] == "STALE"
    assert any("expected 1" in flag for flag in frame["risk_flags"])
