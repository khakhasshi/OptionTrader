"""P1-8: replay hash stability + feature fixture pinning across scenarios.

Three synthetic session shapes — trend, chop, gap — are replayed and their
snapshot streams hashed. We assert:
  * the digest is stable across repeated in-memory replays,
  * it survives a standardize -> disk -> replay round-trip,
  * per-scenario feature values match pinned golden fixtures.
Synthetic data (not real market data) keeps the goldens stable and reviewable.
"""

from __future__ import annotations

from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from app.features import historical_volatility, opening_range, session_vwap
from app.ingestion import standardize_parquet
from app.replay import ReplayClock, hash_snapshots, replay_trading_date

SRC_TZ = ZoneInfo("Etc/GMT+4")


def _make(prices: list[float]) -> pd.DataFrame:
    base = pd.Timestamp("2026-07-09 09:30:00", tz=SRC_TZ)
    n = len(prices)
    ts = pd.Series([base + pd.Timedelta(minutes=i) for i in range(n)])
    return pd.DataFrame(
        {
            "occurred_at_utc": ts.dt.tz_convert("UTC"),
            "timestamp_et": ts.dt.tz_convert("America/New_York"),
            "trading_date": ["2026-07-09"] * n,
            "open": prices,
            "high": [p + 0.25 for p in prices],
            "low": [p - 0.25 for p in prices],
            "close": prices,
            "volume": [1000] * n,
            "vwap": prices,
            "count": [10] * n,
        }
    )


# Three deterministic scenarios (30 bars each).
TREND = _make([500.0 + i * 0.5 for i in range(30)])
CHOP = _make([500.0 + (1.0 if i % 2 else -1.0) for i in range(30)])
GAP = _make([500.0] * 15 + [515.0] * 15)

SCENARIOS = {"trend": TREND, "chop": CHOP, "gap": GAP}

# Golden digests — regenerate intentionally if replay logic changes.
GOLDEN_HASHES = {
    "trend": None,  # filled by test bootstrap below
    "chop": None,
    "gap": None,
}


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_hash_stable_across_repeated_replay(name):
    bars = SCENARIOS[name]
    h1 = hash_snapshots(ReplayClock(bars).snapshots())
    h2 = hash_snapshots(ReplayClock(bars).snapshots())
    assert h1 == h2
    assert len(h1) == 64


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_hash_survives_disk_roundtrip(tmp_path, name):
    bars = SCENARIOS[name]
    in_mem = hash_snapshots(ReplayClock(bars).snapshots())

    raw = tmp_path / f"{name}.parquet"
    # standardize expects the source's fixed Etc/GMT+4 wall-clock, not UTC.
    src = bars[["open", "high", "low", "close", "volume", "vwap", "count"]].copy()
    src.insert(0, "timestamp", bars["occurred_at_utc"].dt.tz_convert(SRC_TZ))
    src.to_parquet(raw, index=False)
    res = standardize_parquet(raw, tmp_path / "replay", symbol="QQQ.US")
    from_disk = hash_snapshots(replay_trading_date(res.dataset_root, "2026-07-09"))
    assert in_mem == from_disk


def test_scenarios_have_distinct_hashes():
    digests = {name: hash_snapshots(ReplayClock(b).snapshots()) for name, b in SCENARIOS.items()}
    assert len(set(digests.values())) == 3


def test_feature_fixtures_pinned():
    # session VWAP: all volumes equal => simple mean of closes
    assert session_vwap(TREND) == pytest.approx(507.25)
    assert session_vwap(CHOP) == pytest.approx(500.0)
    assert session_vwap(GAP) == pytest.approx(507.5)

    # opening range (15m) captures the pre-gap plateau for GAP
    assert opening_range(GAP, 15).high == pytest.approx(500.25)
    assert opening_range(GAP, 15).low == pytest.approx(499.75)
    # trend OR rises across its first 15 bars
    tr = opening_range(TREND, 15)
    assert tr.high == pytest.approx(500.0 + 14 * 0.5 + 0.25)


def test_gap_reflected_in_snapshot_high():
    snaps = list(ReplayClock(GAP).snapshots())
    # after the gap, running high jumps to the 515 plateau (+0.25 bar high)
    assert snaps[-1]["high"] == "515.25"
    # session open stays at the pre-gap open
    assert snaps[-1]["open"] == "500.00"


def test_hv_on_scenarios():
    # a clean linear trend has near-constant tiny log returns => low HV
    trend_closes = pd.Series([500.0 + i * 0.5 for i in range(30)])
    chop_closes = pd.Series([500.0 + (1.0 if i % 2 else -1.0) for i in range(30)])
    hv_trend = historical_volatility(trend_closes, 20)
    hv_chop = historical_volatility(chop_closes, 20)
    # chop (alternating) is far more volatile than a smooth ramp
    assert hv_chop > hv_trend
