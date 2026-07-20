"""Tests for the deterministic replay clock (P1-3), incl. contract validation."""

from __future__ import annotations

import glob
import json
import os
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from app.ingestion import standardize_parquet
from app.replay import ReplayClock, ReplayConfig, replay_trading_date

SRC_TZ = ZoneInfo("Etc/GMT+4")
# tests/ -> application-api -> services -> repo root
_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
_SCHEMA_DIR = os.path.join(_ROOT, "packages", "contracts", "jsonschema")


def _snapshot_validator() -> Draft202012Validator:
    res = {
        os.path.basename(f): Resource.from_contents(json.load(open(f)))
        for f in glob.glob(os.path.join(_SCHEMA_DIR, "*.json"))
    }
    reg = Registry().with_resources(list(res.items()))
    return Draft202012Validator(res["market_snapshot.json"].contents, registry=reg)


def _bars(times: list[str]) -> pd.DataFrame:
    ts = pd.to_datetime(pd.Series(times)).dt.tz_localize(SRC_TZ)
    n = len(times)
    return pd.DataFrame(
        {
            "occurred_at_utc": ts.dt.tz_convert("UTC"),
            "timestamp_et": ts.dt.tz_convert("America/New_York"),
            "trading_date": ["2026-07-09"] * n,
            "open": [500.0 + i for i in range(n)],
            "high": [501.0 + i for i in range(n)],
            "low": [499.0 + i for i in range(n)],
            "close": [500.5 + i for i in range(n)],
            "volume": [1000 + i for i in range(n)],
            "vwap": [500.2 + i for i in range(n)],
            "count": [10 + i for i in range(n)],
        }
    )


def _session(minutes: int) -> pd.DataFrame:
    base = pd.Timestamp("2026-07-09 09:30:00")
    times = [(base + pd.Timedelta(minutes=m)).strftime("%Y-%m-%d %H:%M:%S") for m in range(minutes)]
    return _bars(times)


def test_every_snapshot_satisfies_contract():
    v = _snapshot_validator()
    snaps = list(ReplayClock(_session(30)).snapshots())
    assert len(snaps) == 30
    for s in snaps:
        assert list(v.iter_errors(s)) == [], s


def test_sequence_is_monotonic_from_zero():
    snaps = list(ReplayClock(_session(20)).snapshots())
    assert [s["sequence_number"] for s in snaps] == list(range(20))


def test_open_is_session_open_and_high_low_run():
    snaps = list(ReplayClock(_session(10)).snapshots())
    assert snaps[0]["open"] == "500.00"
    assert all(s["open"] == "500.00" for s in snaps)
    # highs increase across the synthetic session; running high tracks the max
    assert snaps[-1]["high"] == "510.00"
    assert snaps[-1]["low"] == "499.00"


def test_opening_range_freezes_after_window():
    cfg = ReplayConfig(opening_range_minutes=5)
    snaps = list(ReplayClock(_session(20), cfg).snapshots())
    # OR high captured only from the first 5 bars (highs 501..505)
    assert snaps[-1]["opening_range_high"] == "505.00"
    assert snaps[-1]["opening_range_low"] == "499.00"


def test_replay_is_deterministic():
    a = list(ReplayClock(_session(25)).snapshots())
    b = list(ReplayClock(_session(25)).snapshots())
    assert json.dumps(a) == json.dumps(b)


def test_empty_bars_rejected():
    with pytest.raises(ValueError, match="no bars"):
        ReplayClock(_session(0))


def test_replay_from_standardized_partition(tmp_path):
    # end-to-end: standardize -> replay one date off disk
    raw = tmp_path / "raw.parquet"
    src = _session(5)[
        ["occurred_at_utc", "open", "high", "low", "close", "volume", "vwap", "count"]
    ].rename(columns={"occurred_at_utc": "timestamp"})
    src.to_parquet(raw, index=False)
    res = standardize_parquet(raw, tmp_path / "replay", symbol="QQQ.US")
    snaps = replay_trading_date(res.dataset_root, "2026-07-09")
    assert len(snaps) == 5
    assert snaps[0]["symbol"] == "QQQ.US"
