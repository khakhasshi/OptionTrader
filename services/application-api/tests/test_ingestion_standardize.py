"""Offline tests for the THETADATA -> replay standardizer (P1-1)."""

from __future__ import annotations

import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from app.ingestion import standardize_bars, standardize_parquet
from app.ingestion.standardize import STANDARD_COLUMNS

SRC_TZ = ZoneInfo("Etc/GMT+4")  # THETADATA's fixed-offset (mirrors the export)


def _bars(times: list[str], n: int | None = None) -> pd.DataFrame:
    ts = pd.to_datetime(pd.Series(times)).dt.tz_localize(SRC_TZ)
    k = n if n is not None else len(times)
    return pd.DataFrame(
        {
            "timestamp": ts,
            "open": [100.0 + i for i in range(k)],
            "high": [101.0 + i for i in range(k)],
            "low": [99.0 + i for i in range(k)],
            "close": [100.5 + i for i in range(k)],
            "volume": [1000 + i for i in range(k)],
            "vwap": [100.2 + i for i in range(k)],
            "count": [10 + i for i in range(k)],
        }
    )


def test_summer_bar_utc_matches_fixed_offset() -> None:
    # July: Eastern is EDT (-04:00), same as the source's fixed offset.
    df = _bars(["2026-07-09 09:30:00"])
    out, _ = standardize_bars(df, symbol="QQQ.US")
    assert out["occurred_at_utc"].iloc[0] == pd.Timestamp("2026-07-09 13:30:00", tz="UTC")


def test_winter_bar_corrects_dst_offset() -> None:
    # January: Eastern is EST (-05:00). The source's fixed -04:00 is wrong;
    # standardization must yield 14:30Z, not 13:30Z.
    df = _bars(["2026-01-09 09:30:00"])
    out, _ = standardize_bars(df, symbol="QQQ.US")
    assert out["occurred_at_utc"].iloc[0] == pd.Timestamp("2026-01-09 14:30:00", tz="UTC")
    assert out["trading_date"].iloc[0] == "2026-01-09"


def test_columns_and_dedup_and_sort() -> None:
    df = _bars(["2026-07-09 09:31:00", "2026-07-09 09:30:00", "2026-07-09 09:31:00"])
    out, _ = standardize_bars(df, symbol="QQQ.US")
    assert list(out.columns) == STANDARD_COLUMNS
    assert len(out) == 2  # duplicate 09:31 collapsed
    assert out["occurred_at_utc"].is_monotonic_increasing


def test_missing_column_rejected() -> None:
    df = _bars(["2026-07-09 09:30:00"]).drop(columns=["close"])
    with pytest.raises(ValueError, match="required columns"):
        standardize_bars(df, symbol="QQQ.US")


def test_naive_timestamp_rejected() -> None:
    df = _bars(["2026-07-09 09:30:00"])
    df["timestamp"] = df["timestamp"].dt.tz_localize(None)
    with pytest.raises(ValueError, match="timezone-aware"):
        standardize_bars(df, symbol="QQQ.US")


def test_partitions_and_manifest_written(tmp_path: Path) -> None:
    df = _bars(["2026-07-08 09:30:00", "2026-07-08 09:31:00", "2026-07-09 09:30:00"])
    src = tmp_path / "raw.parquet"
    df.to_parquet(src, index=False)

    res = standardize_parquet(src, tmp_path / "replay", symbol="QQQ.US", data_type="equity_1m")
    assert res.manifest.import_status == "COMPLETE"
    assert res.manifest.coverage_start == "2026-07-08"
    assert res.manifest.coverage_end == "2026-07-09"
    assert res.manifest.rows == 3
    assert len(res.manifest.partitions) == 2

    doc = json.loads(res.manifest_path.read_text())
    assert doc["symbol"] == "QQQ.US"
    assert doc["provider"] == "thetadata"
    assert doc["content_checksum"]
    for p in doc["partitions"]:
        assert (res.dataset_root / p["relative_path"]).exists()


def test_checksum_reproducible(tmp_path: Path) -> None:
    df = _bars(["2026-07-08 09:30:00", "2026-07-09 09:30:00"])
    src = tmp_path / "raw.parquet"
    df.to_parquet(src, index=False)

    a = standardize_parquet(src, tmp_path / "a", symbol="QQQ.US")
    b = standardize_parquet(src, tmp_path / "b", symbol="QQQ.US")
    assert a.manifest.content_checksum() == b.manifest.content_checksum()
