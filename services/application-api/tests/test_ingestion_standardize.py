"""Offline tests for the THETADATA -> replay standardizer (P1-1)."""

from __future__ import annotations

import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from app.ingestion import (
    standardize_bars,
    standardize_option_parquet,
    standardize_option_quotes,
    standardize_parquet,
)
from app.ingestion.standardize import OPTION_QUOTE_COLUMNS, STANDARD_COLUMNS

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
    df.loc[2, ["open", "high", "low", "close", "volume", "vwap", "count"]] = df.loc[
        0, ["open", "high", "low", "close", "volume", "vwap", "count"]
    ]
    out, _ = standardize_bars(df, symbol="QQQ.US")
    assert list(out.columns) == STANDARD_COLUMNS
    assert len(out) == 2  # duplicate 09:31 collapsed
    assert out["occurred_at_utc"].is_monotonic_increasing


def test_conflicting_duplicate_rejected() -> None:
    df = _bars(["2026-07-09 09:30:00", "2026-07-09 09:30:00"])
    with pytest.raises(ValueError, match="conflicting duplicate"):
        standardize_bars(df, symbol="QQQ.US")


def test_invalid_market_values_rejected() -> None:
    bad_ohlc = _bars(["2026-07-09 09:30:00"])
    bad_ohlc.loc[0, "high"] = 98.0
    with pytest.raises(ValueError, match="OHLC"):
        standardize_bars(bad_ohlc, symbol="QQQ.US")

    negative_volume = _bars(["2026-07-09 09:30:00"])
    negative_volume.loc[0, "volume"] = -1
    with pytest.raises(ValueError, match="non-negative"):
        standardize_bars(negative_volume, symbol="QQQ.US")


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


def _option_quotes() -> pd.DataFrame:
    timestamp = pd.Series(
        [
            pd.Timestamp("2026-07-09 10:00:00", tz=SRC_TZ),
            pd.Timestamp("2026-07-09 10:00:00", tz=SRC_TZ),
        ]
    )
    return pd.DataFrame(
        {
            "timestamp": timestamp,
            "expiration": ["2026-07-09", "2026-07-09"],
            "strike": [500.0, 500.0],
            "right": ["CALL", "PUT"],
            "bid": [2.0, 3.0],
            "ask": [2.2, 3.2],
            "bid_size": [10, 12],
            "ask_size": [11, 13],
        }
    )


def test_option_quotes_standardize_identity_and_partition(tmp_path: Path) -> None:
    quotes, manifest = standardize_option_quotes(_option_quotes())
    assert list(quotes.columns) == OPTION_QUOTE_COLUMNS
    assert quotes["underlying"].unique().tolist() == ["QQQ"]
    assert set(quotes["option_type"]) == {"C", "P"}
    assert manifest.data_type == "option_quote_1m"

    source = tmp_path / "option-quotes.parquet"
    _option_quotes().to_parquet(source, index=False)
    result = standardize_option_parquet(source, tmp_path / "replay")
    assert result.manifest.import_status == "COMPLETE"
    assert result.manifest.rows == 2
    assert (result.dataset_root / "2026-07-09" / "part-000.parquet").exists()


def test_option_quotes_reject_crossed_and_conflicting_duplicates() -> None:
    crossed = _option_quotes()
    crossed.loc[0, "ask"] = 1.0
    with pytest.raises(ValueError, match="crossed"):
        standardize_option_quotes(crossed)

    duplicate = pd.concat(
        [_option_quotes().iloc[[0]], _option_quotes().iloc[[0]]], ignore_index=True
    )
    duplicate.loc[1, "bid"] = 1.5
    with pytest.raises(ValueError, match="conflicting duplicate"):
        standardize_option_quotes(duplicate)
