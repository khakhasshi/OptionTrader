"""Standardize THETADATA 1-minute bars into the replay Parquet layout.

Input: a cleaned THETADATA export (Parquet or DataFrame) with columns
``timestamp, open, high, low, close, volume`` (``count``/``vwap`` optional),
where ``timestamp`` carries the source's fixed ``Etc/GMT+4`` offset.

Output: one Parquet file per Eastern trading date under
``<root>/<provider>/<data_type>/<symbol>/<trading_date>/part-000.parquet``
plus a ``_manifest.json`` describing the dataset.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from app.ingestion.manifest import DatasetManifest, PartitionEntry, sha256_file

EASTERN = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

# Canonical standardized columns, in order. `occurred_at_utc` is the source of
# truth; `timestamp_et` is display/decision-only (never re-derive UTC from it).
STANDARD_COLUMNS = [
    "occurred_at_utc",
    "timestamp_et",
    "trading_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "count",
]

OPTION_QUOTE_COLUMNS = [
    "occurred_at_utc",
    "timestamp_et",
    "trading_date",
    "underlying",
    "expiry",
    "strike",
    "option_type",
    "bid",
    "ask",
    "bid_size",
    "ask_size",
]

_REQUIRED_INPUT = ("timestamp", "open", "high", "low", "close", "volume")


@dataclass
class StandardizeResult:
    """Outcome of standardizing one source file into a dataset."""

    manifest: DatasetManifest
    dataset_root: Path
    manifest_path: Path


def _correct_utc(ts: pd.Series) -> pd.Series:
    """Re-derive the true UTC instant from Eastern wall-clock.

    The source applies a constant -04:00 offset year-round, which is wrong in
    winter. We discard that offset, treat the clock reading as Eastern local
    time, and localize through ``America/New_York`` so DST is handled.
    """
    naive = ts.dt.tz_localize(None)
    eastern = naive.dt.tz_localize(EASTERN, ambiguous="infer", nonexistent="shift_forward")
    return eastern.dt.tz_convert(UTC)


def standardize_bars(
    df: pd.DataFrame,
    *,
    symbol: str,
    provider: str = "thetadata",
    data_type: str = "equity_1m",
    interval: str = "1m",
    source_file: str = "<dataframe>",
) -> tuple[pd.DataFrame, DatasetManifest]:
    """Standardize an in-memory bar frame; returns (frame, manifest-without-io).

    The returned manifest has no partition entries yet (checksums require the
    written files); :func:`standardize_parquet` fills them in on write.
    """
    missing = [c for c in _REQUIRED_INPUT if c not in df.columns]
    if missing:
        raise ValueError(f"input missing required columns: {missing}")
    if df.empty:
        raise ValueError("input has no rows")

    ts = pd.to_datetime(df["timestamp"])
    if ts.dt.tz is None:
        raise ValueError("input timestamp must be timezone-aware")

    occurred_at_utc = _correct_utc(ts)
    timestamp_et = occurred_at_utc.dt.tz_convert(EASTERN)

    out = pd.DataFrame(
        {
            "occurred_at_utc": occurred_at_utc,
            "timestamp_et": timestamp_et,
            "trading_date": timestamp_et.dt.strftime("%Y-%m-%d"),
            "open": df["open"].astype("float64"),
            "high": df["high"].astype("float64"),
            "low": df["low"].astype("float64"),
            "close": df["close"].astype("float64"),
            "volume": df["volume"].astype("int64"),
            "vwap": (
                df["vwap"].astype("float64")
                if "vwap" in df.columns
                else pd.Series([pd.NA] * len(df), dtype="float64")
            ),
            "count": (
                df["count"].astype("int64")
                if "count" in df.columns
                else pd.Series([0] * len(df), dtype="int64")
            ),
        }
    )
    price_columns = ["open", "high", "low", "close"]
    if not out[price_columns].map(math.isfinite).all().all():
        raise ValueError("OHLC values must be finite")
    if (out[price_columns] <= 0).any().any():
        raise ValueError("OHLC values must be positive")
    if (out["high"] < out[["open", "close", "low"]].max(axis=1)).any() or (
        out["low"] > out[["open", "close", "high"]].min(axis=1)
    ).any():
        raise ValueError("invalid OHLC relationship")
    if (out[["volume", "count"]] < 0).any().any():
        raise ValueError("volume/count must be non-negative")

    duplicate_rows = out[out.duplicated(subset=["occurred_at_utc"], keep=False)]
    for _, group in duplicate_rows.groupby("occurred_at_utc"):
        values = group.drop(columns=["occurred_at_utc", "timestamp_et", "trading_date"])
        if len(values.drop_duplicates()) > 1:
            raise ValueError("conflicting duplicate bars at the same timestamp")
    # Deterministic ordering + dedupe on the authoritative instant.
    out = (
        out.drop_duplicates(subset=["occurred_at_utc"])
        .sort_values("occurred_at_utc")
        .reset_index(drop=True)
    )

    manifest = DatasetManifest(
        provider=provider,
        data_type=data_type,
        symbol=symbol,
        interval=interval,
        source_file=source_file,
    )
    return out[STANDARD_COLUMNS], manifest


def standardize_option_quotes(
    df: pd.DataFrame,
    *,
    underlying: str = "QQQ",
    provider: str = "thetadata",
    source_file: str = "<dataframe>",
) -> tuple[pd.DataFrame, DatasetManifest]:
    """Normalize ThetaData option quotes into a contract-identity-safe frame."""
    aliases = {"expiration": "expiry", "right": "option_type"}
    source = df.rename(
        columns={key: value for key, value in aliases.items() if key in df and value not in df}
    )
    required = ("timestamp", "expiry", "strike", "option_type", "bid", "ask")
    missing = [column for column in required if column not in source]
    if missing:
        raise ValueError(f"option quote input missing required columns: {missing}")
    if source.empty:
        raise ValueError("option quote input has no rows")

    timestamps = pd.to_datetime(source["timestamp"])
    if timestamps.dt.tz is None:
        raise ValueError("option quote timestamp must be timezone-aware")
    occurred_at_utc = _correct_utc(timestamps)
    timestamp_et = occurred_at_utc.dt.tz_convert(EASTERN)
    rights = source["option_type"].astype(str).str.upper().replace({"CALL": "C", "PUT": "P"})
    if not rights.isin(["C", "P"]).all():
        raise ValueError("option_type must be C/P or CALL/PUT")

    out = pd.DataFrame(
        {
            "occurred_at_utc": occurred_at_utc,
            "timestamp_et": timestamp_et,
            "trading_date": timestamp_et.dt.strftime("%Y-%m-%d"),
            "underlying": underlying.upper(),
            "expiry": pd.to_datetime(source["expiry"]).dt.date,
            "strike": source["strike"].astype("float64"),
            "option_type": rights,
            "bid": source["bid"].astype("float64"),
            "ask": source["ask"].astype("float64"),
            "bid_size": (
                source["bid_size"].astype("int64")
                if "bid_size" in source
                else pd.Series([0] * len(source), dtype="int64")
            ),
            "ask_size": (
                source["ask_size"].astype("int64")
                if "ask_size" in source
                else pd.Series([0] * len(source), dtype="int64")
            ),
        }
    )
    numeric = out[["strike", "bid", "ask"]]
    if not numeric.map(math.isfinite).all().all():
        raise ValueError("option strike/bid/ask must be finite")
    if (out["strike"] <= 0).any() or (out[["bid", "ask", "bid_size", "ask_size"]] < 0).any().any():
        raise ValueError("option prices/sizes must be non-negative and strike must be positive")
    if (out["ask"] < out["bid"]).any():
        raise ValueError("crossed option market: ask is below bid")

    identity = ["occurred_at_utc", "underlying", "expiry", "strike", "option_type"]
    duplicate_rows = out[out.duplicated(subset=identity, keep=False)]
    for _, group in duplicate_rows.groupby(identity):
        if len(group.drop_duplicates()) > 1:
            raise ValueError("conflicting duplicate option quotes")
    out = out.drop_duplicates(subset=identity).sort_values(identity).reset_index(drop=True)
    manifest = DatasetManifest(
        provider=provider,
        data_type="option_quote_1m",
        symbol=f"{underlying.upper()}.US",
        interval="1m",
        source_file=source_file,
    )
    return out[OPTION_QUOTE_COLUMNS], manifest


def _dataset_root(root: Path, m: DatasetManifest) -> Path:
    safe_symbol = m.symbol.replace("/", "_")
    return root / m.provider / m.data_type / safe_symbol


def _write_standardized_dataset(
    frame: pd.DataFrame, manifest: DatasetManifest, dest_root: Path
) -> StandardizeResult:
    ds_root = _dataset_root(dest_root, manifest)
    for trading_date, group in frame.groupby("trading_date", sort=True):
        part_dir = ds_root / str(trading_date)
        part_dir.mkdir(parents=True, exist_ok=True)
        part_path = part_dir / "part-000.parquet"
        group.reset_index(drop=True).to_parquet(part_path, index=False)
        manifest.partitions.append(
            PartitionEntry(
                trading_date=str(trading_date),
                relative_path=str(part_path.relative_to(ds_root)),
                rows=len(group),
                sha256=sha256_file(part_path),
                bytes=part_path.stat().st_size,
            )
        )
    manifest.import_status = "COMPLETE"
    manifest_path = manifest.write(ds_root / "_manifest.json")
    return StandardizeResult(manifest=manifest, dataset_root=ds_root, manifest_path=manifest_path)


def standardize_parquet(
    source: str | Path,
    dest_root: str | Path,
    *,
    symbol: str,
    provider: str = "thetadata",
    data_type: str = "equity_1m",
    interval: str = "1m",
) -> StandardizeResult:
    """Read a THETADATA Parquet export, standardize, and write partitions.

    Writes one Parquet per trading date and a finalized ``_manifest.json``.
    Idempotent: re-running overwrites partitions and reproduces identical
    checksums for identical input.
    """
    source = Path(source)
    dest_root = Path(dest_root)
    raw = pd.read_parquet(source)

    frame, manifest = standardize_bars(
        raw,
        symbol=symbol,
        provider=provider,
        data_type=data_type,
        interval=interval,
        source_file=str(source),
    )

    return _write_standardized_dataset(frame, manifest, dest_root)


def standardize_option_parquet(
    source: str | Path,
    dest_root: str | Path,
    *,
    underlying: str = "QQQ",
    provider: str = "thetadata",
) -> StandardizeResult:
    """Normalize a ThetaData option-quote export and write date partitions."""
    source = Path(source)
    frame, manifest = standardize_option_quotes(
        pd.read_parquet(source),
        underlying=underlying,
        provider=provider,
        source_file=str(source),
    )
    return _write_standardized_dataset(frame, manifest, Path(dest_root))
