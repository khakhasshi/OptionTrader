"""Deterministic replay clock: standardized bars -> MarketSnapshot stream.

The clock walks bars in ``occurred_at_utc`` order and, for each, emits one
``MarketSnapshot`` reflecting session state *as of that bar*: running
open/high/low, cumulative volume, session VWAP, and the first-N-minute opening
range. Output is deterministic — no wall-clock, no RNG — so a given dataset
always yields the same snapshot sequence.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import pandas as pd

# Fixed-point rendering for contract `decimal` strings (2dp for equity prices).
_QUANT = Decimal("0.01")


def _dec(value: float | Decimal) -> str:
    return str(Decimal(str(value)).quantize(_QUANT))


@dataclass(frozen=True)
class ReplayConfig:
    """Replay knobs. Defaults match the QQQ intraday design."""

    symbol: str = "QQQ.US"
    opening_range_minutes: int = 15
    previous_close: float | None = None


class ReplayClock:
    """Turns one trading date's bars into a deterministic snapshot stream."""

    def __init__(self, bars: pd.DataFrame, config: ReplayConfig | None = None):
        if bars.empty:
            raise ValueError("no bars to replay")
        self._bars = bars.sort_values("occurred_at_utc").reset_index(drop=True)
        self._cfg = config or ReplayConfig()

    def snapshots(self) -> Iterator[dict[str, object]]:
        cfg = self._cfg
        session_open = float(self._bars["open"].iloc[0])
        first_ts = self._bars["occurred_at_utc"].iloc[0]
        or_deadline = first_ts + pd.Timedelta(minutes=cfg.opening_range_minutes)

        running_high = float("-inf")
        running_low = float("inf")
        cum_vol = 0
        cum_pv = 0.0  # sum(price*volume) for session VWAP
        or_high = float("-inf")
        or_low = float("inf")

        for seq, row in enumerate(self._bars.itertuples(index=False)):
            high = float(row.high)
            low = float(row.low)
            close = float(row.close)
            vol = int(row.volume)

            running_high = max(running_high, high)
            running_low = min(running_low, low)
            cum_vol += vol
            cum_pv += close * vol

            occurred = row.occurred_at_utc
            if occurred < or_deadline:
                or_high = max(or_high, high)
                or_low = min(or_low, low)

            vwap = (cum_pv / cum_vol) if cum_vol else close
            or_set = or_high != float("-inf")

            snap: dict[str, object] = {
                "schema_version": "1.0",
                "snapshot_id": self._snapshot_id(occurred, seq),
                "occurred_at_utc": self._to_z(occurred),
                "timestamp_et": row.timestamp_et.isoformat(),
                "symbol": cfg.symbol,
                "price": _dec(close),
                "open": _dec(session_open),
                "high": _dec(running_high),
                "low": _dec(running_low),
                "vwap": _dec(vwap),
                "volume": cum_vol,
                "sequence_number": seq,
                "data_health": "HEALTHY",
            }
            if or_set:
                snap["opening_range_high"] = _dec(or_high)
                snap["opening_range_low"] = _dec(or_low)
            if cfg.previous_close is not None:
                snap["previous_close"] = _dec(cfg.previous_close)
            yield snap

    @staticmethod
    def _to_z(ts: pd.Timestamp) -> str:
        return str(ts.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"))

    def _snapshot_id(self, ts: pd.Timestamp, seq: int) -> str:
        et = ts.tz_convert("America/New_York")
        return f"mkt_{et.strftime('%Y%m%d_%H%M%S')}_{seq:06d}"


def replay_trading_date(
    dataset_root: str | Path,
    trading_date: str,
    config: ReplayConfig | None = None,
) -> list[dict[str, object]]:
    """Load one date's partition and return its full snapshot list.

    ``dataset_root`` is the standardized dataset directory (the one holding
    ``_manifest.json`` and ``<trading_date>/part-000.parquet``).
    """
    part = Path(dataset_root) / trading_date / "part-000.parquet"
    if not part.exists():
        raise FileNotFoundError(f"no partition for {trading_date}: {part}")
    bars = pd.read_parquet(part)
    return list(ReplayClock(bars, config).snapshots())
