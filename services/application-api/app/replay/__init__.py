"""Deterministic offline replay of standardized 1-minute bars.

Reads the Parquet partitions produced by :mod:`app.ingestion` and replays them
bar-by-bar as schema-compliant ``MarketSnapshot`` records (see
``packages/contracts/jsonschema/market_snapshot.json``). Replay is a pure
function of the input dataset: same partitions in, byte-identical snapshot
stream out, which is what P1-8 reproducibility checks anchor on.
"""

from app.replay.clock import ReplayClock, ReplayConfig, replay_trading_date
from app.replay.reproducibility import hash_snapshots

__all__ = ["ReplayClock", "ReplayConfig", "hash_snapshots", "replay_trading_date"]
