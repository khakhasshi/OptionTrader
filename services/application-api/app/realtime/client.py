"""gRPC client for the Rust Market Core snapshot/bar stream.

Wraps the generated ``MarketServiceStub`` and converts protobuf ticks into the
plain dicts the :class:`CockpitProjector` consumes. Kept deliberately thin: no
engine logic here, just transport + proto->dict, so the projector stays pure and
the client stays trivially mockable in tests.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

import grpc

from app.grpc_gen import market_pb2, market_pb2_grpc

TRADING_CORE_GRPC = os.getenv("TRADING_CORE_GRPC", "localhost:50051")

# proto enum int -> contract DataHealth string.
_HEALTH_NAME: dict[int, str] = {
    int(market_pb2.DATA_HEALTH_HEALTHY): "HEALTHY",
    int(market_pb2.DATA_HEALTH_DEGRADED): "DEGRADED",
    int(market_pb2.DATA_HEALTH_STALE): "STALE",
    int(market_pb2.DATA_HEALTH_DISCONNECTED): "DISCONNECTED",
    int(market_pb2.DATA_HEALTH_RECONCILING): "RECONCILING",
}


def health_name(value: int) -> str:
    """Map a DataHealth enum int to its contract string, fail-closed default."""
    return _HEALTH_NAME.get(int(value), "DISCONNECTED")


def snapshot_to_dict(snap: market_pb2.MarketSnapshot) -> dict[str, Any]:
    """Convert a MarketSnapshot proto to a market_snapshot.json-shaped dict.

    Optional non-nullable decimal fields (previous_close, opening_range_*) are
    OMITTED when the proto carries an empty string — the contract types them as
    decimals, not null, so absence must be absence, never a fabricated zero.
    Nullable fields (premarket_*) become ``None``.
    """
    out: dict[str, Any] = {
        "schema_version": snap.schema_version,
        "snapshot_id": snap.snapshot_id,
        "occurred_at_utc": snap.occurred_at_utc,
        "timestamp_et": snap.timestamp_et,
        "symbol": snap.symbol,
        "price": snap.price,
        "open": snap.open,
        "high": snap.high,
        "low": snap.low,
        "vwap": snap.vwap,
        "volume": snap.volume,
        "premarket_high": snap.premarket_high or None,
        "premarket_low": snap.premarket_low or None,
        "sequence_number": snap.sequence_number,
        "quote_age_ms": snap.quote_age_ms,
        "data_health": health_name(snap.data_health),
    }
    if snap.previous_close:
        out["previous_close"] = snap.previous_close
    if snap.opening_range_high:
        out["opening_range_high"] = snap.opening_range_high
    if snap.opening_range_low:
        out["opening_range_low"] = snap.opening_range_low
    return out


def bar_to_dict(bar: market_pb2.MarketBar) -> dict[str, Any]:
    return {
        "occurred_at_utc": bar.occurred_at_utc,
        "timestamp_et": bar.timestamp_et,
        "minute_et": bar.minute_et,
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "vwap": bar.vwap or None,
    }


def tick_to_dict(tick: market_pb2.MarketTick) -> dict[str, Any]:
    return {
        "snapshot": snapshot_to_dict(tick.snapshot) if tick.HasField("snapshot") else None,
        "bar": bar_to_dict(tick.bar) if tick.HasField("bar") else None,
    }


def stream_ticks(session_id: str, target: str | None = None) -> Iterator[dict[str, Any]]:
    """Yield ticks (as dicts) from the Rust stream for one session.

    Raises ``grpc.RpcError`` on transport failure — callers fail closed by
    emitting a disconnected frame.
    """
    channel = grpc.insecure_channel(target or TRADING_CORE_GRPC)
    stub = market_pb2_grpc.MarketServiceStub(channel)
    request = market_pb2.StreamRequest(session_id=session_id, speedup=0.0)
    try:
        for tick in stub.StreamMarketSnapshots(request):
            yield tick_to_dict(tick)
    finally:
        channel.close()
