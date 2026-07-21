from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime
from typing import Any

import pandas as pd
import pytest

from app.grpc_gen import market_pb2
from app.thetadata_sdk.service import ThetaDataBarSource, ThetaDataSdkService, normalize_ohlc_frame


def frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-07-20 09:30:00-04:00",
                    "2026-07-20 09:31:00-04:00",
                    "2026-07-20 09:32:00-04:00",
                ]
            ),
            "open": [500.0, 500.5, None],
            "high": [501.0, 501.5, None],
            "low": [499.5, 500.0, None],
            "close": [500.5, 501.0, None],
            "volume": [100, 120, 0],
            "vwap": [500.25, 500.75, 0.0],
        }
    )


def test_normalize_filters_provider_placeholder_and_emits_utc_z() -> None:
    bars = normalize_ohlc_frame(frame(), date(2026, 7, 20))

    assert len(bars) == 2
    assert bars[0].occurred_at_utc == "2026-07-20T13:30:00Z"
    assert bars[0].timestamp_et == "2026-07-20T09:30:00-04:00"
    assert bars[0].minute_et == 570
    assert bars[0].open == "500"


def test_normalize_rejects_partial_placeholder_and_bad_range() -> None:
    partial = frame().iloc[[0]].copy()
    partial.loc[0, "close"] = None
    with pytest.raises(ValueError, match="partially empty"):
        normalize_ohlc_frame(partial, date(2026, 7, 20))

    invalid = frame().iloc[[0]].copy()
    invalid.loc[0, "close"] = 502.0
    with pytest.raises(ValueError, match="outside the bar range"):
        normalize_ohlc_frame(invalid, date(2026, 7, 20))


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def stock_history_ohlc(self, **kwargs: object) -> object:
        self.calls.append(kwargs)
        return frame()


class AbortContext:
    async def abort(self, code: Any, details: str) -> None:
        raise RuntimeError(f"{code}: {details}")


def test_first_sdk_batch_is_backfill_and_uses_completed_minute_only() -> None:
    client = FakeClient()
    service = ThetaDataSdkService(
        ThetaDataBarSource(client),
        clock=lambda: datetime(2026, 7, 20, 13, 32, 30, tzinfo=UTC),
    )
    request = market_pb2.ThetaSdkStreamRequest(symbol="QQQ", venue="nqb", poll_interval_ms=500)

    async def first() -> Any:
        stream = service.StreamCompletedBars(request, AbortContext())
        try:
            return await anext(stream)
        finally:
            await stream.aclose()

    batch = asyncio.run(first())

    assert batch.backfill is True
    assert len(batch.bars) == 2
    assert batch.complete_through_minute_et == 571
    assert batch.session_date == "2026-07-20"
    assert client.calls[0]["start_time"] == "09:30:00"
    assert client.calls[0]["end_time"] == "09:32:00"


def test_invalid_contract_fails_closed_before_sdk_call() -> None:
    client = FakeClient()
    service = ThetaDataSdkService(ThetaDataBarSource(client))
    request = market_pb2.ThetaSdkStreamRequest(symbol="SPY", venue="nqb")

    async def first() -> None:
        stream = service.StreamCompletedBars(request, AbortContext())
        await anext(stream)

    with pytest.raises(RuntimeError, match="INVALID_ARGUMENT"):
        asyncio.run(first())
    assert client.calls == []
