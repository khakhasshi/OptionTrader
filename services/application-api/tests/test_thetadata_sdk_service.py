from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, cast

import pandas as pd
import pytest

from app.grpc_gen import market_pb2
from app.thetadata_sdk.service import (
    ThetaDataBarSource,
    ThetaDataOptionSource,
    ThetaDataSdkService,
    normalize_ohlc_frame,
    normalize_option_snapshot,
)


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

    def option_snapshot_quote(self, **kwargs: object) -> object:
        self.calls.append(kwargs)
        return pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2026-07-20 10:29:59.800-04:00"]),
                "bid": [2.4],
                "ask": [2.5],
                "bid_size": [20],
                "ask_size": [25],
            }
        )

    def option_snapshot_greeks_first_order(self, **kwargs: object) -> object:
        self.calls.append(kwargs)
        return pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2026-07-20 10:30:00-04:00"]),
                "delta": [0.52],
                "gamma": [0.08],
                "theta": [-0.12],
                "vega": [0.05],
                "implied_vol": [0.20],
                "underlying_price": [500.0],
                "underlying_timestamp": pd.to_datetime(["2026-07-20 10:29:59.800-04:00"]),
            }
        )


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


def option_contract() -> Any:
    return market_pb2.ThetaOptionContractRequest(
        contract_id="QQQ-20260720-500-C",
        symbol="QQQ",
        expiration="2026-07-20",
        strike="500",
        right=market_pb2.THETA_OPTION_RIGHT_CALL,
    )


def test_option_quote_and_greeks_form_deterministic_thetadata_proof() -> None:
    client = FakeClient()
    service = ThetaDataSdkService(
        ThetaDataBarSource(client),
        ThetaDataOptionSource(client),
        clock=lambda: datetime(2026, 7, 20, 14, 30, 1, tzinfo=UTC),
    )

    async def fetch() -> Any:
        return await service.GetOptionSnapshots(
            market_pb2.ThetaOptionSnapshotRequest(contracts=[option_contract()]), AbortContext()
        )

    first = asyncio.run(fetch())
    second = asyncio.run(fetch())
    assert first.chain_snapshot_id == second.chain_snapshot_id
    assert first.provider == "THETADATA"
    assert first.snapshots[0].occurred_at_utc == "2026-07-20T14:29:59.800Z"
    assert first.snapshots[0].bid == "2.4"
    assert first.snapshots[0].delta == "0.52"
    assert client.calls[0]["right"] == "call"


def test_option_normalization_rejects_duplicate_crossed_or_unsynchronized_rows() -> None:
    client = FakeClient()
    quote = cast(pd.DataFrame, client.option_snapshot_quote())
    greeks = cast(pd.DataFrame, client.option_snapshot_greeks_first_order())
    duplicate = pd.concat([quote, quote], ignore_index=True)
    with pytest.raises(ValueError, match="exactly one"):
        normalize_option_snapshot(duplicate, greeks, option_contract())
    crossed = quote.copy()
    crossed.loc[0, "bid"] = 3.0
    with pytest.raises(ValueError, match="crossed"):
        normalize_option_snapshot(crossed, greeks, option_contract())
    delayed = greeks.copy()
    delayed.loc[0, "timestamp"] = pd.Timestamp("2026-07-20 10:31:00-04:00")
    with pytest.raises(ValueError, match="synchronized"):
        normalize_option_snapshot(quote, delayed, option_contract())


def test_standard_tier_first_order_inputs_derive_gamma_without_broker_data() -> None:
    client = FakeClient()
    quote = cast(pd.DataFrame, client.option_snapshot_quote())
    greeks = cast(pd.DataFrame, client.option_snapshot_greeks_first_order()).drop(columns=["gamma"])
    greeks["implied_vol"] = [0.20]
    greeks["underlying_price"] = [500.0]
    greeks["underlying_timestamp"] = greeks["timestamp"]
    snapshot = normalize_option_snapshot(quote, greeks, option_contract())
    assert Decimal(snapshot.gamma) > 0

    provider_gamma = greeks.copy()
    provider_gamma["gamma"] = [0.0]
    assert normalize_option_snapshot(quote, provider_gamma, option_contract()).gamma == (
        snapshot.gamma
    )

    boundary_delta = greeks.copy()
    boundary_delta["delta"] = [1.0]
    with pytest.raises(ValueError, match="delta"):
        normalize_option_snapshot(quote, boundary_delta, option_contract())

    greeks.loc[0, "underlying_timestamp"] = pd.Timestamp("2026-07-20 10:31:00-04:00")
    with pytest.raises(ValueError, match="underlying timestamps"):
        normalize_option_snapshot(quote, greeks, option_contract())
