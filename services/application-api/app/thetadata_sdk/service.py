"""ThetaData SDK polling and the internal Python-to-Rust gRPC service.

The official SDK connects directly to ThetaData's servers. This module owns
credentials and provider-specific DataFrames; Rust receives only completed
one-minute bars and independently validates them before updating DataHealth.
"""

from __future__ import annotations

import asyncio
import math
import os
import stat
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol, cast
from zoneinfo import ZoneInfo

import grpc
import pandas as pd

from app.grpc_gen import market_pb2

NEW_YORK = ZoneInfo("America/New_York")
RTH_OPEN_MINUTE = 9 * 60 + 30
RTH_LAST_BAR_MINUTE = 15 * 60 + 59
_REQUIRED_COLUMNS = frozenset({"timestamp", "open", "high", "low", "close", "volume", "vwap"})


class ThetaDataClient(Protocol):
    def stock_history_ohlc(self, **kwargs: object) -> object: ...


def _decimal_text(value: object, field: str, *, positive: bool = True) -> str:
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"ThetaData {field} is not a decimal") from exc
    if not decimal.is_finite() or (positive and decimal <= 0):
        raise ValueError(f"ThetaData {field} is outside its valid range")
    text = format(decimal, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _volume(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("ThetaData volume must be an integer")
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("ThetaData volume must be an integer") from exc
    if not decimal.is_finite() or decimal < 0 or decimal != decimal.to_integral_value():
        raise ValueError("ThetaData volume must be a non-negative integer")
    volume = int(decimal)
    if volume > 2**64 - 1:
        raise ValueError("ThetaData volume exceeds uint64")
    return volume


def normalize_ohlc_frame(frame: object, expected_date: date) -> list[Any]:
    """Convert an SDK pandas frame to strict protobuf bars.

    ThetaData may append an empty placeholder minute. Fully empty OHLC rows are
    dropped; partially empty or internally inconsistent rows fail the batch.
    """
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("ThetaData OHLC response must be a pandas DataFrame")
    missing = _REQUIRED_COLUMNS.difference(frame.columns)
    if missing:
        raise ValueError(f"ThetaData OHLC response missing columns: {sorted(missing)}")

    bars: list[Any] = []
    seen_minutes: set[int] = set()
    for _, row in frame.iterrows():
        ohlc_missing = [bool(pd.isna(row[field])) for field in ("open", "high", "low", "close")]
        if all(ohlc_missing):
            continue
        if any(ohlc_missing):
            raise ValueError("ThetaData OHLC row is partially empty")

        timestamp = pd.Timestamp(row["timestamp"])
        if timestamp.tzinfo is None:
            raise ValueError("ThetaData timestamp must be timezone-aware")
        timestamp_et = timestamp.tz_convert(NEW_YORK).to_pydatetime()
        if timestamp_et.date() != expected_date:
            raise ValueError("ThetaData OHLC date does not match the requested session")
        minute_et = timestamp_et.hour * 60 + timestamp_et.minute
        if not RTH_OPEN_MINUTE <= minute_et <= RTH_LAST_BAR_MINUTE:
            continue
        if minute_et in seen_minutes:
            raise ValueError("ThetaData OHLC response contains a duplicate minute")

        prices = {
            field: _decimal_text(row[field], field) for field in ("open", "high", "low", "close")
        }
        numeric = {field: float(value) for field, value in prices.items()}
        if not all(math.isfinite(value) for value in numeric.values()):
            raise ValueError("ThetaData OHLC contains a non-finite price")
        if not (
            numeric["low"] <= numeric["open"] <= numeric["high"]
            and numeric["low"] <= numeric["close"] <= numeric["high"]
        ):
            raise ValueError("ThetaData OHLC price is outside the bar range")

        vwap = "" if pd.isna(row["vwap"]) else _decimal_text(row["vwap"], "vwap")
        timestamp_utc = timestamp_et.astimezone(UTC)
        bars.append(
            market_pb2.MarketBar(
                occurred_at_utc=timestamp_utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
                timestamp_et=timestamp_et.isoformat(timespec="seconds"),
                minute_et=minute_et,
                open=prices["open"],
                high=prices["high"],
                low=prices["low"],
                close=prices["close"],
                volume=_volume(row["volume"]),
                vwap=vwap,
            )
        )
        seen_minutes.add(minute_et)

    bars.sort(key=lambda bar: cast(str, bar.occurred_at_utc))
    return bars


def _credentials_file() -> Path | None:
    raw = os.getenv("THETADATA_CREDENTIALS_FILE") or os.getenv("THETADATA_CREDS_FILE")
    if not raw:
        return None
    path = Path(raw).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError("ThetaData credentials file does not exist")
    if os.name != "nt" and stat.S_IMODE(path.stat().st_mode) & 0o077:
        raise PermissionError("ThetaData credentials file must not be accessible by group/others")
    return path


def create_sdk_client() -> ThetaDataClient:
    """Create the official direct SDK client without exposing credential data."""
    try:
        from thetadata.client import ThetaClient
    except ImportError as exc:
        raise RuntimeError("install the official thetadata Python SDK") from exc

    credentials = _credentials_file()
    if credentials is not None:
        client = ThetaClient(creds_file=str(credentials), dataframe_type="pandas")
    elif dotenv_path := os.getenv("THETADATA_DOTENV_PATH"):
        client = ThetaClient(
            dotenv_path=str(Path(dotenv_path).expanduser().resolve()), dataframe_type="pandas"
        )
    elif api_key := os.getenv("THETADATA_API_KEY"):
        client = ThetaClient(api_key=api_key, dataframe_type="pandas")
    else:
        raise RuntimeError(
            "set THETADATA_CREDENTIALS_FILE, THETADATA_DOTENV_PATH, or THETADATA_API_KEY"
        )
    return cast(ThetaDataClient, client)


@dataclass
class ThetaDataBarSource:
    client: ThetaDataClient

    def fetch(
        self,
        *,
        symbol: str,
        venue: str,
        session_date: date,
        start_minute: int,
        end_minute: int,
    ) -> list[Any]:
        def clock_text(minute: int) -> str:
            return f"{minute // 60:02d}:{minute % 60:02d}:00"

        frame = self.client.stock_history_ohlc(
            symbol=symbol,
            interval="1m",
            date=session_date,
            start_time=clock_text(start_minute),
            end_time=clock_text(end_minute),
            venue=venue,
        )
        return normalize_ohlc_frame(frame, session_date)


class ThetaDataSdkService:
    def __init__(
        self,
        source: ThetaDataBarSource,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._source = source
        self._clock = clock or (lambda: datetime.now(tz=UTC))

    async def StreamCompletedBars(
        self, request: Any, context: grpc.aio.ServicerContext[Any, Any]
    ) -> AsyncGenerator[Any, None]:
        symbol = str(request.symbol).upper()
        venue = str(request.venue).lower()
        if symbol != "QQQ" or venue not in {"nqb", "utp_cta"}:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "unsupported symbol or venue")
            return
        poll_ms = min(max(int(request.poll_interval_ms or 2_000), 500), 60_000)

        session_date: date | None = None
        last_minute: int | None = None
        first_batch = True
        while True:
            now_et = self._clock().astimezone(NEW_YORK)
            if session_date != now_et.date():
                session_date = now_et.date()
                last_minute = None
                first_batch = True

            current_minute = now_et.hour * 60 + now_et.minute
            complete_through = min(current_minute - 1, RTH_LAST_BAR_MINUTE)
            start_minute = RTH_OPEN_MINUTE if last_minute is None else last_minute + 1
            if complete_through >= start_minute:
                try:
                    bars = await asyncio.to_thread(
                        self._source.fetch,
                        symbol=symbol,
                        venue=venue,
                        session_date=session_date,
                        start_minute=start_minute,
                        # Query through the current clock minute because the SDK
                        # may append that minute as an empty placeholder. The
                        # filter below still forbids an in-progress bar.
                        end_minute=min(current_minute, RTH_LAST_BAR_MINUTE + 1),
                    )
                except Exception as exc:
                    await context.abort(
                        grpc.StatusCode.UNAVAILABLE,
                        f"ThetaData SDK fetch failed ({type(exc).__name__})",
                    )
                    return
                bars = [bar for bar in bars if int(bar.minute_et) <= complete_through]
                if bars:
                    last_minute = int(bars[-1].minute_et)
                if bars or first_batch:
                    fetched_at = self._clock().astimezone(UTC)
                    yield market_pb2.ThetaSdkBarBatch(
                        bars=bars,
                        backfill=first_batch,
                        fetched_at_utc=fetched_at.isoformat(timespec="seconds").replace(
                            "+00:00", "Z"
                        ),
                        complete_through_minute_et=complete_through,
                        session_date=session_date.isoformat(),
                    )
                    first_batch = False
            elif first_batch:
                fetched_at = self._clock().astimezone(UTC)
                yield market_pb2.ThetaSdkBarBatch(
                    bars=[],
                    backfill=True,
                    fetched_at_utc=fetched_at.isoformat(timespec="seconds").replace("+00:00", "Z"),
                    complete_through_minute_et=0,
                    session_date=session_date.isoformat(),
                )
                first_batch = False
            await asyncio.sleep(poll_ms / 1_000)
