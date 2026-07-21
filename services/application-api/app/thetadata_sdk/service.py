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
from datetime import UTC, date, datetime, time
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from pathlib import Path
from statistics import NormalDist
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

    def option_snapshot_quote(self, **kwargs: object) -> object: ...

    def option_snapshot_greeks_first_order(self, **kwargs: object) -> object: ...


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


def _positive_size(value: object, field: str) -> int:
    size = _volume(value)
    if size == 0 or size > 2**32 - 1:
        raise ValueError(f"ThetaData {field} must be a positive uint32")
    return size


def _column(row: pd.Series, *names: str) -> object:
    for name in names:
        if name in row.index:
            return row[name]
    raise ValueError(f"ThetaData option response missing one of columns: {list(names)}")


def _utc_timestamp(value: object, field: str) -> datetime:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        raise ValueError(f"ThetaData {field} must be timezone-aware")
    return cast(datetime, timestamp.to_pydatetime()).astimezone(UTC)


def _gamma_from_first_order(
    greeks: pd.Series, contract: Any, quote_at: datetime, delta: Decimal
) -> str:
    """Derive Gamma only from Standard-tier ThetaData first-order inputs."""
    underlying_at = _utc_timestamp(_column(greeks, "underlying_timestamp"), "underlying timestamp")
    if abs((quote_at - underlying_at).total_seconds()) > 5:
        raise ValueError("ThetaData option and underlying timestamps are not synchronized")
    implied_vol = Decimal(
        _decimal_text(_column(greeks, "implied_vol"), "implied_vol", positive=True)
    )
    underlying = Decimal(
        _decimal_text(_column(greeks, "underlying_price"), "underlying_price", positive=True)
    )
    expiration = date.fromisoformat(str(contract.expiration))
    close_et = datetime.combine(expiration, time(16), tzinfo=NEW_YORK)
    years = Decimal(str((close_et - quote_at.astimezone(NEW_YORK)).total_seconds())) / Decimal(
        str(365.25 * 24 * 60 * 60)
    )
    if years <= 0:
        raise ValueError("ThetaData option has no positive time to expiration")
    probability = delta if contract.right == market_pb2.THETA_OPTION_RIGHT_CALL else delta + 1
    if not Decimal("0") < probability < Decimal("1"):
        raise ValueError("ThetaData delta cannot derive a finite Gamma")
    d1 = NormalDist().inv_cdf(float(probability))
    density = math.exp(-(d1**2) / 2) / math.sqrt(2 * math.pi)
    gamma = Decimal(str(density)) / (underlying * implied_vol * years.sqrt())
    return _decimal_text(gamma, "derived gamma", positive=False)


def normalize_option_snapshot(
    quote_frame: object,
    greeks_frame: object,
    contract: Any,
) -> Any:
    """Merge one exact-contract ThetaData quote and first-order Greeks row."""
    if not isinstance(quote_frame, pd.DataFrame) or not isinstance(greeks_frame, pd.DataFrame):
        raise TypeError("ThetaData option responses must be pandas DataFrames")
    if len(quote_frame.index) != 1 or len(greeks_frame.index) != 1:
        raise ValueError("ThetaData exact option request must return exactly one row")
    quote = quote_frame.iloc[0]
    greeks = greeks_frame.iloc[0]
    bid = _decimal_text(_column(quote, "bid", "bid_price"), "option bid")
    ask = _decimal_text(_column(quote, "ask", "ask_price"), "option ask")
    if Decimal(bid) > Decimal(ask):
        raise ValueError("ThetaData option quote is crossed")
    quote_at = _utc_timestamp(
        _column(quote, "timestamp", "quote_timestamp", "occurred_at"), "option quote timestamp"
    )
    greek_at = _utc_timestamp(
        _column(greeks, "timestamp", "greeks_timestamp", "occurred_at"),
        "option Greeks timestamp",
    )
    if abs((quote_at - greek_at).total_seconds()) > 5:
        raise ValueError("ThetaData quote and Greeks timestamps are not synchronized")
    values = {
        name: _decimal_text(_column(greeks, name), name, positive=False)
        for name in ("delta", "theta", "vega")
    }
    delta = Decimal(values["delta"])
    if not Decimal("-1") < delta < Decimal("1"):
        raise ValueError("ThetaData delta is outside (-1, 1)")
    values["gamma"] = _gamma_from_first_order(greeks, contract, quote_at, delta)
    if Decimal(values["gamma"]) <= 0 or Decimal(values["vega"]) < 0:
        raise ValueError("ThetaData gamma must be positive and vega non-negative")
    return market_pb2.ThetaOptionSnapshot(
        contract_id=str(contract.contract_id),
        symbol=str(contract.symbol).upper(),
        expiration=str(contract.expiration),
        strike=_decimal_text(contract.strike, "option strike"),
        right=contract.right,
        bid=bid,
        ask=ask,
        bid_size=_positive_size(_column(quote, "bid_size", "bid_sz"), "option bid_size"),
        ask_size=_positive_size(_column(quote, "ask_size", "ask_sz"), "option ask_size"),
        occurred_at_utc=quote_at.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        delta=values["delta"],
        gamma=values["gamma"],
        theta=values["theta"],
        vega=values["vega"],
        provider="THETADATA",
    )


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


@dataclass
class ThetaDataOptionSource:
    client: ThetaDataClient

    def fetch(self, contract: Any) -> Any:
        right: str | None = {
            market_pb2.THETA_OPTION_RIGHT_CALL: "call",
            market_pb2.THETA_OPTION_RIGHT_PUT: "put",
        }.get(contract.right)
        if right is None:
            raise ValueError("ThetaData option right is unspecified")
        arguments = {
            "symbol": str(contract.symbol).upper(),
            "expiration": str(contract.expiration),
            "strike": str(contract.strike),
            "right": right,
        }
        quote = self.client.option_snapshot_quote(**arguments)
        greeks = self.client.option_snapshot_greeks_first_order(**arguments)
        return normalize_option_snapshot(quote, greeks, contract)


class ThetaDataSdkService:
    def __init__(
        self,
        source: ThetaDataBarSource,
        option_source: ThetaDataOptionSource | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._source = source
        self._option_source = option_source
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

    async def GetOptionSnapshots(
        self, request: Any, context: grpc.aio.ServicerContext[Any, Any]
    ) -> Any:
        option_source = self._option_source
        if option_source is None:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "option source unavailable")
            raise AssertionError("gRPC abort must raise")
        contracts = list(request.contracts)
        identities = [str(contract.contract_id) for contract in contracts]
        try:
            strikes_valid = all(
                (strike := Decimal(str(contract.strike))).is_finite() and strike > 0
                for contract in contracts
            )
        except (InvalidOperation, ValueError):
            strikes_valid = False
        if (
            not 1 <= len(contracts) <= 4
            or len(set(identities)) != len(identities)
            or not strikes_valid
            or any(
                not contract.contract_id
                or str(contract.symbol).upper() != "QQQ"
                or not contract.expiration
                or contract.right
                not in {
                    market_pb2.THETA_OPTION_RIGHT_CALL,
                    market_pb2.THETA_OPTION_RIGHT_PUT,
                }
                for contract in contracts
            )
        ):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "invalid option contract request")
        try:
            snapshots = [
                await asyncio.to_thread(option_source.fetch, contract) for contract in contracts
            ]
        except Exception as exc:
            await context.abort(
                grpc.StatusCode.UNAVAILABLE,
                f"ThetaData option fetch failed ({type(exc).__name__})",
            )
            raise AssertionError("gRPC abort must raise") from exc
        digest = sha256()
        for snapshot in snapshots:
            digest.update(snapshot.SerializeToString(deterministic=True))
        fetched_at = self._clock().astimezone(UTC)
        return market_pb2.ThetaOptionSnapshotBatch(
            chain_snapshot_id=f"thetaopt_{digest.hexdigest()}",
            fetched_at_utc=fetched_at.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            snapshots=snapshots,
            provider="THETADATA",
        )
