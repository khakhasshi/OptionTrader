"""Thin historical adapter for the ThetaData v3 Python SDK.

Historical downloads are research jobs because ThetaData's supported local
SDK is Python/gRPC. Raw frames are handed to the Rust Market Core boundary for
authoritative normalization and deterministic features; the Python
standardizer remains an offline compatibility path for existing datasets.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol, cast

import pandas as pd


class ThetaDataClient(Protocol):
    """Subset of ``thetadata.ThetaClient`` used by Phase 1."""

    def stock_history_ohlc(self, **kwargs: object) -> object: ...

    def index_history_ohlc(self, **kwargs: object) -> object: ...

    def option_history_quote(self, **kwargs: object) -> object: ...

    def option_history_trade(self, **kwargs: object) -> object: ...


def _frame(value: object, label: str) -> pd.DataFrame:
    if not isinstance(value, pd.DataFrame):
        raise TypeError(f"ThetaData {label} response must be a pandas DataFrame")
    if value.empty:
        raise ValueError(f"ThetaData {label} response is empty")
    return cast(pd.DataFrame, value).copy()


@dataclass(frozen=True)
class ThetaDataHistoricalAdapter:
    """Fetch the complete Phase 1 source set through an injected SDK client."""

    client: ThetaDataClient
    interval: str = "1m"
    venue: str = "utp_cta"

    @classmethod
    def from_sdk(cls, **client_kwargs: Any) -> ThetaDataHistoricalAdapter:
        """Create the adapter when the optional ``thetadata`` SDK is installed."""
        try:
            from thetadata.client import ThetaClient as SdkClient
        except ImportError as exc:
            raise RuntimeError(
                "ThetaData SDK is not installed; run this research job in the ThetaData environment"
            ) from exc
        return cls(
            client=cast(ThetaDataClient, SdkClient(dataframe_type="pandas", **client_kwargs))
        )

    def qqq_bars(self, start_date: date, end_date: date) -> pd.DataFrame:
        return _frame(
            self.client.stock_history_ohlc(
                symbol="QQQ",
                interval=self.interval,
                start_date=start_date,
                end_date=end_date,
                venue=self.venue,
            ),
            "QQQ OHLC",
        )

    def vix_bars(self, start_date: date, end_date: date) -> pd.DataFrame:
        return _frame(
            self.client.index_history_ohlc(
                symbol="VIX",
                interval=self.interval,
                start_date=start_date,
                end_date=end_date,
            ),
            "VIX OHLC",
        )

    def qqq_option_quotes(self, trading_date: date, expiry: date) -> pd.DataFrame:
        return _frame(
            self.client.option_history_quote(
                symbol="QQQ",
                expiration=expiry,
                interval=self.interval,
                date=trading_date,
                strike="*",
                right="both",
            ),
            "QQQ option quotes",
        )

    def qqq_option_trades(self, trading_date: date, expiry: date) -> pd.DataFrame:
        return _frame(
            self.client.option_history_trade(
                symbol="QQQ",
                expiration=expiry,
                date=trading_date,
                strike="*",
                right="both",
            ),
            "QQQ option trades",
        )


__all__ = ["ThetaDataClient", "ThetaDataHistoricalAdapter"]
