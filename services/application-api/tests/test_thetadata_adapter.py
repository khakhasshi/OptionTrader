"""Offline contract tests for the ThetaData historical research adapter."""

from __future__ import annotations

from datetime import date

import pandas as pd

from app.ingestion import ThetaDataHistoricalAdapter


class FakeThetaClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def _respond(self, name: str, kwargs: dict[str, object]) -> pd.DataFrame:
        self.calls.append((name, kwargs))
        return pd.DataFrame({"timestamp": ["2026-07-09 09:30:00"], "value": [1.0]})

    def stock_history_ohlc(self, **kwargs: object) -> object:
        return self._respond("stock_history_ohlc", kwargs)

    def index_history_ohlc(self, **kwargs: object) -> object:
        return self._respond("index_history_ohlc", kwargs)

    def option_history_quote(self, **kwargs: object) -> object:
        return self._respond("option_history_quote", kwargs)

    def option_history_trade(self, **kwargs: object) -> object:
        return self._respond("option_history_trade", kwargs)


def test_adapter_requests_complete_phase1_source_set() -> None:
    client = FakeThetaClient()
    adapter = ThetaDataHistoricalAdapter(client)
    trading_date = date(2026, 7, 9)

    adapter.qqq_bars(trading_date, trading_date)
    adapter.vix_bars(trading_date, trading_date)
    adapter.qqq_option_quotes(trading_date, trading_date)
    adapter.qqq_option_trades(trading_date, trading_date)

    assert [name for name, _ in client.calls] == [
        "stock_history_ohlc",
        "index_history_ohlc",
        "option_history_quote",
        "option_history_trade",
    ]
    assert client.calls[0][1]["symbol"] == "QQQ"
    assert client.calls[1][1]["symbol"] == "VIX"
    assert client.calls[2][1]["right"] == "both"
    assert client.calls[3][1]["expiration"] == trading_date
