"""Option-derived features: ATM straddle mark and bid/ask spread.

These require an option-quote source not yet ingested. To keep them testable
now and trivially wireable later, they operate on a documented quote-frame
contract rather than any specific provider payload:

    columns: ``strike`` (float), ``option_type`` ("C"/"P"),
             ``bid`` (float), ``ask`` (float)

Each row is one option's top-of-book at a single instant. The functions are
pure and make no network calls.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

_REQUIRED = ("strike", "option_type", "bid", "ask")


@dataclass(frozen=True)
class StraddleMark:
    """ATM straddle: the call+put mid at the strike nearest spot."""

    strike: float
    call_mid: float
    put_mid: float

    @property
    def mark(self) -> float:
        return self.call_mid + self.put_mid


def _validate(quotes: pd.DataFrame) -> None:
    missing = [c for c in _REQUIRED if c not in quotes.columns]
    if missing:
        raise ValueError(f"quotes missing columns: {missing}")
    if quotes.empty:
        raise ValueError("no quotes")


def _mid(row: pd.Series) -> float:
    return (float(row["bid"]) + float(row["ask"])) / 2.0


def bid_ask_spread(quotes: pd.DataFrame) -> pd.Series:
    """Absolute bid/ask spread per quote row, indexed like the input."""
    _validate(quotes)
    return quotes["ask"].astype("float64") - quotes["bid"].astype("float64")


def atm_straddle(quotes: pd.DataFrame, spot: float) -> StraddleMark:
    """ATM straddle mark: pick the strike nearest ``spot``, sum call+put mids.

    Requires both a call and a put at the chosen strike.
    """
    _validate(quotes)
    strikes = quotes["strike"].astype("float64")
    atm_strike = float(strikes.iloc[(strikes - spot).abs().argmin()])
    at = quotes[quotes["strike"].astype("float64") == atm_strike]
    calls = at[at["option_type"].str.upper() == "C"]
    puts = at[at["option_type"].str.upper() == "P"]
    if calls.empty or puts.empty:
        raise ValueError(f"strike {atm_strike} missing a call or put leg")
    return StraddleMark(
        strike=atm_strike,
        call_mid=_mid(calls.iloc[0]),
        put_mid=_mid(puts.iloc[0]),
    )
