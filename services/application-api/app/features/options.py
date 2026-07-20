"""Option-derived features: ATM straddle mark and bid/ask spread.

These are offline reference calculations for independent fixture comparison
with Rust Market Core. They consume the provider-neutral quote-frame contract:

    columns: ``underlying``, ``expiry``, ``strike``, ``option_type`` ("C"/"P"),
             ``occurred_at_utc``, ``bid``, ``ask``

Each row is one option's top-of-book at a single instant. The functions are
pure and make no network calls.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import math

import pandas as pd

_REQUIRED = (
    "underlying",
    "expiry",
    "strike",
    "option_type",
    "occurred_at_utc",
    "bid",
    "ask",
)


@dataclass(frozen=True)
class StraddleMark:
    """ATM straddle: the call+put mid at the strike nearest spot."""

    underlying: str
    expiry: date
    occurred_at_utc: pd.Timestamp
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
    numeric = quotes[["strike", "bid", "ask"]].astype("float64")
    if not numeric.map(math.isfinite).all().all():
        raise ValueError("quotes contain non-finite strike/bid/ask")
    if (numeric["strike"] <= 0).any() or (numeric[["bid", "ask"]] < 0).any().any():
        raise ValueError("strike/bid/ask must be non-negative and strike must be positive")
    if (numeric["ask"] < numeric["bid"]).any():
        raise ValueError("crossed option market: ask is below bid")
    rights = quotes["option_type"].astype(str).str.upper()
    if not rights.isin(["C", "P"]).all():
        raise ValueError("option_type must be C or P")
    if any(pd.Timestamp(value).tzinfo is None for value in quotes["occurred_at_utc"]):
        raise ValueError("quote timestamps must be timezone-aware")
    occurred = pd.to_datetime(quotes["occurred_at_utc"], utc=True, errors="raise")
    if occurred.isna().any():
        raise ValueError("quote timestamp is required")


def _mid(row: pd.Series) -> float:
    return (float(row["bid"]) + float(row["ask"])) / 2.0


def bid_ask_spread(quotes: pd.DataFrame) -> pd.Series:
    """Absolute bid/ask spread per quote row, indexed like the input."""
    _validate(quotes)
    return quotes["ask"].astype("float64") - quotes["bid"].astype("float64")


def atm_straddle(
    quotes: pd.DataFrame,
    spot: float,
    *,
    underlying: str,
    expiry: date | str,
    as_of: pd.Timestamp,
    max_quote_age_ms: int = 1_000,
) -> StraddleMark:
    """Return a same-expiry, same-snapshot ATM straddle mark.

    Both legs must match the requested underlying/expiry, have exactly the
    same authoritative quote timestamp, and be no older than
    ``max_quote_age_ms`` at ``as_of``.
    """
    _validate(quotes)
    if not math.isfinite(spot) or spot <= 0:
        raise ValueError("spot must be positive and finite")
    if max_quote_age_ms < 0:
        raise ValueError("max_quote_age_ms must be non-negative")

    target_expiry = pd.Timestamp(expiry).date()
    target_as_of = pd.Timestamp(as_of)
    if target_as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    target_as_of = target_as_of.tz_convert("UTC")

    frame = quotes.copy()
    frame["option_type"] = frame["option_type"].astype(str).str.upper()
    frame["_occurred"] = pd.to_datetime(frame["occurred_at_utc"], utc=True)
    frame["_expiry"] = pd.to_datetime(frame["expiry"]).dt.date
    age_ms = (target_as_of - frame["_occurred"]).dt.total_seconds() * 1_000
    frame = frame[
        (frame["underlying"].astype(str).str.upper() == underlying.upper())
        & (frame["_expiry"] == target_expiry)
        & (age_ms >= 0)
        & (age_ms <= max_quote_age_ms)
    ]
    if frame.empty:
        raise ValueError("no fresh quotes for requested underlying and expiry")

    strikes = frame["strike"].astype("float64")
    atm_strike = float(strikes.iloc[(strikes - spot).abs().argmin()])
    at = frame[frame["strike"].astype("float64") == atm_strike]
    calls = at[at["option_type"].str.upper() == "C"]
    puts = at[at["option_type"].str.upper() == "P"]
    if calls.empty or puts.empty:
        raise ValueError(f"strike {atm_strike} missing a call or put leg")
    call_ts = calls["_occurred"].max()
    put_ts = puts["_occurred"].max()
    if call_ts != put_ts:
        raise ValueError("call and put quote timestamps do not match")
    calls = calls[calls["_occurred"] == call_ts]
    puts = puts[puts["_occurred"] == put_ts]
    if len(calls) != 1 or len(puts) != 1:
        raise ValueError("duplicate option quote at selected snapshot")
    return StraddleMark(
        underlying=underlying.upper(),
        expiry=target_expiry,
        occurred_at_utc=call_ts,
        strike=atm_strike,
        call_mid=_mid(calls.iloc[0]),
        put_mid=_mid(puts.iloc[0]),
    )
