"""Opt-in credential/entitlement smoke for the official direct Python SDK."""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pytest

from app.thetadata_sdk.service import ThetaDataBarSource, create_sdk_client


@pytest.mark.skipif(
    not os.getenv("THETADATA_CREDENTIALS_FILE"),
    reason="set THETADATA_CREDENTIALS_FILE to run the live ThetaData SDK smoke",
)
def test_live_sdk_can_fetch_qqq_standard_ohlc() -> None:
    credentials = Path(os.environ["THETADATA_CREDENTIALS_FILE"])
    assert credentials.is_file()
    bars = ThetaDataBarSource(create_sdk_client()).fetch(
        symbol="QQQ",
        venue="nqb",
        session_date=date(2026, 7, 20),
        start_minute=570,
        end_minute=572,
    )
    assert bars
    assert bars[0].minute_et == 570
    assert bars[0].occurred_at_utc.endswith("Z")
