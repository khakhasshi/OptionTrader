"""Shared fail-closed runtime dependencies for realtime tests."""

from pathlib import Path
from typing import Iterator

import pytest
from pytest import MonkeyPatch

from app.events import EventContextStore
from app.realtime import session


@pytest.fixture(autouse=True)
def _event_store_fixture(monkeypatch: MonkeyPatch) -> Iterator[None]:
    root = Path(__file__).parent / "fixtures" / "events"
    monkeypatch.setattr(session, "_EVENT_STORE", EventContextStore(root))
    yield
