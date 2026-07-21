"""Phase 2 event ingestion, coverage, aggregation, and fail-closed tests."""

from __future__ import annotations

from datetime import datetime, timezone
import glob
import json
from pathlib import Path
from typing import Any, cast

from jsonschema import Draft202012Validator
from fastapi.testclient import TestClient
import pytest
from referencing import Registry, Resource
from pydantic import ValidationError

from app.events import EventContextStore, build_event_context
from app.events.models import (
    EarningsEventsDocument,
    MacroEventsDocument,
    NewsEventsDocument,
    QqqHoldingsDocument,
)
from app import main

_FIXTURES = Path(__file__).parent / "fixtures" / "events"
_DAY = _FIXTURES / "2026-07-20"
_SCHEMAS = Path(__file__).resolve().parents[3] / "packages" / "contracts" / "jsonschema"


def _load(name: str) -> object:
    return json.loads((_DAY / name).read_text())


def _docs() -> tuple[
    MacroEventsDocument,
    QqqHoldingsDocument,
    EarningsEventsDocument,
    NewsEventsDocument,
]:
    return (
        MacroEventsDocument.model_validate(_load("macro_events.json")),
        QqqHoldingsDocument.model_validate(_load("qqq_holdings.json")),
        EarningsEventsDocument.model_validate(_load("qqq_top20_earnings.json")),
        NewsEventsDocument.model_validate(_load("qqq_top20_news_events.json")),
    )


def _validator(name: str) -> Draft202012Validator:
    resources = {
        Path(path).name: Resource.from_contents(json.loads(Path(path).read_text()))
        for path in glob.glob(str(_SCHEMAS / "*.json"))
    }
    registry = Registry().with_resources(list(resources.items()))
    return Draft202012Validator(resources[name].contents, registry=registry)


def test_source_documents_and_built_context_match_json_contracts() -> None:
    for filename in (
        "macro_events.json",
        "qqq_holdings.json",
        "qqq_top20_earnings.json",
        "qqq_top20_news_events.json",
    ):
        assert list(_validator(filename).iter_errors(_load(filename))) == []

    context = build_event_context(datetime(2026, 7, 20, 13, 45, tzinfo=timezone.utc), *_docs())
    payload = context.model_dump(mode="json")
    assert list(_validator("event_context.json").iter_errors(payload)) == []


def test_empty_calendar_still_requires_document_source_proof() -> None:
    raw = cast(dict[str, Any], _load("macro_events.json")).copy()
    raw.pop("source")
    raw["events"] = []

    assert list(_validator("macro_events.json").iter_errors(raw))
    with pytest.raises(ValidationError):
        MacroEventsDocument.model_validate(raw)


def test_context_blocks_short_premium_before_confirmed_fomc() -> None:
    now = datetime(2026, 7, 20, 13, 45, tzinfo=timezone.utc)
    context = build_event_context(now, *_docs())
    assert context.available is True
    assert context.event_day_type == "FOMC"
    assert context.minutes_to_major_event == 15
    assert context.event_released is False
    assert "NO_SHORT_PREMIUM_BEFORE_EVENT" in context.risk_flags
    assert "ELEVATED_EVENT_RISK" in context.risk_flags
    assert context.macro_events[0].source == "Federal Reserve"


def test_context_enters_post_release_wait_window() -> None:
    now = datetime(2026, 7, 20, 14, 5, tzinfo=timezone.utc)
    context = build_event_context(now, *_docs())
    assert context.event_released is True
    assert "WAIT_AFTER_RELEASE" in context.risk_flags


def test_store_missing_file_fails_closed(tmp_path: Path) -> None:
    context = EventContextStore(tmp_path).get(datetime(2026, 7, 20, 13, 45, tzinfo=timezone.utc))
    assert context.available is False
    assert context.event_day_type == "HighRisk"
    assert context.minutes_to_major_event is None
    assert "ELEVATED_EVENT_RISK" in context.risk_flags


def test_store_recomputes_time_sensitive_fields_from_cached_documents() -> None:
    store = EventContextStore(_FIXTURES)
    before = store.get(datetime(2026, 7, 20, 13, 45, tzinfo=timezone.utc))
    after = store.get(datetime(2026, 7, 20, 14, 5, tzinfo=timezone.utc))
    assert before.minutes_to_major_event == 15
    assert before.event_released is False
    assert after.event_released is True


def test_stale_holdings_fail_closed() -> None:
    macro, holdings, earnings, news = _docs()
    stale = holdings.model_copy(update={"as_of_date": "2026-06-01"})
    try:
        build_event_context(
            datetime(2026, 7, 20, 13, 45, tzinfo=timezone.utc),
            macro,
            stale,
            earnings,
            news,
        )
    except ValueError as exc:
        assert "holdings" in str(exc)
    else:
        raise AssertionError("stale holdings must not produce an available context")


def test_future_received_source_fails_closed() -> None:
    macro, holdings, earnings, news = _docs()
    future_event = macro.events[0].model_copy(update={"received_at_utc": "2026-07-20T15:00:00Z"})
    invalid_macro = macro.model_copy(update={"events": [future_event]})
    with pytest.raises(ValueError, match="future"):
        build_event_context(
            datetime(2026, 7, 20, 13, 45, tzinfo=timezone.utc),
            invalid_macro,
            holdings,
            earnings,
            news,
        )


def test_event_context_api_aliases_return_the_same_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = build_event_context(datetime(2026, 7, 20, 13, 45, tzinfo=timezone.utc), *_docs())
    monkeypatch.setattr(main, "current_event_context", lambda: context)
    client = TestClient(main.app)

    canonical = client.get("/api/v1/events/today")
    compatibility = client.get("/api/v1/events/context")

    assert canonical.status_code == 200
    assert canonical.json() == compatibility.json()
    assert list(_validator("event_context.json").iter_errors(canonical.json())) == []
