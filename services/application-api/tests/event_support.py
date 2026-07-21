"""Deterministic available EventContext fixture helpers."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from zoneinfo import ZoneInfo

from app.events import EventContext
from app.events.models import SourceDocument

_ET = ZoneInfo("America/New_York")


def _source_document(
    category: Literal["macro", "holdings", "earnings", "news"], at_utc: str
) -> SourceDocument:
    return SourceDocument(
        category=category,
        source="test-source",
        source_timestamp_utc=at_utc,
        received_at_utc=at_utc,
        confidence=1.0,
        raw_ref=f"fixture://{category}",
    )


def available_event_context(at_utc: str) -> dict[str, Any]:
    instant = datetime.fromisoformat(at_utc.replace("Z", "+00:00"))
    trading_date = instant.astimezone(_ET).date().isoformat()
    return EventContext(
        schema_version="1.0",
        event_context_id=f"evtctx_{trading_date}_test",
        trading_date=trading_date,
        generated_at_utc=at_utc,
        available=True,
        source_documents=[
            _source_document("macro", at_utc),
            _source_document("holdings", at_utc),
            _source_document("earnings", at_utc),
            _source_document("news", at_utc),
        ],
        event_day_type="Normal",
        macro_events=[],
        earnings_events=[],
        news_events=[],
        qqq_weighted_event_score="0.0000",
        minutes_to_major_event=1440,
        event_released=False,
        risk_flags=["NO_NAKED_0DTE"],
        deterministic_context_summary="day=Normal; fixture",
    ).model_dump(mode="json")
