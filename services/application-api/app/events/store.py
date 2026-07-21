"""File-backed EventContext store with strict parsing and fail-closed fallback."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic import ValidationError

from app.events.context import build_event_context, unavailable_event_context
from app.events.models import (
    EarningsEventsDocument,
    EventContext,
    MacroEventsDocument,
    NewsEventsDocument,
    QqqHoldingsDocument,
)

_FILES = (
    "macro_events.json",
    "qqq_holdings.json",
    "qqq_top20_earnings.json",
    "qqq_top20_news_events.json",
)
_ET = ZoneInfo("America/New_York")


class EventContextStore:
    """Load one date directory and rebuild when any source file changes."""

    def __init__(self, root: Path) -> None:
        self._root = root
        self._cache_key: tuple[str, tuple[int, ...]] | None = None
        self._cache_docs: (
            tuple[
                MacroEventsDocument,
                QqqHoldingsDocument,
                EarningsEventsDocument,
                NewsEventsDocument,
            ]
            | None
        ) = None

    def get(self, now_utc: datetime) -> EventContext:
        trading_date = now_utc.astimezone(_ET).date().isoformat()
        # Directory names are exchange dates. The builder performs authoritative
        # America/New_York coverage validation after parsing.
        date_dir = self._root / trading_date
        paths = tuple(date_dir / name for name in _FILES)
        try:
            signature = tuple(path.stat().st_mtime_ns for path in paths)
            key = (trading_date, signature)
            if key != self._cache_key or self._cache_docs is None:
                docs = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
                self._cache_docs = (
                    MacroEventsDocument.model_validate(docs[0]),
                    QqqHoldingsDocument.model_validate(docs[1]),
                    EarningsEventsDocument.model_validate(docs[2]),
                    NewsEventsDocument.model_validate(docs[3]),
                )
                self._cache_key = key
            context = build_event_context(now_utc, *self._cache_docs)
        except (OSError, json.JSONDecodeError, ValidationError, ValueError) as exc:
            return unavailable_event_context(now_utc, f"{type(exc).__name__}: {exc}")
        return context


__all__ = ["EventContextStore"]
