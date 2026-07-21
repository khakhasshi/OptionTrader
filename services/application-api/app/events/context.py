"""Deterministically combine four sourced documents into one EventContext."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from math import ceil
from typing import Literal, cast
from zoneinfo import ZoneInfo

from app.events.models import (
    EarningsEventsDocument,
    EventDayType,
    EventContext,
    MacroEventsDocument,
    NewsEventsDocument,
    QqqHoldingsDocument,
    RiskFlag,
    SourceDocument,
    SourceMetadata,
)

_ET = ZoneInfo("America/New_York")
_NO_EVENT_HORIZON_MINUTES = 24 * 60
_HOLDINGS_MAX_AGE_DAYS = 14
_POST_RELEASE_WAIT_MINUTES = 15
_DOCUMENT_MIN_CONFIDENCE = Decimal("0.8")
_HOLDINGS_MIN_CONFIDENCE = Decimal("0.9")
_CRITICAL_EVENT_MIN_CONFIDENCE = Decimal("0.8")
_FLAG_ORDER = (
    "NO_SHORT_PREMIUM_BEFORE_EVENT",
    "WAIT_AFTER_RELEASE",
    "ELEVATED_EVENT_RISK",
    "SIZE_HALF",
    "NO_NAKED_0DTE",
)


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _utc_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _context_id(trading_date: date, generated_at: datetime) -> str:
    stamp = generated_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"evtctx_{trading_date.isoformat()}_{stamp}"


def unavailable_event_context(now_utc: datetime, reason: str) -> EventContext:
    """Canonical fail-closed context for missing, stale, or invalid source data."""
    trading_date = now_utc.astimezone(_ET).date()
    return EventContext(
        schema_version="1.0",
        event_context_id=_context_id(trading_date, now_utc),
        trading_date=trading_date.isoformat(),
        generated_at_utc=_utc_z(now_utc),
        available=False,
        source_documents=[],
        event_day_type="HighRisk",
        macro_events=[],
        earnings_events=[],
        news_events=[],
        qqq_weighted_event_score="1.0000",
        minutes_to_major_event=None,
        event_released=False,
        risk_flags=["ELEVATED_EVENT_RISK", "NO_NAKED_0DTE"],
        deterministic_context_summary=f"Event context unavailable: {reason}",
    )


def _require_coverage(target: date, start: str, end: str, label: str) -> None:
    if not date.fromisoformat(start) <= target <= date.fromisoformat(end):
        raise ValueError(f"{label} does not cover {target.isoformat()}")


def _require_scheduled_within_coverage(
    scheduled_at_utc: str, start: str, end: str, label: str
) -> None:
    scheduled_date = _parse_utc(scheduled_at_utc).astimezone(_ET).date()
    if not date.fromisoformat(start) <= scheduled_date <= date.fromisoformat(end):
        raise ValueError(f"{label} falls outside its document coverage")


def _require_sane_source(item: SourceMetadata, now_utc: datetime, label: str) -> None:
    source_at = _parse_utc(item.source_timestamp_utc)
    received_at = _parse_utc(item.received_at_utc)
    if source_at > received_at:
        raise ValueError(f"{label} source timestamp is after receipt")
    if received_at > now_utc + timedelta(minutes=5):
        raise ValueError(f"{label} receipt timestamp is in the future")


def _source_document(
    category: Literal["macro", "holdings", "earnings", "news"], document: SourceMetadata
) -> SourceDocument:
    return SourceDocument(
        category=category,
        source=document.source,
        source_timestamp_utc=document.source_timestamp_utc,
        received_at_utc=document.received_at_utc,
        confidence=document.confidence,
        raw_ref=document.raw_ref,
    )


def build_event_context(
    now_utc: datetime,
    macro: MacroEventsDocument,
    holdings: QqqHoldingsDocument,
    earnings: EarningsEventsDocument,
    news: NewsEventsDocument,
) -> EventContext:
    """Build an auditable event state. Raises when source coverage is insufficient."""
    if now_utc.tzinfo is None:
        raise ValueError("now_utc must be timezone-aware")
    now_utc = now_utc.astimezone(timezone.utc)
    trading_date = now_utc.astimezone(_ET).date()
    _require_coverage(trading_date, macro.coverage_start, macro.coverage_end, "macro calendar")
    _require_coverage(
        trading_date, earnings.coverage_start, earnings.coverage_end, "earnings calendar"
    )
    if news.trading_date != trading_date.isoformat():
        raise ValueError(f"news document does not cover {trading_date.isoformat()}")
    holdings_age = (trading_date - date.fromisoformat(holdings.as_of_date)).days
    if holdings_age < 0 or holdings_age > _HOLDINGS_MAX_AGE_DAYS:
        raise ValueError(f"QQQ holdings are stale or future-dated: age={holdings_age} days")
    _require_sane_source(holdings, now_utc, "QQQ holdings")
    _require_sane_source(macro, now_utc, "macro calendar document")
    _require_sane_source(earnings, now_utc, "earnings calendar document")
    _require_sane_source(news, now_utc, "news document")
    if Decimal(str(holdings.confidence)) < _HOLDINGS_MIN_CONFIDENCE:
        raise ValueError("QQQ holdings confidence is below 0.9")
    for document, label in (
        (macro, "macro calendar document"),
        (earnings, "earnings calendar document"),
        (news, "news document"),
    ):
        if Decimal(str(document.confidence)) < _DOCUMENT_MIN_CONFIDENCE:
            raise ValueError(f"{label} confidence is below 0.8")
    for index, macro_item in enumerate(macro.events):
        _require_sane_source(macro_item, now_utc, f"macro event {index}")
        _require_scheduled_within_coverage(
            macro_item.scheduled_at_utc,
            macro.coverage_start,
            macro.coverage_end,
            f"macro event {index}",
        )
    for index, earnings_item in enumerate(earnings.events):
        _require_sane_source(earnings_item, now_utc, f"earnings event {index}")
        _require_scheduled_within_coverage(
            earnings_item.scheduled_at_utc,
            earnings.coverage_start,
            earnings.coverage_end,
            f"earnings event {index}",
        )
    for index, news_item in enumerate(news.events):
        _require_sane_source(news_item, now_utc, f"news event {index}")
        event_at = _parse_utc(news_item.event_at_utc)
        if event_at > now_utc + timedelta(minutes=5):
            raise ValueError(f"news event {index} timestamp is in the future")
        if event_at.astimezone(_ET).date() != trading_date:
            raise ValueError(f"news event {index} falls outside its document trading date")

    top_holdings = sorted(holdings.holdings, key=lambda item: Decimal(item.weight), reverse=True)[
        :20
    ]
    weights = {item.symbol: Decimal(item.weight) for item in top_holdings}

    macro_today = [
        event
        for event in macro.events
        if _parse_utc(event.scheduled_at_utc).astimezone(_ET).date() == trading_date
    ]
    earnings_today = [
        event
        for event in earnings.events
        if event.symbol in weights
        and _parse_utc(event.scheduled_at_utc).astimezone(_ET).date() == trading_date
    ]
    news_today = [
        event for event in news.events if any(symbol in weights for symbol in event.symbols)
    ]
    low_confidence_critical = any(
        Decimal(str(event.confidence)) < _CRITICAL_EVENT_MIN_CONFIDENCE
        for event in [*macro_today, *earnings_today, *news_today]
    )
    if low_confidence_critical:
        raise ValueError("critical event confidence is below 0.8")

    major_instants: list[datetime] = []
    for macro_event in macro_today:
        if macro_event.importance == "HIGH":
            major_instants.append(_parse_utc(macro_event.scheduled_at_utc))
    for earnings_event in earnings_today:
        if weights[earnings_event.symbol] >= Decimal("0.01"):
            major_instants.append(_parse_utc(earnings_event.scheduled_at_utc))

    future = sorted(instant for instant in major_instants if instant >= now_utc)
    past = sorted((instant for instant in major_instants if instant < now_utc), reverse=True)
    minutes_to_major = (
        ceil((future[0] - now_utc).total_seconds() / 60) if future else _NO_EVENT_HORIZON_MINUTES
    )
    event_released = bool(
        past and 0 <= (now_utc - past[0]).total_seconds() <= _POST_RELEASE_WAIT_MINUTES * 60
    )

    score = Decimal("0")
    score += sum(
        Decimal("0.30") if event.importance == "HIGH" else Decimal("0.10")
        for event in macro_today
        if event.importance != "LOW"
    )
    score += sum(
        weights[event.symbol] * (Decimal("1.5") if not event.confirmed else Decimal("1"))
        for event in earnings_today
    )
    severity = {"LOW": Decimal("0.25"), "MEDIUM": Decimal("0.75"), "HIGH": Decimal("1.5")}
    for news_event in news_today:
        affected = max(
            (weights.get(symbol, Decimal("0")) for symbol in news_event.symbols),
            default=Decimal("0"),
        )
        score += affected * severity[news_event.severity]
    score = min(score, Decimal("1"))

    unconfirmed = any(
        not event.confirmed or event.confidence < 0.8 for event in macro_today
    ) or any(not event.confirmed or event.confidence < 0.8 for event in earnings_today)
    low_confidence_news = any(event.confidence < 0.8 for event in news_today)
    unconfirmed = unconfirmed or low_confidence_news
    has_macro = bool(macro_today)
    has_earnings = bool(earnings_today)
    if unconfirmed or score >= Decimal("0.50"):
        day_type: EventDayType = "HighRisk"
    elif any(event.event_kind == "FOMC" for event in macro_today):
        day_type = "FOMC"
    elif has_macro and has_earnings:
        day_type = "Mixed"
    elif has_macro:
        day_type = "MacroEvent"
    elif has_earnings:
        day_type = "EarningsEvent"
    else:
        day_type = "Normal"

    flags: set[str] = {"NO_NAKED_0DTE"}
    if 0 <= minutes_to_major <= 30:
        flags.add("NO_SHORT_PREMIUM_BEFORE_EVENT")
    if event_released:
        flags.add("WAIT_AFTER_RELEASE")
    if unconfirmed or score >= Decimal("0.25"):
        flags.add("ELEVATED_EVENT_RISK")
    if score >= Decimal("0.10"):
        flags.add("SIZE_HALF")
    ordered_flags = [flag for flag in _FLAG_ORDER if flag in flags]

    next_text = str(minutes_to_major) if future else "none_today"
    summary = (
        f"day={day_type}; macro={len(macro_today)}; earnings={len(earnings_today)}; "
        f"news={len(news_today)}; next_major_minutes={next_text}; score={score:.4f}"
    )
    return EventContext(
        schema_version="1.0",
        event_context_id=_context_id(trading_date, now_utc),
        trading_date=trading_date.isoformat(),
        generated_at_utc=_utc_z(now_utc),
        available=True,
        source_documents=[
            _source_document("macro", macro),
            _source_document("holdings", holdings),
            _source_document("earnings", earnings),
            _source_document("news", news),
        ],
        event_day_type=day_type,
        macro_events=macro_today,
        earnings_events=earnings_today,
        news_events=news_today,
        qqq_weighted_event_score=f"{score:.4f}",
        minutes_to_major_event=minutes_to_major,
        event_released=event_released,
        risk_flags=cast(list[RiskFlag], ordered_flags),
        deterministic_context_summary=summary,
    )
