"""Pydantic mirrors of the Phase 2 event JSON contracts."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_validator,
)


def _utc_timestamp(value: str) -> str:
    from datetime import datetime

    if not value.endswith("Z"):
        raise ValueError("UTC timestamp must end in Z")
    datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value


def _iso_date(value: str) -> str:
    from datetime import date

    date.fromisoformat(value)
    return value


UtcTimestamp = Annotated[str, AfterValidator(_utc_timestamp)]
IsoDate = Annotated[str, AfterValidator(_iso_date)]
DecimalString = Annotated[str, StringConstraints(pattern=r"^-?[0-9]+(\.[0-9]+)?$")]
Symbol = Annotated[str, StringConstraints(pattern=r"^[A-Z][A-Z0-9.-]*$")]
Importance = Literal["LOW", "MEDIUM", "HIGH"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class SourceMetadata(StrictModel):
    source: str = Field(min_length=1)
    source_timestamp_utc: UtcTimestamp
    received_at_utc: UtcTimestamp
    confidence: float = Field(ge=0, le=1)
    raw_ref: str = Field(min_length=1)


class MacroEvent(SourceMetadata):
    event_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    event_kind: Literal["FOMC", "CPI", "PCE", "NFP", "GDP", "OTHER"]
    scheduled_at_utc: UtcTimestamp
    importance: Importance
    confirmed: bool


class MacroEventsDocument(SourceMetadata):
    schema_version: Literal["1.0"]
    coverage_start: IsoDate
    coverage_end: IsoDate
    events: list[MacroEvent]

    @model_validator(mode="after")
    def coverage_is_ordered(self) -> MacroEventsDocument:
        if self.coverage_start > self.coverage_end:
            raise ValueError("macro coverage_start is after coverage_end")
        return self


class Holding(StrictModel):
    symbol: Symbol
    name: str = Field(min_length=1)
    weight: DecimalString


class QqqHoldingsDocument(SourceMetadata):
    schema_version: Literal["1.0"]
    as_of_date: IsoDate
    holdings: list[Holding] = Field(min_length=20)

    @model_validator(mode="after")
    def symbols_are_unique(self) -> QqqHoldingsDocument:
        symbols = [holding.symbol for holding in self.holdings]
        if len(symbols) != len(set(symbols)):
            raise ValueError("holding symbols must be unique")
        return self


class EarningsEvent(SourceMetadata):
    event_id: str = Field(min_length=1)
    symbol: Symbol
    scheduled_at_utc: UtcTimestamp
    timing: Literal["BMO", "AMC", "DURING_MARKET", "UNKNOWN"]
    confirmed: bool


class EarningsEventsDocument(SourceMetadata):
    schema_version: Literal["1.0"]
    coverage_start: IsoDate
    coverage_end: IsoDate
    events: list[EarningsEvent]

    @model_validator(mode="after")
    def coverage_is_ordered(self) -> EarningsEventsDocument:
        if self.coverage_start > self.coverage_end:
            raise ValueError("earnings coverage_start is after coverage_end")
        return self


class NewsEvent(SourceMetadata):
    event_id: str = Field(min_length=1)
    symbols: list[Symbol] = Field(min_length=1)
    headline: str = Field(min_length=1, max_length=500)
    event_at_utc: UtcTimestamp
    severity: Importance


class NewsEventsDocument(SourceMetadata):
    schema_version: Literal["1.0"]
    trading_date: IsoDate
    events: list[NewsEvent]


RiskFlag = Literal[
    "NO_SHORT_PREMIUM_BEFORE_EVENT",
    "SIZE_HALF",
    "WAIT_AFTER_RELEASE",
    "ELEVATED_EVENT_RISK",
    "NO_NAKED_0DTE",
]
EventDayType = Literal["Normal", "MacroEvent", "EarningsEvent", "FOMC", "Mixed", "HighRisk"]


class SourceDocument(SourceMetadata):
    category: Literal["macro", "holdings", "earnings", "news"]


class EventContext(StrictModel):
    schema_version: Literal["1.0"]
    event_context_id: str
    trading_date: IsoDate
    generated_at_utc: UtcTimestamp
    available: bool
    source_documents: list[SourceDocument]
    event_day_type: EventDayType
    macro_events: list[MacroEvent]
    earnings_events: list[EarningsEvent]
    news_events: list[NewsEvent]
    qqq_weighted_event_score: DecimalString
    minutes_to_major_event: int | None
    event_released: bool
    risk_flags: list[RiskFlag]
    deterministic_context_summary: str

    @model_validator(mode="after")
    def available_context_has_all_sources(self) -> EventContext:
        categories = [document.category for document in self.source_documents]
        if len(categories) != len(set(categories)):
            raise ValueError("EventContext source document categories must be unique")
        if self.available and set(categories) != {"macro", "holdings", "earnings", "news"}:
            raise ValueError("available EventContext requires all four source documents")
        return self


__all__ = [
    "EarningsEventsDocument",
    "EventDayType",
    "EventContext",
    "MacroEventsDocument",
    "NewsEventsDocument",
    "QqqHoldingsDocument",
    "RiskFlag",
    "SourceDocument",
]
