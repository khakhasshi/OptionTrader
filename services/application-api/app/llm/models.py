"""Strict contracts at the untrusted LLM boundary."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_validator,
)

from app.events.models import EventContext
from app.trading.models import CandidateTradePlan, RiskDecision


def _utc_z(value: str) -> str:
    if not value.endswith("Z"):
        raise ValueError("UTC timestamp must end in Z")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    offset = parsed.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise ValueError("timestamp must be UTC")
    return value


def _iso_date(value: str) -> str:
    date.fromisoformat(value)
    return value


UtcTimestamp = Annotated[str, AfterValidator(_utc_z)]
IsoDate = Annotated[str, AfterValidator(_iso_date)]
Hash = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
DecimalString = Annotated[str, StringConstraints(pattern=r"^-?[0-9]+(\.[0-9]+)?$")]


def _nonnegative_decimal(value: DecimalString) -> DecimalString:
    if Decimal(value) < 0:
        raise ValueError("decimal must be nonnegative")
    return value


NonnegativeDecimalString = Annotated[DecimalString, AfterValidator(_nonnegative_decimal)]
ReviewStage = Literal[
    "POST_MARKET",
    "PRE_MARKET",
    "INTRADAY",
    "PRE_EXECUTION",
    "RULE_HYPOTHESIS",
]
UnavailableReason = Literal[
    "CONFIG_MISSING",
    "TIMEOUT",
    "RATE_LIMIT",
    "PROVIDER_ERROR",
    "INVALID_RESPONSE",
    "INPUT_REJECTED",
    "INITIAL_RISK_REQUIRED",
    "BUDGET_EXCEEDED",
]


class ReviewConstraintViolation(ValueError):
    """Safe cross-document validation error with no provider-supplied values."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class SourceReference(StrictModel):
    source_id: str = Field(min_length=1, max_length=160)
    source_type: Literal[
        "market_snapshot",
        "option_snapshot",
        "event_context",
        "signal",
        "trade",
        "candidate_trade_plan",
        "risk_decision",
        "broker_snapshot",
        "session_metrics",
    ]
    source: str = Field(min_length=1, max_length=120)
    occurred_at_utc: UtcTimestamp
    raw_ref: str = Field(min_length=1, max_length=500)
    confidence: float = Field(ge=0, le=1)


class ReviewContext(StrictModel):
    market_snapshot: dict[str, Any] | None = None
    option_snapshot: dict[str, Any] | None = None
    regime_state: dict[str, Any] | None = None
    vol_state: dict[str, Any] | None = None
    risk_state: dict[str, Any] | None = None
    data_health: dict[str, Any] | None = None
    broker_health: dict[str, Any] | None = None
    active_playbook: dict[str, Any] | None = None
    candidate_trade_plan: CandidateTradePlan | dict[str, Any] | None = None
    initial_risk_decision: RiskDecision | dict[str, Any] | None = None
    event_context: EventContext | dict[str, Any] | None = None
    recent_signals: list[dict[str, Any]] = Field(default_factory=list, max_length=20)
    recent_trades: list[dict[str, Any]] = Field(default_factory=list, max_length=50)
    session_metrics: dict[str, Any] | None = None
    deterministic_summary: str | None = Field(default=None, max_length=4000)

    @model_validator(mode="after")
    def contains_structured_evidence(self) -> ReviewContext:
        values = self.model_dump(exclude={"recent_signals", "recent_trades"})
        if not any(value is not None for value in values.values()):
            if not self.recent_signals and not self.recent_trades:
                raise ValueError("review context cannot be empty")
        return self


class LLMReviewRequest(StrictModel):
    schema_version: Literal["1.0"]
    request_id: str = Field(min_length=1, max_length=160)
    correlation_id: str = Field(min_length=1, max_length=160)
    causation_id: str | None = Field(default=None, max_length=160)
    session_id: str = Field(min_length=1, max_length=160)
    occurred_at_utc: UtcTimestamp
    received_at_utc: UtcTimestamp
    source: Literal["application-service"]
    source_sequence: int = Field(ge=0)
    rule_version: str = Field(min_length=1, max_length=160)
    stage: ReviewStage
    trading_date: IsoDate | None = None
    plan_id: str | None = Field(default=None, max_length=160)
    plan_hash: Hash | None = None
    context: ReviewContext
    source_refs: list[SourceReference] = Field(default_factory=list, max_length=100)

    @model_validator(mode="after")
    def stage_requirements_are_met(self) -> LLMReviewRequest:
        occurred = datetime.fromisoformat(self.occurred_at_utc.replace("Z", "+00:00"))
        received = datetime.fromisoformat(self.received_at_utc.replace("Z", "+00:00"))
        if received < occurred:
            raise ValueError("review received_at_utc cannot precede occurred_at_utc")
        source_ids = [source.source_id for source in self.source_refs]
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("review source ids must be unique")
        if self.stage == "PRE_EXECUTION":
            if not self.plan_id or not self.plan_hash:
                raise ValueError("pre-execution review requires plan identity")
            if self.context.candidate_trade_plan is None:
                raise ValueError("pre-execution review requires candidate plan context")
            if self.context.initial_risk_decision is None:
                raise ValueError("pre-execution review requires Initial Risk context")
        elif self.plan_id is not None or self.plan_hash is not None:
            if not self.plan_id or not self.plan_hash:
                raise ValueError("plan_id and plan_hash must be supplied together")
        if self.stage == "POST_MARKET" and self.trading_date is None:
            raise ValueError("post-market review requires trading_date")
        return self


class EvidenceCitation(StrictModel):
    source_id: str = Field(min_length=1, max_length=160)
    claim: str = Field(min_length=1, max_length=500)


class LossAttribution(StrictModel):
    kind: Literal["DIRECTION", "IV", "THETA", "SLIPPAGE", "EXECUTION_ERROR", "OTHER"]
    explanation: str = Field(min_length=1, max_length=500)
    evidence_ids: list[str] = Field(default_factory=list, max_length=10)


class DailyReviewDetail(StrictModel):
    best_trade: str | None = Field(default=None, max_length=500)
    worst_trade: str | None = Field(default=None, max_length=500)
    good_losses: list[str] = Field(default_factory=list, max_length=10)
    bad_losses: list[str] = Field(default_factory=list, max_length=10)
    sop_violations: list[str] = Field(default_factory=list, max_length=20)
    loss_attribution: list[LossAttribution] = Field(default_factory=list, max_length=10)
    one_change_tomorrow: str = Field(min_length=1, max_length=500)


class RuleHypothesis(StrictModel):
    title: str = Field(min_length=1, max_length=200)
    rationale: str = Field(min_length=1, max_length=1000)
    validation_plan: str = Field(min_length=1, max_length=1000)
    evidence_ids: list[str] = Field(default_factory=list, max_length=20)
    status: Literal["RESEARCH_ONLY"]
    activation_allowed: Literal[False]


class RuleHypothesisRecord(StrictModel):
    hypothesis_id: str = Field(min_length=1, max_length=160)
    review_id: str = Field(min_length=1, max_length=160)
    session_id: str | None = Field(default=None, max_length=160)
    trading_date: IsoDate | None = None
    status: Literal["PENDING_RESEARCH", "VALIDATING", "REJECTED", "APPROVED_FOR_SHADOW"]
    activation_allowed: Literal[False]
    payload: RuleHypothesis


class LLMReviewContent(StrictModel):
    summary: str = Field(max_length=2000)
    decision_support: str = Field(max_length=2000)
    sop_alignment: Literal["Aligned", "Conflict", "Unknown"]
    risk_notes: list[str] = Field(default_factory=list, max_length=20)
    invalidations: list[str] = Field(default_factory=list, max_length=20)
    recommended_action: Literal["Proceed", "Wait", "Cancel", "Reduce Risk", "Review Only"]
    confidence: float = Field(ge=0, le=1)
    rule_references: list[str] = Field(default_factory=list, max_length=20)
    evidence_citations: list[EvidenceCitation] = Field(default_factory=list, max_length=30)
    daily_review: DailyReviewDetail | None = None
    rule_hypotheses: list[RuleHypothesis] = Field(default_factory=list, max_length=5)

    @model_validator(mode="after")
    def lists_are_unique(self) -> LLMReviewContent:
        for name, values in (
            ("risk_notes", self.risk_notes),
            ("invalidations", self.invalidations),
            ("rule_references", self.rule_references),
        ):
            if len(values) != len(set(values)):
                raise ValueError(f"{name} must not contain duplicates")
        return self


class ProviderMetadata(StrictModel):
    provider: str = Field(min_length=1, max_length=80)
    model: str = Field(min_length=1, max_length=120)
    provider_request_id: str | None = Field(default=None, max_length=200)
    prompt_version: str = Field(min_length=1, max_length=80)
    input_hash: Hash
    latency_ms: int = Field(ge=0)
    attempts: int = Field(ge=0, le=4)
    cache_hit: bool
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    estimated_cost_usd: NonnegativeDecimalString


class LLMReview(StrictModel):
    schema_version: Literal["1.0"]
    review_id: str = Field(min_length=1, max_length=160)
    request_id: str = Field(min_length=1, max_length=160)
    correlation_id: str = Field(min_length=1, max_length=160)
    causation_id: str | None = Field(default=None, max_length=160)
    session_id: str = Field(min_length=1, max_length=160)
    occurred_at_utc: UtcTimestamp
    received_at_utc: UtcTimestamp
    source: Literal["llm-intelligence-layer"]
    source_sequence: int = Field(ge=0)
    rule_version: str = Field(min_length=1, max_length=160)
    stage: ReviewStage
    trading_date: IsoDate | None = None
    plan_id: str | None = Field(default=None, max_length=160)
    plan_hash: Hash | None = None
    review_status: Literal["COMPLETED", "UNAVAILABLE", "INVALID"]
    summary: str = Field(max_length=2000)
    decision_support: str = Field(max_length=2000)
    sop_alignment: Literal["Aligned", "Conflict", "Unknown"]
    risk_notes: list[str] = Field(default_factory=list, max_length=20)
    invalidations: list[str] = Field(default_factory=list, max_length=20)
    recommended_action: Literal["Proceed", "Wait", "Cancel", "Reduce Risk", "Review Only"]
    confidence: float = Field(ge=0, le=1)
    rule_references: list[str] = Field(default_factory=list, max_length=20)
    evidence_citations: list[EvidenceCitation] = Field(default_factory=list, max_length=30)
    daily_review: DailyReviewDetail | None = None
    rule_hypotheses: list[RuleHypothesis] = Field(default_factory=list, max_length=5)
    unavailable_reason_code: UnavailableReason | None = None
    provider: ProviderMetadata
    source_refs: list[SourceReference] = Field(default_factory=list, max_length=100)

    @model_validator(mode="after")
    def status_and_stage_are_consistent(self) -> LLMReview:
        if self.review_status == "COMPLETED":
            if self.unavailable_reason_code is not None:
                raise ValueError("completed review cannot carry an unavailable reason")
        elif (
            self.unavailable_reason_code is None
            or self.recommended_action != "Review Only"
            or self.confidence != 0
        ):
            raise ValueError("non-completed review must be inert and explain why")
        if self.review_status != "COMPLETED":
            if self.daily_review is not None or self.rule_hypotheses:
                raise ValueError("non-completed review cannot carry generated artifacts")
        elif self.stage == "POST_MARKET":
            if self.trading_date is None or self.daily_review is None:
                raise ValueError("completed post-market review requires date and detail")
        elif self.daily_review is not None:
            raise ValueError("daily review detail is only valid for completed post-market review")
        if self.review_status == "COMPLETED" and self.stage == "RULE_HYPOTHESIS":
            if not self.rule_hypotheses:
                raise ValueError("completed rule review requires a research hypothesis")
        elif self.stage != "POST_MARKET" and self.rule_hypotheses:
            raise ValueError("rule hypotheses are forbidden for this review stage")
        if self.recommended_action == "Proceed" and self.stage != "PRE_EXECUTION":
            raise ValueError("Proceed is restricted to pre-execution review")
        if self.stage == "PRE_EXECUTION" and (not self.plan_id or not self.plan_hash):
            raise ValueError("pre-execution review requires plan identity")
        if (self.plan_id is None) != (self.plan_hash is None):
            raise ValueError("review plan identity must be supplied as a pair")
        source_ids = [source.source_id for source in self.source_refs]
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("review source ids must be unique")
        known_sources = set(source_ids)
        cited_sources = {citation.source_id for citation in self.evidence_citations}
        cited_sources.update(
            source_id
            for hypothesis in self.rule_hypotheses
            for source_id in hypothesis.evidence_ids
        )
        if self.daily_review is not None:
            cited_sources.update(
                source_id
                for attribution in self.daily_review.loss_attribution
                for source_id in attribution.evidence_ids
            )
        if cited_sources - known_sources:
            raise ValueError("review artifacts cite unknown source ids")
        for name, values in (
            ("risk_notes", self.risk_notes),
            ("invalidations", self.invalidations),
            ("rule_references", self.rule_references),
        ):
            if len(values) != len(set(values)):
                raise ValueError(f"{name} must not contain duplicates")
        return self


def validate_content_for_request(
    content: LLMReviewContent, request: LLMReviewRequest
) -> LLMReviewContent:
    """Apply deterministic cross-document checks after provider validation."""
    known_sources = {source.source_id for source in request.source_refs}
    cited = {citation.source_id for citation in content.evidence_citations}
    hypothesis_evidence = {
        source_id for hypothesis in content.rule_hypotheses for source_id in hypothesis.evidence_ids
    }
    daily_evidence = {
        source_id
        for item in (content.daily_review.loss_attribution if content.daily_review else [])
        for source_id in item.evidence_ids
    }
    unknown = (cited | hypothesis_evidence | daily_evidence) - known_sources
    if unknown:
        raise ReviewConstraintViolation("UNKNOWN_SOURCE_CITATION")
    if request.stage == "POST_MARKET":
        if content.daily_review is None:
            raise ReviewConstraintViolation("POST_MARKET_DETAIL_REQUIRED")
    elif content.daily_review is not None:
        raise ReviewConstraintViolation("DAILY_REVIEW_FORBIDDEN_FOR_STAGE")
    if request.stage == "RULE_HYPOTHESIS":
        if not content.rule_hypotheses:
            raise ReviewConstraintViolation("RULE_HYPOTHESIS_REQUIRED")
    elif request.stage != "POST_MARKET" and content.rule_hypotheses:
        raise ReviewConstraintViolation("RULE_HYPOTHESIS_FORBIDDEN_FOR_STAGE")
    if content.recommended_action == "Proceed" and request.stage != "PRE_EXECUTION":
        raise ReviewConstraintViolation("PROCEED_FORBIDDEN_FOR_STAGE")
    return content


__all__ = [
    "DailyReviewDetail",
    "EvidenceCitation",
    "LLMReview",
    "LLMReviewContent",
    "LLMReviewRequest",
    "ProviderMetadata",
    "ReviewContext",
    "ReviewConstraintViolation",
    "ReviewStage",
    "RuleHypothesis",
    "RuleHypothesisRecord",
    "SourceReference",
    "UnavailableReason",
    "validate_content_for_request",
]
