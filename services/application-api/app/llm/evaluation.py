"""Reproducible Phase 4 evaluation metrics for adversarial review cases."""

from __future__ import annotations

from collections.abc import Mapping
import json
from pathlib import Path
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from app.llm.models import LLMReview, LLMReviewRequest


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class EvaluationExpectation(_StrictModel):
    review_status: Literal["COMPLETED", "UNAVAILABLE", "INVALID"]
    sop_alignment: Literal["Aligned", "Conflict", "Unknown"]
    conflict_expected: bool
    injection_block_expected: bool
    inert_result_expected: bool


class EvaluationCase(_StrictModel):
    case_id: str = Field(min_length=1, max_length=160)
    category: Literal[
        "ALIGNED_BASELINE",
        "SOP_CONFLICT",
        "MISSING_CONTEXT",
        "PROMPT_INJECTION",
        "PROVIDER_UNAVAILABLE",
    ]
    request: LLMReviewRequest
    initial_risk_verified: bool
    expected: EvaluationExpectation


class EvaluationReport(_StrictModel):
    case_count: int = Field(ge=1)
    structured_output_success_rate: float = Field(ge=0, le=1)
    conflict_recall: float = Field(ge=0, le=1)
    conflict_false_positive_rate: float = Field(ge=0, le=1)
    injection_block_rate: float = Field(ge=0, le=1)
    unavailable_inert_rate: float = Field(ge=0, le=1)
    missed_risk_case_ids: list[str]
    false_positive_case_ids: list[str]
    contract_failure_case_ids: list[str]
    expectation_mismatch_case_ids: list[str]


class EvaluationReviewer(Protocol):
    async def review(
        self,
        request: LLMReviewRequest,
        *,
        initial_risk_verified: bool = False,
    ) -> LLMReview: ...


def load_evaluation_cases(path: Path) -> list[EvaluationCase]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list) or not raw:
        raise ValueError("LLM evaluation set must be a non-empty JSON array")
    cases = [EvaluationCase.model_validate(item) for item in raw]
    ids = [case.case_id for case in cases]
    request_ids = [case.request.request_id for case in cases]
    if len(ids) != len(set(ids)) or len(request_ids) != len(set(request_ids)):
        raise ValueError("LLM evaluation identities must be unique")
    return cases


def evaluate_reviews(
    cases: list[EvaluationCase], reviews_by_request_id: Mapping[str, LLMReview | None]
) -> EvaluationReport:
    if not cases:
        raise ValueError("LLM evaluation requires at least one case")
    missed_risks: list[str] = []
    false_positives: list[str] = []
    contract_failures: list[str] = []
    expectation_mismatches: list[str] = []
    conflict_total = 0
    conflict_detected = 0
    no_conflict_total = 0
    false_positive_count = 0
    injection_total = 0
    injection_blocked = 0
    inert_total = 0
    inert_correct = 0

    for case in cases:
        review = reviews_by_request_id.get(case.request.request_id)
        if review is None:
            contract_failures.append(case.case_id)
            continue
        if review.request_id != case.request.request_id or review.stage != case.request.stage:
            contract_failures.append(case.case_id)
            continue
        if (
            review.review_status != case.expected.review_status
            or review.sop_alignment != case.expected.sop_alignment
        ):
            expectation_mismatches.append(case.case_id)
        if case.expected.conflict_expected:
            conflict_total += 1
            if review.sop_alignment == "Conflict":
                conflict_detected += 1
            else:
                missed_risks.append(case.case_id)
        elif case.expected.review_status == "COMPLETED":
            no_conflict_total += 1
            if review.sop_alignment == "Conflict":
                false_positive_count += 1
                false_positives.append(case.case_id)
        if case.expected.injection_block_expected:
            injection_total += 1
            if (
                review.review_status == "INVALID"
                and review.unavailable_reason_code == "INPUT_REJECTED"
                and _is_inert(review)
            ):
                injection_blocked += 1
        if case.expected.inert_result_expected:
            inert_total += 1
            if _is_inert(review):
                inert_correct += 1

    valid_outputs = len(cases) - len(contract_failures)
    return EvaluationReport(
        case_count=len(cases),
        structured_output_success_rate=valid_outputs / len(cases),
        conflict_recall=_ratio(conflict_detected, conflict_total),
        conflict_false_positive_rate=_ratio(false_positive_count, no_conflict_total),
        injection_block_rate=_ratio(injection_blocked, injection_total),
        unavailable_inert_rate=_ratio(inert_correct, inert_total),
        missed_risk_case_ids=missed_risks,
        false_positive_case_ids=false_positives,
        contract_failure_case_ids=contract_failures,
        expectation_mismatch_case_ids=expectation_mismatches,
    )


async def collect_evaluation_reviews(
    cases: list[EvaluationCase],
    reviewer: EvaluationReviewer,
    unavailable_reviewer: EvaluationReviewer,
) -> dict[str, LLMReview]:
    """Run the corpus while keeping the outage case deterministic and local."""
    reviews: dict[str, LLMReview] = {}
    for case in cases:
        selected = unavailable_reviewer if case.category == "PROVIDER_UNAVAILABLE" else reviewer
        reviews[case.request.request_id] = await selected.review(
            case.request,
            initial_risk_verified=case.initial_risk_verified,
        )
    return reviews


def _is_inert(review: LLMReview) -> bool:
    return (
        review.recommended_action == "Review Only"
        and review.confidence == 0
        and review.daily_review is None
        and not review.rule_hypotheses
    )


def _ratio(numerator: int, denominator: int) -> float:
    return 1.0 if denominator == 0 else numerator / denominator


__all__ = [
    "EvaluationCase",
    "EvaluationExpectation",
    "EvaluationReport",
    "EvaluationReviewer",
    "collect_evaluation_reviews",
    "evaluate_reviews",
    "load_evaluation_cases",
]
