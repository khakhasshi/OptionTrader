from __future__ import annotations

import json
from pathlib import Path

from app.llm.evaluation import evaluate_reviews, load_evaluation_cases
from app.llm.models import LLMReview


_ROOT = Path(__file__).resolve().parents[3]
_CASES = _ROOT / "data/llm_eval/phase4_adversarial.json"
_FIXTURES = _ROOT / "packages/contracts/fixtures"


def _review_for_case(
    request_id: str,
    stage: str,
    status: str,
    alignment: str,
    *,
    reason: str | None = None,
) -> LLMReview:
    raw = json.loads((_FIXTURES / "llm_review.unavailable.json").read_text())
    raw.update(
        {
            "review_id": f"review_{request_id}",
            "request_id": request_id,
            "stage": stage,
            "review_status": status,
            "sop_alignment": alignment,
            "unavailable_reason_code": reason,
        }
    )
    if stage == "PRE_EXECUTION":
        raw["plan_id"] = "eval_plan_conflict"
        raw["plan_hash"] = "a" * 64
    if status == "COMPLETED":
        raw.update(
            {
                "summary": "evaluated",
                "decision_support": "advisory",
                "recommended_action": "Review Only",
                "confidence": 0.5,
                "unavailable_reason_code": None,
            }
        )
    return LLMReview.model_validate(raw)


def test_adversarial_evaluation_set_is_valid_and_metrics_expose_misses() -> None:
    cases = load_evaluation_cases(_CASES)
    assert {case.category for case in cases} == {
        "ALIGNED_BASELINE",
        "SOP_CONFLICT",
        "MISSING_CONTEXT",
        "PROMPT_INJECTION",
        "PROVIDER_UNAVAILABLE",
    }
    reviews: dict[str, LLMReview | None] = {}
    for case in cases:
        if case.category == "PROMPT_INJECTION":
            reviews[case.request.request_id] = _review_for_case(
                case.request.request_id,
                case.request.stage,
                "INVALID",
                "Unknown",
                reason="INPUT_REJECTED",
            )
        elif case.category == "PROVIDER_UNAVAILABLE":
            reviews[case.request.request_id] = _review_for_case(
                case.request.request_id,
                case.request.stage,
                "UNAVAILABLE",
                "Unknown",
                reason="TIMEOUT",
            )
        else:
            reviews[case.request.request_id] = _review_for_case(
                case.request.request_id,
                case.request.stage,
                "COMPLETED",
                case.expected.sop_alignment,
            )
    report = evaluate_reviews(cases, reviews)
    assert report.case_count == 5
    assert report.structured_output_success_rate == 1
    assert report.conflict_recall == 1
    assert report.conflict_false_positive_rate == 0
    assert report.injection_block_rate == 1
    assert report.unavailable_inert_rate == 1
    assert report.missed_risk_case_ids == []
    assert report.expectation_mismatch_case_ids == []

    conflict = next(case for case in cases if case.category == "SOP_CONFLICT")
    reviews[conflict.request.request_id] = _review_for_case(
        conflict.request.request_id, conflict.request.stage, "COMPLETED", "Aligned"
    )
    degraded = evaluate_reviews(cases, reviews)
    assert degraded.conflict_recall == 0
    assert degraded.missed_risk_case_ids == [conflict.case_id]
    assert degraded.expectation_mismatch_case_ids == [conflict.case_id]


def test_missing_observation_is_a_contract_failure_not_a_silent_pass() -> None:
    cases = load_evaluation_cases(_CASES)
    report = evaluate_reviews(cases, {})
    assert report.structured_output_success_rate == 0
    assert len(report.contract_failure_case_ids) == len(cases)
