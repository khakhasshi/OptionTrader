from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient
import pytest
from sqlalchemy.exc import SQLAlchemyError

from app import main
from app.llm.models import LLMReview, LLMReviewRequest
from app.llm.security import review_input_hash
from app.llm.service import InFlightReviewConflict
from app.trading.models import CandidateTradePlan, RiskDecision


_ROOT = Path(__file__).resolve().parents[3]
_FIXTURES = _ROOT / "packages/contracts/fixtures"


def _json(name: str) -> dict[str, Any]:
    value = json.loads((_FIXTURES / name).read_text())
    assert isinstance(value, dict)
    return value


def _plan_and_risk() -> tuple[CandidateTradePlan, RiskDecision]:
    return (
        CandidateTradePlan.model_validate(_json("candidate_trade_plan.sample.json")),
        RiskDecision.model_validate(_json("risk_decision.sample.json")),
    )


class CapturingService:
    def __init__(self) -> None:
        self.calls = 0
        self.request: LLMReviewRequest | None = None
        self.initial_risk_verified = False

    async def review(
        self, request: LLMReviewRequest, *, initial_risk_verified: bool = False
    ) -> LLMReview:
        self.calls += 1
        self.request = request
        self.initial_risk_verified = initial_risk_verified
        raw = _json("llm_review.completed.json")
        raw.update(
            {
                "request_id": request.request_id,
                "correlation_id": request.correlation_id,
                "causation_id": request.causation_id,
                "session_id": request.session_id,
                "occurred_at_utc": request.occurred_at_utc,
                "source_sequence": request.source_sequence,
                "rule_version": request.rule_version,
                "stage": request.stage,
                "trading_date": request.trading_date,
                "plan_id": request.plan_id,
                "plan_hash": request.plan_hash,
                "source_refs": [source.model_dump(mode="json") for source in request.source_refs],
            }
        )
        raw["provider"]["input_hash"] = review_input_hash(request, 60_000)
        return LLMReview.model_validate(raw)


def test_pre_execution_review_replaces_caller_context_with_durable_facts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, risk = _plan_and_risk()
    service = CapturingService()
    persisted: dict[str, object] = {}
    monkeypatch.setattr(main, "_require_execution_engine", lambda: object())
    monkeypatch.setattr(main, "assert_review_store_available", lambda _engine: None)
    monkeypatch.setattr(main, "get_llm_review_by_request_id", lambda *_args: None)
    monkeypatch.setattr(main, "verified_initial_risk_context", lambda *_args: (plan, risk))
    monkeypatch.setattr(main, "_llm_review_service", lambda: service)

    def persist(_engine: object, request: LLMReviewRequest, review: LLMReview) -> bool:
        persisted["request"] = request
        persisted["review"] = review
        return True

    monkeypatch.setattr(main, "persist_llm_review", persist)
    response = TestClient(main.app).post(
        "/api/v1/llm/reviews", json=_json("llm_review_request.sample.json")
    )
    assert response.status_code == 200
    assert service.calls == 1
    assert service.initial_risk_verified is True
    assert service.request is not None
    assert service.request.context.candidate_trade_plan == plan
    assert service.request.context.initial_risk_decision == risk
    assert service.request.causation_id == risk.decision_id
    assert {source.source_id for source in service.request.source_refs} == {
        plan.plan_id,
        risk.decision_id,
    }
    assert persisted["request"] is service.request
    assert response.json()["recommended_action"] == "Proceed"


def test_pre_execution_without_durable_initial_approval_never_calls_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = CapturingService()
    monkeypatch.setattr(main, "_require_execution_engine", lambda: object())
    monkeypatch.setattr(main, "assert_review_store_available", lambda _engine: None)
    monkeypatch.setattr(main, "get_llm_review_by_request_id", lambda *_args: None)
    monkeypatch.setattr(main, "verified_initial_risk_context", lambda *_args: None)
    monkeypatch.setattr(main, "_llm_review_service", lambda: service)
    response = TestClient(main.app).post(
        "/api/v1/llm/reviews", json=_json("llm_review_request.sample.json")
    )
    assert response.status_code == 409
    assert response.json()["detail"] == "initial_risk_approval_required"
    assert service.calls == 0


def test_unavailable_audit_store_blocks_provider_spend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = CapturingService()
    raw = _json("llm_review_request.sample.json")
    raw.update({"stage": "PRE_MARKET", "plan_id": None, "plan_hash": None})
    raw["context"]["candidate_trade_plan"] = None
    raw["context"]["initial_risk_decision"] = None
    monkeypatch.setattr(main, "_require_execution_engine", lambda: object())
    monkeypatch.setattr(
        main,
        "assert_review_store_available",
        lambda _engine: (_ for _ in ()).throw(SQLAlchemyError("offline")),
    )
    monkeypatch.setattr(main, "get_llm_review_by_request_id", lambda *_args: None)
    monkeypatch.setattr(main, "_llm_review_service", lambda: service)
    response = TestClient(main.app).post("/api/v1/llm/reviews", json=raw)
    assert response.status_code == 503
    assert response.json()["detail"] == "llm_review_audit_failed"
    assert service.calls == 0


def test_llm_status_never_exposes_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LLM_PROVIDER", "deepseek-openai")
    monkeypatch.setenv("LLM_BASE_URL", "https://api.deepseek.com")
    monkeypatch.setenv("LLM_API_KEY", "status-test-secret-key")
    monkeypatch.setenv("LLM_MODEL", "deepseek-v4-flash")
    response = TestClient(main.app).get("/api/v1/llm/status")
    assert response.status_code == 200
    assert response.json() == {
        "configured": True,
        "provider": "deepseek-openai",
        "model": "deepseek-v4-flash",
        "trading_authority": "NONE",
    }
    assert "status-test-secret-key" not in response.text


def test_daily_review_read_is_strictly_read_only(monkeypatch: pytest.MonkeyPatch) -> None:
    review = LLMReview.model_validate(_json("llm_review.completed.json"))
    monkeypatch.setattr(main, "_require_execution_engine", lambda: object())
    monkeypatch.setattr(main, "latest_daily_review", lambda *_args: review)
    response = TestClient(main.app).get("/api/v1/llm/daily-reviews/latest")
    assert response.status_code == 200
    assert response.json()["review_id"] == review.review_id


def test_repeated_request_returns_exact_persisted_review_without_provider_spend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = _json("llm_review_request.sample.json")
    raw.update({"stage": "PRE_MARKET", "plan_id": None, "plan_hash": None})
    raw["context"]["candidate_trade_plan"] = None
    raw["context"]["initial_risk_decision"] = None
    request = LLMReviewRequest.model_validate(raw)
    service = CapturingService()
    completed_raw = _json("llm_review.completed.json")
    completed_raw.update(
        {
            "request_id": request.request_id,
            "correlation_id": request.correlation_id,
            "causation_id": request.causation_id,
            "session_id": request.session_id,
            "occurred_at_utc": request.occurred_at_utc,
            "source_sequence": request.source_sequence,
            "rule_version": request.rule_version,
            "stage": request.stage,
            "trading_date": request.trading_date,
            "plan_id": None,
            "plan_hash": None,
            "recommended_action": "Review Only",
            "source_refs": [source.model_dump(mode="json") for source in request.source_refs],
        }
    )
    completed_raw["provider"]["input_hash"] = review_input_hash(request, 60_000)
    existing = LLMReview.model_validate(completed_raw)
    monkeypatch.setattr(main, "_require_execution_engine", lambda: object())
    monkeypatch.setattr(main, "assert_review_store_available", lambda _engine: None)
    monkeypatch.setattr(main, "get_llm_review_by_request_id", lambda *_args: existing)
    monkeypatch.setattr(main, "_llm_review_service", lambda: service)
    monkeypatch.setattr(
        main,
        "persist_llm_review",
        lambda *_args: pytest.fail("persist must not run for an exact retry"),
    )
    response = TestClient(main.app).post("/api/v1/llm/reviews", json=raw)
    assert response.status_code == 200
    assert response.json()["review_id"] == existing.review_id
    assert service.calls == 0


def test_reused_request_id_with_changed_input_fails_before_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = _json("llm_review_request.sample.json")
    raw.update({"stage": "PRE_MARKET", "plan_id": None, "plan_hash": None})
    raw["context"]["candidate_trade_plan"] = None
    raw["context"]["initial_risk_decision"] = None
    original = LLMReviewRequest.model_validate(raw)
    completed_raw = _json("llm_review.completed.json")
    completed_raw.update(
        {
            "request_id": original.request_id,
            "correlation_id": original.correlation_id,
            "causation_id": original.causation_id,
            "session_id": original.session_id,
            "occurred_at_utc": original.occurred_at_utc,
            "source_sequence": original.source_sequence,
            "rule_version": original.rule_version,
            "stage": original.stage,
            "trading_date": original.trading_date,
            "plan_id": None,
            "plan_hash": None,
            "recommended_action": "Review Only",
            "source_refs": [source.model_dump(mode="json") for source in original.source_refs],
        }
    )
    completed_raw["provider"]["input_hash"] = review_input_hash(original, 60_000)
    existing = LLMReview.model_validate(completed_raw)
    raw["context"]["deterministic_summary"] = "Changed context under a reused request id."
    service = CapturingService()
    monkeypatch.setattr(main, "_require_execution_engine", lambda: object())
    monkeypatch.setattr(main, "assert_review_store_available", lambda _engine: None)
    monkeypatch.setattr(main, "get_llm_review_by_request_id", lambda *_args: existing)
    monkeypatch.setattr(main, "_llm_review_service", lambda: service)
    response = TestClient(main.app).post("/api/v1/llm/reviews", json=raw)
    assert response.status_code == 409
    assert response.json()["detail"] == "llm_request_id_conflict"
    assert service.calls == 0


def test_inflight_request_identity_conflict_maps_to_http_409(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ConflictService:
        async def review(
            self, _request: LLMReviewRequest, *, initial_risk_verified: bool = False
        ) -> LLMReview:
            del initial_risk_verified
            raise InFlightReviewConflict("conflicting in-flight input")

    raw = _json("llm_review_request.sample.json")
    raw.update({"stage": "PRE_MARKET", "plan_id": None, "plan_hash": None})
    raw["context"]["candidate_trade_plan"] = None
    raw["context"]["initial_risk_decision"] = None
    monkeypatch.setattr(main, "_require_execution_engine", lambda: object())
    monkeypatch.setattr(main, "assert_review_store_available", lambda _engine: None)
    monkeypatch.setattr(main, "get_llm_review_by_request_id", lambda *_args: None)
    monkeypatch.setattr(main, "_llm_review_service", ConflictService)
    monkeypatch.setattr(
        main,
        "persist_llm_review",
        lambda *_args: pytest.fail("conflicting request must not be persisted"),
    )
    response = TestClient(main.app).post("/api/v1/llm/reviews", json=raw)
    assert response.status_code == 409
    assert response.json()["detail"] == "llm_request_id_conflict"
