from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from app.llm.config import LLMSettings, load_llm_env_file
from app.llm.models import (
    LLMReviewRequest,
    ReviewContext,
    SourceReference,
    validate_content_for_request,
)
from app.llm.prompt import system_prompt
from app.llm.provider import (
    ContentValidator,
    OpenAICompatibleProvider,
    ProviderCompletion,
    ProviderFailure,
)
from app.llm.security import prepare_provider_payload
from app.llm.service import LLMReviewService


@pytest.mark.skipif(
    os.getenv("OPTIONTRADER_RUN_LLM_LIVE_SMOKE") != "true",
    reason="explicit LLM live-smoke opt-in is required",
)
def test_deepseek_live_json_review_without_secret_output() -> None:
    root = Path(__file__).resolve().parents[3]
    settings = LLMSettings.from_env(load_llm_env_file(root / ".env"))
    assert settings.configured
    request = LLMReviewRequest(
        schema_version="1.0",
        request_id="llm_live_smoke_20260722",
        correlation_id="session_live_smoke",
        causation_id=None,
        session_id="session_live_smoke",
        occurred_at_utc="2026-07-22T12:00:00Z",
        received_at_utc="2026-07-22T12:00:01Z",
        source="application-service",
        source_sequence=1,
        rule_version="rules_phase4_smoke",
        stage="PRE_MARKET",
        trading_date="2026-07-22",
        plan_id=None,
        plan_hash=None,
        context=ReviewContext(
            event_context={"event_day_type": "Normal", "risk_flags": []},
            active_playbook={"strategy": "NoTrade"},
            deterministic_summary="No major event is present in this synthetic smoke fixture.",
        ),
        source_refs=[
            SourceReference(
                source_id="synthetic_event_context",
                source_type="event_context",
                source="live-smoke-fixture",
                occurred_at_utc="2026-07-22T12:00:00Z",
                raw_ref="fixture:llm-live-smoke",
                confidence=1.0,
            )
        ],
    )
    payload, _ = prepare_provider_payload(request, settings.max_input_chars)
    provider = OpenAICompatibleProvider(settings)
    try:
        completion = asyncio.run(
            provider.complete(
                system_prompt(request.stage),
                payload,
                validator=lambda content: validate_content_for_request(content, request),
            )
        )
    except ProviderFailure as exc:
        pytest.fail(
            f"provider failure: {exc.reason_code}; schema={','.join(exc.validation_errors)}"
        )

    class FixedProvider:
        async def complete(
            self,
            _system: str,
            _payload: str,
            *,
            validator: ContentValidator | None = None,
        ) -> ProviderCompletion:
            if validator is None:
                return completion
            return ProviderCompletion(
                content=validator(completion.content),
                provider_request_id=completion.provider_request_id,
                attempts=completion.attempts,
                latency_ms=completion.latency_ms,
                input_tokens=completion.input_tokens,
                output_tokens=completion.output_tokens,
            )

    review = asyncio.run(LLMReviewService(settings, provider=FixedProvider()).review(request))
    assert review.review_status == "COMPLETED", review.unavailable_reason_code
    assert review.provider.model == settings.model
    assert review.recommended_action != "Proceed"
    assert review.provider.input_tokens > 0
