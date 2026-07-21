from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from app.llm.config import LLMSettings
from app.llm.models import LLMReviewContent
from app.llm.provider import OpenAICompatibleProvider, ProviderFailure


def _settings(**overrides: str) -> LLMSettings:
    values = {
        "LLM_PROVIDER": "deepseek-openai",
        "LLM_BASE_URL": "https://api.deepseek.com",
        "LLM_API_KEY": "provider-test-key",
        "LLM_MODEL": "deepseek-v4-flash",
        "LLM_MAX_RETRIES": "2",
    }
    values.update(overrides)
    settings = LLMSettings.from_env(values)
    assert settings.configured
    return settings


def _content() -> dict[str, object]:
    return {
        "summary": "ok",
        "decision_support": "advisory only",
        "sop_alignment": "Unknown",
        "risk_notes": [],
        "invalidations": [],
        "recommended_action": "Review Only",
        "confidence": 0.5,
        "rule_references": [],
        "evidence_citations": [],
        "daily_review": None,
        "rule_hypotheses": [],
    }


def _response(request: httpx.Request, status: int = 200) -> httpx.Response:
    return httpx.Response(
        status,
        request=request,
        json={
            "id": "deepseek-test-id",
            "choices": [
                {
                    "finish_reason": "stop",
                    "message": {"role": "assistant", "content": json.dumps(_content())},
                }
            ],
            "usage": {"prompt_tokens": 120, "completion_tokens": 40},
        },
    )


def test_deepseek_openai_request_uses_json_mode_without_tools() -> None:
    observed: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        observed["path"] = request.url.path
        observed["authorization"] = request.headers.get("Authorization")
        observed["body"] = json.loads(request.content)
        return _response(request)

    provider = OpenAICompatibleProvider(_settings(), transport=httpx.MockTransport(handler))
    completion = asyncio.run(provider.complete("return JSON", '{"safe":"data"}'))
    body = observed["body"]
    assert isinstance(body, dict)
    assert observed["path"] == "/chat/completions"
    assert observed["authorization"] == "Bearer provider-test-key"
    assert body["response_format"] == {"type": "json_object"}
    assert body["thinking"] == {"type": "disabled"}
    assert "tools" not in body
    assert completion.content.recommended_action == "Review Only"
    assert completion.input_tokens == 120


def test_rate_limit_retries_with_bound_and_then_succeeds() -> None:
    calls = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, request=request, json={"error": "rate limited"})
        return _response(request)

    async def no_sleep(delay: float) -> None:
        sleeps.append(delay)

    provider = OpenAICompatibleProvider(
        _settings(), transport=httpx.MockTransport(handler), sleep=no_sleep
    )
    completion = asyncio.run(provider.complete("return JSON", "{}"))
    assert completion.attempts == 2
    assert calls == 2
    assert sleeps == [0.25]


def test_authentication_failure_is_not_retried_or_leaked() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(401, request=request, json={"error": "do-not-surface-body"})

    provider = OpenAICompatibleProvider(_settings(), transport=httpx.MockTransport(handler))
    with pytest.raises(ProviderFailure) as raised:
        asyncio.run(provider.complete("return JSON", "{}"))
    assert raised.value.reason_code == "PROVIDER_ERROR"
    assert raised.value.attempts == 1
    assert "do-not-surface-body" not in str(raised.value)
    assert calls == 1


def test_empty_json_mode_content_retries_only_to_configured_bound() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            request=request,
            json={
                "id": "empty",
                "choices": [{"finish_reason": "stop", "message": {"content": ""}}],
                "usage": {},
            },
        )

    async def no_sleep(_delay: float) -> None:
        return None

    provider = OpenAICompatibleProvider(
        _settings(LLM_MAX_RETRIES="1"),
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )
    with pytest.raises(ProviderFailure) as raised:
        asyncio.run(provider.complete("return JSON", "{}"))
    assert raised.value.reason_code == "INVALID_RESPONSE"
    assert raised.value.attempts == 2
    assert calls == 2


def test_schema_diagnostics_never_include_provider_values() -> None:
    secret_marker = "provider-secret-marker"

    def handler(request: httpx.Request) -> httpx.Response:
        content = _content()
        content["confidence"] = secret_marker
        return httpx.Response(
            200,
            request=request,
            json={
                "id": "invalid-schema",
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {"content": json.dumps(content)},
                    }
                ],
                "usage": {},
            },
        )

    provider = OpenAICompatibleProvider(
        _settings(LLM_MAX_RETRIES="0"), transport=httpx.MockTransport(handler)
    )
    with pytest.raises(ProviderFailure) as raised:
        asyncio.run(provider.complete("return JSON", "{}"))
    assert raised.value.validation_errors == ("confidence:float_type",)
    assert secret_marker not in repr(raised.value.validation_errors)
    assert secret_marker not in str(raised.value)


def test_request_validator_retries_and_accounts_for_rejected_completion() -> None:
    calls = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return _response(request)

    async def no_sleep(delay: float) -> None:
        sleeps.append(delay)

    validations = 0

    def validator(content: LLMReviewContent) -> LLMReviewContent:
        nonlocal validations
        validations += 1
        if validations == 1:
            raise ValueError("stage field conflict")
        return content

    provider = OpenAICompatibleProvider(
        _settings(LLM_MAX_RETRIES="1"),
        transport=httpx.MockTransport(handler),
        sleep=no_sleep,
    )
    completion = asyncio.run(provider.complete("return JSON", "{}", validator=validator))
    assert calls == 2
    assert sleeps == [0.25]
    assert completion.attempts == 2
    assert completion.input_tokens == 240
    assert completion.output_tokens == 80
