"""Provider-neutral protocol and OpenAI-compatible JSON-mode implementation."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import json
from time import perf_counter
from typing import Any, Protocol

import httpx
from pydantic import ValidationError

from app.llm.config import LLMSettings
from app.llm.models import (
    MAX_PROVIDER_INPUT_TOKENS,
    MAX_PROVIDER_OUTPUT_TOKENS,
    LLMReviewContent,
    ReviewConstraintViolation,
    UnavailableReason,
)


ContentValidator = Callable[[LLMReviewContent], LLMReviewContent]


@dataclass(frozen=True)
class ProviderCompletion:
    content: LLMReviewContent
    provider_request_id: str | None
    attempts: int
    latency_ms: int
    input_tokens: int
    output_tokens: int


class ReviewProvider(Protocol):
    async def complete(
        self,
        system: str,
        provider_payload: str,
        *,
        validator: ContentValidator | None = None,
    ) -> ProviderCompletion: ...


class ProviderFailure(RuntimeError):
    def __init__(
        self,
        reason_code: UnavailableReason,
        attempts: int,
        latency_ms: int,
        *,
        validation_errors: tuple[str, ...] = (),
        input_tokens: int = 0,
        output_tokens: int = 0,
    ) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code
        self.attempts = attempts
        self.latency_ms = latency_ms
        # Schema paths and validator types only; provider values never cross this boundary.
        self.validation_errors = validation_errors
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens


class OpenAICompatibleProvider:
    def __init__(
        self,
        settings: LLMSettings,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._settings = settings
        self._transport = transport
        self._sleep = sleep

    async def complete(
        self,
        system: str,
        provider_payload: str,
        *,
        validator: ContentValidator | None = None,
    ) -> ProviderCompletion:
        started = perf_counter()
        attempts = 0
        last_reason: UnavailableReason = "PROVIDER_ERROR"
        validation_errors: tuple[str, ...] = ()
        consumed_input_tokens = 0
        consumed_output_tokens = 0
        body: dict[str, Any] = {
            "model": self._settings.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": provider_payload},
            ],
            "temperature": 0,
            "max_tokens": self._settings.max_output_tokens,
            "response_format": {"type": "json_object"},
            "stream": False,
        }
        if self._settings.provider == "deepseek-openai":
            body["thinking"] = {"type": "disabled"}
        headers = {
            "Authorization": f"Bearer {self._settings.api_key}",
            "Content-Type": "application/json",
        }
        timeout = httpx.Timeout(self._settings.timeout_seconds)
        async with httpx.AsyncClient(timeout=timeout, transport=self._transport) as client:
            for attempt in range(1, self._settings.max_retries + 2):
                attempts = attempt
                retry = False
                try:
                    response = await client.post(
                        f"{self._settings.base_url}/chat/completions",
                        headers=headers,
                        json=body,
                    )
                except httpx.TimeoutException:
                    last_reason = "TIMEOUT"
                    retry = True
                except httpx.RequestError:
                    last_reason = "PROVIDER_ERROR"
                    retry = True
                else:
                    if response.status_code == 429:
                        last_reason = "RATE_LIMIT"
                        used_input, used_output = _safe_response_usage(response)
                        consumed_input_tokens += used_input
                        consumed_output_tokens += used_output
                        retry = True
                    elif response.status_code in {500, 502, 503, 504}:
                        last_reason = "PROVIDER_ERROR"
                        used_input, used_output = _safe_response_usage(response)
                        consumed_input_tokens += used_input
                        consumed_output_tokens += used_output
                        retry = True
                    elif response.status_code >= 400:
                        used_input, used_output = _safe_response_usage(response)
                        raise ProviderFailure(
                            "PROVIDER_ERROR",
                            attempts,
                            _elapsed_ms(started),
                            input_tokens=consumed_input_tokens + used_input,
                            output_tokens=consumed_output_tokens + used_output,
                        )
                    else:
                        try:
                            completion = _parse_completion(
                                response,
                                attempts,
                                _elapsed_ms(started),
                                validator=validator,
                            )
                            return ProviderCompletion(
                                content=completion.content,
                                provider_request_id=completion.provider_request_id,
                                attempts=completion.attempts,
                                latency_ms=completion.latency_ms,
                                input_tokens=consumed_input_tokens + completion.input_tokens,
                                output_tokens=consumed_output_tokens + completion.output_tokens,
                            )
                        except ReviewConstraintViolation as exc:
                            last_reason = "INVALID_RESPONSE"
                            validation_errors = (f"request:{exc.code}",)
                            used_input, used_output = _safe_response_usage(response)
                            consumed_input_tokens += used_input
                            consumed_output_tokens += used_output
                            retry = True
                        except ValidationError as exc:
                            last_reason = "INVALID_RESPONSE"
                            validation_errors = _safe_validation_errors(exc)
                            used_input, used_output = _safe_response_usage(response)
                            consumed_input_tokens += used_input
                            consumed_output_tokens += used_output
                            retry = True
                        except (
                            KeyError,
                            TypeError,
                            ValueError,
                            json.JSONDecodeError,
                        ):
                            last_reason = "INVALID_RESPONSE"
                            validation_errors = ()
                            used_input, used_output = _safe_response_usage(response)
                            consumed_input_tokens += used_input
                            consumed_output_tokens += used_output
                            retry = True
                if not retry or attempt > self._settings.max_retries:
                    break
                await self._sleep(min(0.25 * (2 ** (attempt - 1)), 1.0))
        raise ProviderFailure(
            last_reason,
            attempts,
            _elapsed_ms(started),
            validation_errors=validation_errors,
            input_tokens=consumed_input_tokens,
            output_tokens=consumed_output_tokens,
        )


def _parse_completion(
    response: httpx.Response,
    attempts: int,
    latency_ms: int,
    *,
    validator: ContentValidator | None = None,
) -> ProviderCompletion:
    raw = response.json()
    if not isinstance(raw, dict):
        raise TypeError("provider response is not an object")
    choices = raw["choices"]
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        raise TypeError("provider choices are invalid")
    choice = choices[0]
    if choice.get("finish_reason") != "stop":
        raise ValueError("provider response did not finish cleanly")
    message = choice["message"]
    if not isinstance(message, dict):
        raise TypeError("provider message is invalid")
    raw_content = message["content"]
    if not isinstance(raw_content, str) or not raw_content.strip():
        raise ValueError("provider returned empty content")
    parsed_content = json.loads(raw_content)
    if not isinstance(parsed_content, dict):
        raise TypeError("provider content is not a JSON object")
    content = LLMReviewContent.model_validate(parsed_content)
    if validator is not None:
        content = validator(content)
    usage = raw.get("usage", {})
    if not isinstance(usage, dict):
        raise TypeError("provider usage is invalid")
    input_tokens = _bounded_nonnegative_int(
        usage.get("prompt_tokens", 0), MAX_PROVIDER_INPUT_TOKENS
    )
    output_tokens = _bounded_nonnegative_int(
        usage.get("completion_tokens", 0), MAX_PROVIDER_OUTPUT_TOKENS
    )
    provider_request_id = raw.get("id")
    if provider_request_id is not None and not isinstance(provider_request_id, str):
        raise TypeError("provider request id is invalid")
    return ProviderCompletion(
        content=content,
        provider_request_id=provider_request_id,
        attempts=attempts,
        latency_ms=latency_ms,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )


def _bounded_nonnegative_int(value: Any, maximum: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0 or value > maximum:
        raise TypeError("provider token usage is invalid")
    return value


def _safe_validation_errors(exc: ValidationError) -> tuple[str, ...]:
    """Return bounded schema diagnostics without provider-supplied values or messages."""
    diagnostics: list[str] = []
    for error in exc.errors(include_input=False, include_url=False)[:5]:
        location = ".".join(str(part) for part in error.get("loc", ())) or "root"
        error_type = str(error.get("type", "validation_error"))
        diagnostics.append(f"{location}:{error_type}")
    return tuple(diagnostics)


def _safe_response_usage(response: httpx.Response) -> tuple[int, int]:
    try:
        raw = response.json()
        if not isinstance(raw, dict) or not isinstance(raw.get("usage"), dict):
            return 0, 0
        usage = raw["usage"]
        return (
            _bounded_nonnegative_int(usage.get("prompt_tokens", 0), MAX_PROVIDER_INPUT_TOKENS),
            _bounded_nonnegative_int(usage.get("completion_tokens", 0), MAX_PROVIDER_OUTPUT_TOKENS),
        )
    except (TypeError, ValueError, json.JSONDecodeError):
        return 0, 0


def _elapsed_ms(started: float) -> int:
    return max(0, int((perf_counter() - started) * 1000))


__all__ = [
    "ContentValidator",
    "OpenAICompatibleProvider",
    "ProviderCompletion",
    "ProviderFailure",
    "ReviewProvider",
]
