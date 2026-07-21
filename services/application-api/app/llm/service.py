"""Advisory review orchestration with inert failures, cache and cost bounds."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, ROUND_UP
from hashlib import sha256
from time import monotonic
from typing import Callable, Literal

from app.llm.config import LLMSettings
from app.llm.models import (
    LLMReview,
    LLMReviewContent,
    LLMReviewRequest,
    ProviderMetadata,
    UnavailableReason,
    validate_content_for_request,
)
from app.llm.prompt import PROMPT_VERSION, system_prompt
from app.llm.provider import (
    OpenAICompatibleProvider,
    ProviderCompletion,
    ProviderFailure,
    ReviewProvider,
)
from app.llm.security import UnsafeReviewInput, prepare_provider_payload, review_input_hash


@dataclass(frozen=True)
class _CacheEntry:
    content: LLMReviewContent
    provider_request_id: str | None
    expires_at: float


class _DailyBudget:
    def __init__(self, settings: LLMSettings) -> None:
        self._settings = settings
        self._lock = asyncio.Lock()
        self._date = datetime.now(UTC).date()
        self._requests = 0
        self._reserved = Decimal("0")

    async def reserve(self, prompt_and_payload: str) -> Decimal | None:
        estimate = _estimated_cost(
            max(1, (len(prompt_and_payload) + 3) // 4),
            self._settings.max_output_tokens,
            self._settings,
        ) * Decimal(self._settings.max_retries + 1)
        async with self._lock:
            today = datetime.now(UTC).date()
            if today != self._date:
                self._date = today
                self._requests = 0
                self._reserved = Decimal("0")
            if self._requests >= self._settings.daily_max_requests:
                return None
            if self._reserved + estimate > self._settings.daily_max_estimated_usd:
                return None
            self._requests += 1
            self._reserved += estimate
        return estimate


class LLMReviewService:
    def __init__(
        self,
        settings: LLMSettings,
        provider: ReviewProvider | None = None,
        *,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._settings = settings
        self._provider = provider or OpenAICompatibleProvider(settings)
        self._now = now
        self._cache: dict[str, _CacheEntry] = {}
        self._cache_lock = asyncio.Lock()
        self._budget = _DailyBudget(settings)
        self._semaphore = asyncio.Semaphore(settings.max_concurrency)

    async def review(
        self,
        request: LLMReviewRequest,
        *,
        initial_risk_verified: bool = False,
    ) -> LLMReview:
        try:
            provider_payload, input_hash = prepare_provider_payload(
                request, self._settings.max_input_chars
            )
        except UnsafeReviewInput:
            input_hash = review_input_hash(request, self._settings.max_input_chars)
            return self._inert(request, input_hash, "INPUT_REJECTED", status="INVALID")

        if request.stage == "PRE_EXECUTION" and not initial_risk_verified:
            return self._inert(request, input_hash, "INITIAL_RISK_REQUIRED", status="INVALID")
        if not self._settings.configured:
            return self._inert(request, input_hash, "CONFIG_MISSING")

        cache_key = self._cache_key(request, input_hash)
        cached = await self._cache_get(cache_key)
        if cached is not None:
            metadata = ProviderMetadata(
                provider=self._settings.provider,
                model=self._settings.model,
                provider_request_id=cached.provider_request_id,
                prompt_version=PROMPT_VERSION,
                input_hash=input_hash,
                latency_ms=0,
                attempts=0,
                cache_hit=True,
                input_tokens=0,
                output_tokens=0,
                estimated_cost_usd="0",
            )
            return self._completed(request, cached.content, metadata)

        prompt = system_prompt(request.stage)
        reserved = await self._budget.reserve(f"{prompt}\n{provider_payload}")
        if reserved is None:
            return self._inert(request, input_hash, "BUDGET_EXCEEDED")
        try:
            async with self._semaphore:
                completion = await self._provider.complete(
                    prompt,
                    provider_payload,
                    validator=lambda content: validate_content_for_request(content, request),
                )
            content = completion.content
        except ProviderFailure as exc:
            return self._inert(
                request,
                input_hash,
                exc.reason_code,
                attempts=exc.attempts,
                latency_ms=exc.latency_ms,
                estimated_cost=reserved,
            )
        await self._cache_put(cache_key, content, completion.provider_request_id)
        metadata = self._metadata(input_hash, completion)
        return self._completed(request, content, metadata)

    async def _cache_get(self, key: str) -> _CacheEntry | None:
        now = monotonic()
        async with self._cache_lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            if entry.expires_at <= now:
                del self._cache[key]
                return None
            return entry

    async def _cache_put(
        self, key: str, content: LLMReviewContent, provider_request_id: str | None
    ) -> None:
        if self._settings.cache_ttl_seconds <= 0:
            return
        async with self._cache_lock:
            self._cache[key] = _CacheEntry(
                content=content,
                provider_request_id=provider_request_id,
                expires_at=monotonic() + self._settings.cache_ttl_seconds,
            )
            if len(self._cache) > 1_000:
                oldest = min(self._cache, key=lambda item: self._cache[item].expires_at)
                del self._cache[oldest]

    def _cache_key(self, request: LLMReviewRequest, input_hash: str) -> str:
        return ":".join(
            (
                self._settings.provider,
                self._settings.model,
                PROMPT_VERSION,
                request.rule_version,
                request.stage,
                input_hash,
            )
        )

    def _metadata(self, input_hash: str, completion: ProviderCompletion) -> ProviderMetadata:
        return ProviderMetadata(
            provider=self._settings.provider,
            model=self._settings.model,
            provider_request_id=completion.provider_request_id,
            prompt_version=PROMPT_VERSION,
            input_hash=input_hash,
            latency_ms=completion.latency_ms,
            attempts=completion.attempts,
            cache_hit=False,
            input_tokens=completion.input_tokens,
            output_tokens=completion.output_tokens,
            estimated_cost_usd=_decimal_text(
                _estimated_cost(
                    completion.input_tokens,
                    completion.output_tokens,
                    self._settings,
                )
            ),
        )

    def _completed(
        self,
        request: LLMReviewRequest,
        content: LLMReviewContent,
        metadata: ProviderMetadata,
    ) -> LLMReview:
        return LLMReview.model_validate(
            {
                **self._envelope(request, metadata.input_hash),
                "review_status": "COMPLETED",
                **content.model_dump(mode="python"),
                "unavailable_reason_code": None,
                "provider": metadata.model_dump(mode="python"),
                "source_refs": [source.model_dump(mode="python") for source in request.source_refs],
            }
        )

    def _inert(
        self,
        request: LLMReviewRequest,
        input_hash: str,
        reason: UnavailableReason,
        *,
        status: Literal["UNAVAILABLE", "INVALID"] = "UNAVAILABLE",
        attempts: int = 0,
        latency_ms: int = 0,
        estimated_cost: Decimal = Decimal("0"),
    ) -> LLMReview:
        metadata = ProviderMetadata(
            provider=self._settings.provider,
            model=self._settings.model,
            provider_request_id=None,
            prompt_version=PROMPT_VERSION,
            input_hash=input_hash,
            latency_ms=latency_ms,
            attempts=attempts,
            cache_hit=False,
            input_tokens=0,
            output_tokens=0,
            estimated_cost_usd=_decimal_text(estimated_cost),
        )
        return LLMReview.model_validate(
            {
                **self._envelope(request, input_hash),
                "review_status": status,
                "summary": "",
                "decision_support": (
                    "LLM review unavailable; deterministic trading flow remains unchanged."
                ),
                "sop_alignment": "Unknown",
                "risk_notes": [],
                "invalidations": [],
                "recommended_action": "Review Only",
                "confidence": 0,
                "rule_references": [],
                "evidence_citations": [],
                "daily_review": None,
                "rule_hypotheses": [],
                "unavailable_reason_code": reason,
                "provider": metadata.model_dump(mode="python"),
                "source_refs": [source.model_dump(mode="python") for source in request.source_refs],
            }
        )

    def _envelope(self, request: LLMReviewRequest, input_hash: str) -> dict[str, object]:
        review_seed = ":".join(
            (request.request_id, input_hash, PROMPT_VERSION, self._settings.model)
        )
        return {
            "schema_version": "1.0",
            "review_id": f"llm_{sha256(review_seed.encode('utf-8')).hexdigest()[:32]}",
            "request_id": request.request_id,
            "correlation_id": request.correlation_id,
            "causation_id": request.causation_id,
            "session_id": request.session_id,
            "occurred_at_utc": request.occurred_at_utc,
            "received_at_utc": _utc_z(self._now()),
            "source": "llm-intelligence-layer",
            "source_sequence": request.source_sequence,
            "rule_version": request.rule_version,
            "stage": request.stage,
            "trading_date": request.trading_date,
            "plan_id": request.plan_id,
            "plan_hash": request.plan_hash,
        }


def _estimated_cost(input_tokens: int, output_tokens: int, settings: LLMSettings) -> Decimal:
    million = Decimal(1_000_000)
    return (
        Decimal(input_tokens) * settings.input_cost_per_million_usd
        + Decimal(output_tokens) * settings.output_cost_per_million_usd
    ) / million


def _decimal_text(value: Decimal) -> str:
    if value == 0:
        return "0"
    quantized = value.quantize(Decimal("0.0000000001"), rounding=ROUND_UP)
    return format(quantized.normalize(), "f")


def _utc_z(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("LLM service clock must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


__all__ = ["LLMReviewService"]
