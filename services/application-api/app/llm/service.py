"""Advisory review orchestration with inert failures, cache and cost bounds."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, ROUND_UP
from hashlib import sha256
from time import monotonic
from typing import Callable, Literal, cast

from app.llm.config import LLMSettings
from app.llm.coordinator import (
    CoordinationConflict,
    PostgresReviewCoordinator,
    coordination_identity,
)
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


@dataclass(frozen=True)
class _ProviderOutcome:
    completion: ProviderCompletion | None
    unavailable_reason: UnavailableReason | None
    attempts: int
    latency_ms: int
    input_tokens: int
    output_tokens: int


@dataclass(frozen=True)
class _InFlightReview:
    cache_key: str
    task: asyncio.Task[_ProviderOutcome]


class InFlightReviewConflict(ValueError):
    """The same request identity is already running with different input."""


class ReviewCoordinationUnavailable(ValueError):
    """A global request lease could not reach a safe terminal result in time."""


class _DailyBudget:
    def __init__(self, settings: LLMSettings) -> None:
        self._settings = settings
        self._lock = asyncio.Lock()
        self._date = datetime.now(UTC).date()
        self._requests = 0
        self._reserved = Decimal("0")

    async def reserve(self, prompt_and_payload: str) -> Decimal | None:
        estimate = _reservation_estimate(prompt_and_payload, self._settings)
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
        coordinator: PostgresReviewCoordinator | None = None,
    ) -> None:
        self._settings = settings
        self._provider = provider or OpenAICompatibleProvider(settings)
        self._now = now
        self._coordinator = coordinator
        self._cache: dict[str, _CacheEntry] = {}
        self._cache_lock = asyncio.Lock()
        self._inflight: dict[str, _InFlightReview] = {}
        self._inflight_lock = asyncio.Lock()
        self._budget = _DailyBudget(settings)
        self._semaphore = asyncio.Semaphore(settings.max_concurrency)

    @property
    def settings(self) -> LLMSettings:
        return self._settings

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
        if self._coordinator is not None:
            return await self._coordinated_review(request, input_hash, cache_key, provider_payload)
        cached = await self._cache_get(cache_key)
        if cached is not None:
            return self._cached_review(request, input_hash, cached)

        outcome = await self._singleflight_provider(
            request.request_id, cache_key, request, provider_payload
        )
        return self._review_from_outcome(request, input_hash, outcome)

    async def _coordinated_review(
        self,
        request: LLMReviewRequest,
        input_hash: str,
        cache_key: str,
        provider_payload: str,
    ) -> LLMReview:
        assert self._coordinator is not None
        identity_hash = coordination_identity(cache_key)
        prompt_and_payload = f"{system_prompt(request.stage)}\n{provider_payload}"
        estimate = _reservation_estimate(prompt_and_payload, self._settings)
        deadline = monotonic() + self._coordinator.wait_timeout_seconds
        while True:
            try:
                claim = await asyncio.to_thread(
                    self._coordinator.claim,
                    request.request_id,
                    identity_hash,
                    estimate,
                )
            except CoordinationConflict as exc:
                raise InFlightReviewConflict(
                    "request id has a different globally coordinated input"
                ) from exc
            if claim.role == "COMPLETED":
                if claim.review is None:
                    raise ReviewCoordinationUnavailable("completed coordination has no result")
                return claim.review
            if claim.role == "WAIT":
                if monotonic() >= deadline:
                    raise ReviewCoordinationUnavailable("global review lease wait timed out")
                await asyncio.sleep(self._coordinator.poll_interval_seconds)
                continue
            if claim.role == "INERT":
                reason = cast(
                    UnavailableReason,
                    claim.failure_code or "COORDINATION_RECOVERY",
                )
                review = self._inert(request, input_hash, reason)
                await asyncio.to_thread(
                    self._coordinator.complete,
                    request.request_id,
                    identity_hash,
                    review,
                )
                return review
            if claim.role != "LEADER":
                raise ReviewCoordinationUnavailable("global review claim role is invalid")
            return await self._run_global_leader(
                request, input_hash, identity_hash, cache_key, provider_payload
            )

    async def _run_global_leader(
        self,
        request: LLMReviewRequest,
        input_hash: str,
        identity_hash: str,
        cache_key: str,
        provider_payload: str,
    ) -> LLMReview:
        assert self._coordinator is not None
        heartbeat = asyncio.create_task(
            self._heartbeat_coordination(request.request_id, identity_hash)
        )
        try:
            cached = await self._cache_get(cache_key)
            if cached is not None:
                review = self._cached_review(request, input_hash, cached)
            else:
                outcome = await self._run_provider(
                    cache_key,
                    request,
                    provider_payload,
                    reserve_local_budget=False,
                )
                review = self._review_from_outcome(request, input_hash, outcome)
            await asyncio.to_thread(
                self._coordinator.complete,
                request.request_id,
                identity_hash,
                review,
            )
            return review
        except asyncio.CancelledError:
            await asyncio.to_thread(
                self._coordinator.abandon,
                request.request_id,
                identity_hash,
                "COORDINATION_RECOVERY",
            )
            raise
        except Exception:
            await asyncio.to_thread(
                self._coordinator.abandon,
                request.request_id,
                identity_hash,
                "COORDINATION_RECOVERY",
            )
            raise
        finally:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat

    async def _heartbeat_coordination(self, request_id: str, identity_hash: str) -> None:
        assert self._coordinator is not None
        while True:
            await asyncio.sleep(self._coordinator.heartbeat_interval_seconds)
            renewed = await asyncio.to_thread(self._coordinator.renew, request_id, identity_hash)
            if not renewed:
                return

    async def _singleflight_provider(
        self,
        request_id: str,
        cache_key: str,
        request: LLMReviewRequest,
        provider_payload: str,
    ) -> _ProviderOutcome:
        async with self._inflight_lock:
            entry = self._inflight.get(request_id)
            if entry is not None and entry.cache_key != cache_key:
                raise InFlightReviewConflict("request id is already in flight with different input")
            if entry is None:
                task = asyncio.create_task(self._run_provider(cache_key, request, provider_payload))
                entry = _InFlightReview(cache_key=cache_key, task=task)
                self._inflight[request_id] = entry
                asyncio.create_task(self._clear_inflight(request_id, entry))
        return await asyncio.shield(entry.task)

    async def _clear_inflight(self, request_id: str, entry: _InFlightReview) -> None:
        try:
            await asyncio.shield(entry.task)
        except Exception:
            pass
        finally:
            async with self._inflight_lock:
                if self._inflight.get(request_id) is entry:
                    del self._inflight[request_id]

    async def _run_provider(
        self,
        cache_key: str,
        request: LLMReviewRequest,
        provider_payload: str,
        *,
        reserve_local_budget: bool = True,
    ) -> _ProviderOutcome:
        prompt = system_prompt(request.stage)
        if reserve_local_budget:
            reserved = await self._budget.reserve(f"{prompt}\n{provider_payload}")
            if reserved is None:
                return _ProviderOutcome(None, "BUDGET_EXCEEDED", 0, 0, 0, 0)
        try:
            async with self._semaphore:
                completion = await self._provider.complete(
                    prompt,
                    provider_payload,
                    validator=lambda content: validate_content_for_request(content, request),
                )
        except ProviderFailure as exc:
            return _ProviderOutcome(
                None,
                exc.reason_code,
                exc.attempts,
                exc.latency_ms,
                exc.input_tokens,
                exc.output_tokens,
            )
        await self._cache_put(cache_key, completion.content, completion.provider_request_id)
        return _ProviderOutcome(
            completion,
            None,
            completion.attempts,
            completion.latency_ms,
            completion.input_tokens,
            completion.output_tokens,
        )

    def _cached_review(
        self, request: LLMReviewRequest, input_hash: str, cached: _CacheEntry
    ) -> LLMReview:
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

    def _review_from_outcome(
        self,
        request: LLMReviewRequest,
        input_hash: str,
        outcome: _ProviderOutcome,
    ) -> LLMReview:
        if outcome.completion is None:
            return self._inert(
                request,
                input_hash,
                outcome.unavailable_reason or "PROVIDER_ERROR",
                attempts=outcome.attempts,
                latency_ms=outcome.latency_ms,
                input_tokens=outcome.input_tokens,
                output_tokens=outcome.output_tokens,
                estimated_cost=_estimated_cost(
                    outcome.input_tokens, outcome.output_tokens, self._settings
                ),
            )
        return self._completed(
            request,
            outcome.completion.content,
            self._metadata(input_hash, outcome.completion),
        )

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
        input_tokens: int = 0,
        output_tokens: int = 0,
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
            input_tokens=input_tokens,
            output_tokens=output_tokens,
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


def _reservation_estimate(prompt_and_payload: str, settings: LLMSettings) -> Decimal:
    # One Unicode code point per token is deliberately conservative for CJK input.
    return _estimated_cost(
        max(1, len(prompt_and_payload)),
        settings.max_output_tokens,
        settings,
    ) * Decimal(settings.max_retries + 1)


def _decimal_text(value: Decimal) -> str:
    if value == 0:
        return "0"
    quantized = value.quantize(Decimal("0.0000000001"), rounding=ROUND_UP)
    return format(quantized.normalize(), "f")


def _utc_z(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("LLM service clock must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def same_review_request(
    review: LLMReview,
    request: LLMReviewRequest,
    input_hash: str,
) -> bool:
    """Compare one durable result with the full deterministic request identity."""
    return (
        review.request_id == request.request_id
        and review.correlation_id == request.correlation_id
        and review.causation_id == request.causation_id
        and review.session_id == request.session_id
        and review.occurred_at_utc == request.occurred_at_utc
        and review.source_sequence == request.source_sequence
        and review.rule_version == request.rule_version
        and review.stage == request.stage
        and review.trading_date == request.trading_date
        and review.plan_id == request.plan_id
        and review.plan_hash == request.plan_hash
        and review.provider.input_hash == input_hash
    )


__all__ = [
    "InFlightReviewConflict",
    "LLMReviewService",
    "ReviewCoordinationUnavailable",
    "same_review_request",
]
