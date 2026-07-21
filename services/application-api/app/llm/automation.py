"""Non-blocking Phase 4 review scheduler and transactional outbox worker."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
import os
import socket
from typing import Callable, Mapping
from uuid import uuid4

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.llm.automation_repository import (
    due_post_market_dates,
    enqueue_intraday_review,
    ingest_intraday_trigger_events,
    mark_automation_state,
    schedule_post_market_review,
    validate_automation_delivery,
)
from app.llm.models import LLMReviewRequest
from app.llm.market_calendar import materialize_recent_xnys_sessions
from app.llm.security import review_input_hash
from app.llm.service import LLMReviewService, same_review_request
from app.persistence import (
    claim_outbox_batch,
    get_llm_review_by_request_id,
    mark_outbox_published,
    persist_llm_review,
    reschedule_outbox_message,
)
from app.persistence.tables import outbox_events


@dataclass(frozen=True)
class LLMAutomationSettings:
    enabled: bool
    poll_seconds: int
    post_market_grace_seconds: int
    intraday_debounce_seconds: int
    intraday_min_interval_seconds: int
    max_attempts: int

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> LLMAutomationSettings:
        source = os.environ if env is None else env
        enabled_raw = source.get("OPTIONTRADER_LLM_AUTOMATION_ENABLED", "false")
        if enabled_raw not in {"true", "false"}:
            raise ValueError("OPTIONTRADER_LLM_AUTOMATION_ENABLED must be true or false")
        return cls(
            enabled=enabled_raw == "true",
            poll_seconds=_int_setting(source, "OPTIONTRADER_LLM_AUTOMATION_POLL_SECONDS", 5, 1, 60),
            post_market_grace_seconds=_int_setting(
                source, "OPTIONTRADER_LLM_POST_MARKET_GRACE_SECONDS", 60, 0, 3600
            ),
            intraday_debounce_seconds=_int_setting(
                source, "OPTIONTRADER_LLM_INTRADAY_DEBOUNCE_SECONDS", 5, 1, 300
            ),
            intraday_min_interval_seconds=_int_setting(
                source, "OPTIONTRADER_LLM_INTRADAY_MIN_INTERVAL_SECONDS", 60, 10, 3600
            ),
            max_attempts=_int_setting(source, "OPTIONTRADER_LLM_AUTOMATION_MAX_ATTEMPTS", 8, 1, 20),
        )


@dataclass(frozen=True)
class AutomationStatus:
    running: bool
    worker_id: str
    last_cycle_at_utc: str | None
    last_error_code: str | None
    processed_requests: int


class LLMAutomationSupervisor:
    def __init__(
        self,
        engine: Engine,
        review_service: LLMReviewService,
        settings: LLMAutomationSettings,
        *,
        rule_version: str,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        worker_id: str | None = None,
    ) -> None:
        if not rule_version:
            raise ValueError("LLM automation rule version is missing")
        self._engine = engine
        self._review_service = review_service
        self._settings = settings
        self._rule_version = rule_version
        self._now = now
        self._worker_id = worker_id or (
            f"llm-automation:{socket.gethostname()}:{os.getpid()}:{uuid4().hex}"
        )
        self._running = False
        self._last_cycle: datetime | None = None
        self._last_error_code: str | None = None
        self._processed = 0

    def status(self) -> AutomationStatus:
        return AutomationStatus(
            running=self._running,
            worker_id=self._worker_id,
            last_cycle_at_utc=_utc_z(self._last_cycle) if self._last_cycle else None,
            last_error_code=self._last_error_code,
            processed_requests=self._processed,
        )

    async def run_once(self) -> int:
        now = _aware_utc(self._now())
        await asyncio.to_thread(materialize_recent_xnys_sessions, self._engine, now=now)
        dates = await asyncio.to_thread(due_post_market_dates, self._engine, now=now)
        for trading_day in dates:
            await asyncio.to_thread(
                schedule_post_market_review,
                self._engine,
                trading_day,
                now=now,
                rule_version=self._rule_version,
                grace_seconds=self._settings.post_market_grace_seconds,
            )
        await asyncio.to_thread(
            ingest_intraday_trigger_events,
            self._engine,
            now=now,
            debounce_seconds=self._settings.intraday_debounce_seconds,
        )
        for _ in range(10):
            run = await asyncio.to_thread(
                enqueue_intraday_review,
                self._engine,
                now=now,
                rule_version=self._rule_version,
                min_interval_seconds=self._settings.intraday_min_interval_seconds,
            )
            if run is None:
                break
        processed = await self._process_review_outbox(now)
        self._last_cycle = now
        self._last_error_code = None
        self._processed += processed
        return processed

    async def serve(self) -> None:
        self._running = True
        try:
            while True:
                try:
                    await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - supervisor stays isolated
                    self._last_error_code = f"AUTOMATION_{type(exc).__name__.upper()}"[:64]
                await asyncio.sleep(self._settings.poll_seconds)
        finally:
            self._running = False

    async def _process_review_outbox(self, now: datetime) -> int:
        messages = await asyncio.to_thread(
            claim_outbox_batch,
            self._engine,
            self._worker_id,
            limit=10,
            lease_seconds=300,
            now=now,
            topics=("llm.review.requested",),
        )
        processed = 0
        for message in messages:
            run_id = str(message.payload.get("run_id") or "")
            try:
                request = LLMReviewRequest.model_validate(message.payload.get("request"))
                if not run_id:
                    raise ValueError("automation outbox run id is missing")
                await asyncio.to_thread(
                    validate_automation_delivery,
                    self._engine,
                    run_id,
                    message.event_id,
                    request,
                )
                marked = await asyncio.to_thread(
                    mark_automation_state,
                    self._engine,
                    run_id,
                    "PROCESSING",
                    now=now,
                )
                if not marked:
                    raise ValueError("automation run disappeared before processing")
                existing = await asyncio.to_thread(
                    get_llm_review_by_request_id, self._engine, request.request_id
                )
                if existing is not None:
                    input_hash = review_input_hash(
                        request, self._review_service.settings.max_input_chars
                    )
                    if not same_review_request(existing, request, input_hash):
                        raise ValueError("automation request id conflicts with durable review")
                else:
                    review = await self._review_service.review(request)
                    await asyncio.to_thread(persist_llm_review, self._engine, request, review)
                await asyncio.to_thread(
                    mark_automation_state,
                    self._engine,
                    run_id,
                    "COMPLETED",
                    now=_aware_utc(self._now()),
                )
                acknowledged = await asyncio.to_thread(
                    mark_outbox_published,
                    self._engine,
                    message.event_id,
                    self._worker_id,
                    now=_aware_utc(self._now()),
                )
                if not acknowledged:
                    raise ValueError("automation outbox acknowledgement lost its lease")
                processed += 1
            except asyncio.CancelledError:
                raise
            except (ValidationError, ValueError):
                await self._retry_or_dead_letter(
                    message.event_id, run_id, "AUTOMATION_PAYLOAD_INVALID"
                )
            except SQLAlchemyError:
                await self._retry_or_dead_letter(
                    message.event_id, run_id, "AUTOMATION_STORAGE_ERROR"
                )
            except Exception:  # noqa: BLE001 - provider internals remain sanitized
                await self._retry_or_dead_letter(
                    message.event_id, run_id, "AUTOMATION_WORKER_ERROR"
                )
        return processed

    async def _retry_or_dead_letter(self, event_id: str, run_id: str, error_code: str) -> None:
        now = _aware_utc(self._now())
        await asyncio.to_thread(
            reschedule_outbox_message,
            self._engine,
            event_id,
            self._worker_id,
            error_code,
            retry_delay_seconds=min(3600, self._settings.poll_seconds * 2),
            max_attempts=self._settings.max_attempts,
            now=now,
        )
        dead = await asyncio.to_thread(self._dead_lettered_at, event_id)
        if dead is not None and run_id:
            await asyncio.to_thread(
                mark_automation_state,
                self._engine,
                run_id,
                "DEAD_LETTERED",
                now=now,
                reason_code=error_code,
            )

    def _dead_lettered_at(self, event_id: str) -> object:
        with self._engine.connect() as conn:
            return conn.execute(
                select(outbox_events.c.dead_lettered_at_utc).where(
                    outbox_events.c.event_id == event_id
                )
            ).scalar_one_or_none()


def _int_setting(env: Mapping[str, str], key: str, default: int, minimum: int, maximum: int) -> int:
    raw = env.get(key, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{key} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"{key} is outside its allowed range")
    return value


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("LLM automation clock must be timezone-aware")
    return value.astimezone(UTC)


def _utc_z(value: datetime) -> str:
    return _aware_utc(value).isoformat().replace("+00:00", "Z")


__all__ = [
    "AutomationStatus",
    "LLMAutomationSettings",
    "LLMAutomationSupervisor",
]
