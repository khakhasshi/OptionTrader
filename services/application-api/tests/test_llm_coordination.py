from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
import os
from pathlib import Path
from threading import Barrier
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, delete, event, select
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

from app.llm.coordinator import (
    CoordinationConflict,
    PostgresReviewCoordinator,
    coordination_identity,
)
from app.llm.config import LLMSettings
from app.llm.models import LLMReview, LLMReviewContent, LLMReviewRequest
from app.llm.provider import ContentValidator, ProviderCompletion
from app.llm.service import LLMReviewService
from app.persistence.tables import llm_daily_budgets, llm_request_leases, metadata


_ROOT = Path(__file__).resolve().parents[3]


def _engine() -> Engine:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _attach(dbapi_conn: Any, _record: Any) -> None:
        cursor = dbapi_conn.cursor()
        for schema in ("trading", "audit", "events", "risk", "review"):
            cursor.execute(f"ATTACH DATABASE ':memory:' AS {schema}")
        cursor.close()

    metadata.create_all(engine)
    return engine


def _review() -> LLMReview:
    raw = json.loads((_ROOT / "packages/contracts/fixtures/llm_review.completed.json").read_text())
    return LLMReview.model_validate(raw)


class _BlockingProvider:
    def __init__(self) -> None:
        self.calls = 0
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def complete(
        self,
        _system: str,
        _provider_payload: str,
        *,
        validator: ContentValidator | None = None,
    ) -> ProviderCompletion:
        self.calls += 1
        self.entered.set()
        await self.release.wait()
        content = LLMReviewContent(
            summary="Coordinated review completed.",
            decision_support="Advisory only.",
            sop_alignment="Aligned",
            risk_notes=[],
            invalidations=[],
            recommended_action="Review Only",
            confidence=0.5,
            rule_references=[],
            evidence_citations=[],
            daily_review=None,
            rule_hypotheses=[],
        )
        if validator is not None:
            content = validator(content)
        return ProviderCompletion(
            content=content,
            provider_request_id="global-single-flight",
            attempts=1,
            latency_ms=10,
            input_tokens=100,
            output_tokens=50,
        )


def _settings() -> LLMSettings:
    return LLMSettings.from_env(
        {
            "LLM_PROVIDER": "deepseek-openai",
            "LLM_BASE_URL": "https://api.deepseek.com",
            "LLM_API_KEY": "test-key-never-real",
            "LLM_MODEL": "deepseek-v4-flash",
        }
    )


def _pre_market_request(request_id: str) -> LLMReviewRequest:
    raw = json.loads(
        (_ROOT / "packages/contracts/fixtures/llm_review_request.sample.json").read_text()
    )
    raw.update(
        {
            "request_id": request_id,
            "stage": "PRE_MARKET",
            "plan_id": None,
            "plan_hash": None,
            "causation_id": None,
        }
    )
    raw["context"]["candidate_trade_plan"] = None
    raw["context"]["initial_risk_decision"] = None
    return LLMReviewRequest.model_validate(raw)


def test_global_claim_is_single_flight_and_result_survives_worker_restart() -> None:
    engine = _engine()
    now = datetime(2026, 7, 22, 15, 0, tzinfo=UTC)
    first = PostgresReviewCoordinator(
        engine,
        daily_max_requests=2,
        daily_max_estimated_usd=Decimal("1"),
        worker_id="worker-a",
        now=lambda: now,
    )
    second = PostgresReviewCoordinator(
        engine,
        daily_max_requests=2,
        daily_max_estimated_usd=Decimal("1"),
        worker_id="worker-b",
        now=lambda: now,
    )
    identity = coordination_identity("same canonical request")
    assert first.claim("request-a", identity, Decimal("0.2")).role == "LEADER"
    assert second.claim("request-a", identity, Decimal("0.2")).role == "WAIT"
    review = _review()
    assert first.complete("request-a", identity, review) is True
    completed = second.claim("request-a", identity, Decimal("0.2"))
    assert completed.role == "COMPLETED"
    assert completed.review == review
    with engine.connect() as conn:
        budget = conn.execute(select(llm_daily_budgets)).mappings().one()
    assert budget["request_count"] == 1
    assert Decimal(str(budget["reserved_cost_usd"])) == Decimal("0.2")


def test_request_identity_conflict_fails_before_second_budget_reservation() -> None:
    engine = _engine()
    coordinator = PostgresReviewCoordinator(
        engine,
        daily_max_requests=2,
        daily_max_estimated_usd=Decimal("1"),
        worker_id="worker-a",
    )
    first_identity = coordination_identity("first")
    second_identity = coordination_identity("second")
    assert coordinator.claim("request-a", first_identity, Decimal("0.1")).role == "LEADER"
    try:
        coordinator.claim("request-a", second_identity, Decimal("0.1"))
    except CoordinationConflict:
        pass
    else:
        raise AssertionError("conflicting request identity was accepted")
    with engine.connect() as conn:
        assert conn.execute(select(llm_daily_budgets.c.request_count)).scalar_one() == 1


def test_expired_unknown_lease_recovers_inert_without_second_provider_budget() -> None:
    engine = _engine()
    clock = [datetime(2026, 7, 22, 15, 0, tzinfo=UTC)]
    first = PostgresReviewCoordinator(
        engine,
        daily_max_requests=2,
        daily_max_estimated_usd=Decimal("1"),
        lease_seconds=30,
        worker_id="crashed-worker",
        now=lambda: clock[0],
    )
    recovery = PostgresReviewCoordinator(
        engine,
        daily_max_requests=2,
        daily_max_estimated_usd=Decimal("1"),
        lease_seconds=30,
        worker_id="recovery-worker",
        now=lambda: clock[0],
    )
    identity = coordination_identity("lease-expiry")
    assert first.claim("request-expired", identity, Decimal("0.4")).role == "LEADER"
    clock[0] += timedelta(seconds=31)
    claim = recovery.claim("request-expired", identity, Decimal("0.4"))
    assert claim.role == "INERT"
    assert claim.failure_code == "COORDINATION_LEASE_EXPIRED"
    with engine.connect() as conn:
        budget = conn.execute(select(llm_daily_budgets)).mappings().one()
    assert budget["request_count"] == 1
    assert Decimal(str(budget["reserved_cost_usd"])) == Decimal("0.4")


def test_atomic_daily_budget_rejects_second_distinct_request() -> None:
    engine = _engine()
    coordinator = PostgresReviewCoordinator(
        engine,
        daily_max_requests=1,
        daily_max_estimated_usd=Decimal("0.5"),
        worker_id="worker-a",
    )
    assert (
        coordinator.claim("request-a", coordination_identity("a"), Decimal("0.4")).role == "LEADER"
    )
    rejected = coordinator.claim("request-b", coordination_identity("b"), Decimal("0.1"))
    assert rejected.role == "INERT"
    assert rejected.failure_code == "BUDGET_EXCEEDED"
    with engine.connect() as conn:
        assert conn.execute(select(llm_daily_budgets.c.request_count)).scalar_one() == 1


@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL is required")
def test_postgresql_multi_worker_claim_and_budget_are_atomic() -> None:
    raw_url = os.environ["DATABASE_URL"]
    engine = create_engine(
        raw_url.replace("postgresql://", "postgresql+psycopg://", 1), pool_size=5
    )
    suffix = uuid4().hex
    now = datetime(2099, 1, 7, 15, 0, tzinfo=UTC)
    same_request = f"pg-same-{suffix}"
    budget_requests = (f"pg-budget-a-{suffix}", f"pg-budget-b-{suffix}")
    identity = coordination_identity(f"pg-same-{suffix}")
    same_barrier = Barrier(2)
    budget_barrier = Barrier(2)

    def same_claim(worker: str) -> str:
        coordinator = PostgresReviewCoordinator(
            engine,
            daily_max_requests=10,
            daily_max_estimated_usd=Decimal("10"),
            worker_id=worker,
            now=lambda: now,
        )
        same_barrier.wait(timeout=3)
        return coordinator.claim(same_request, identity, Decimal("0.1")).role

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            same_roles = list(executor.map(same_claim, ("pg-worker-a", "pg-worker-b")))
        assert sorted(same_roles) == ["LEADER", "WAIT"]

        with engine.begin() as conn:
            conn.execute(
                delete(llm_request_leases).where(llm_request_leases.c.request_id == same_request)
            )
            conn.execute(
                delete(llm_daily_budgets).where(llm_daily_budgets.c.budget_date == now.date())
            )

        def budget_claim(item: tuple[str, str]) -> str:
            request_id, worker = item
            coordinator = PostgresReviewCoordinator(
                engine,
                daily_max_requests=1,
                daily_max_estimated_usd=Decimal("1"),
                worker_id=worker,
                now=lambda: now,
            )
            budget_barrier.wait(timeout=3)
            return coordinator.claim(
                request_id, coordination_identity(request_id), Decimal("0.2")
            ).role

        with ThreadPoolExecutor(max_workers=2) as executor:
            budget_roles = list(
                executor.map(
                    budget_claim,
                    zip(budget_requests, ("pg-budget-worker-a", "pg-budget-worker-b")),
                )
            )
        assert sorted(budget_roles) == ["INERT", "LEADER"]
        with engine.connect() as conn:
            assert (
                conn.execute(
                    select(llm_daily_budgets.c.request_count).where(
                        llm_daily_budgets.c.budget_date == now.date()
                    )
                ).scalar_one()
                == 1
            )
    finally:
        with engine.begin() as conn:
            conn.execute(
                delete(llm_request_leases).where(
                    llm_request_leases.c.request_id.in_((same_request, *budget_requests))
                )
            )
            conn.execute(
                delete(llm_daily_budgets).where(llm_daily_budgets.c.budget_date == now.date())
            )
        engine.dispose()


@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL is required")
def test_postgresql_two_services_make_exactly_one_provider_call() -> None:
    raw_url = os.environ["DATABASE_URL"]
    engine = create_engine(
        raw_url.replace("postgresql://", "postgresql+psycopg://", 1), pool_size=5
    )
    suffix = uuid4().hex
    request = _pre_market_request(f"pg-service-{suffix}")
    budget_now = datetime(2099, 1, 8, 15, 0, tzinfo=UTC)
    provider = _BlockingProvider()
    settings = _settings()
    first = LLMReviewService(
        settings,
        provider,
        now=lambda: datetime(2026, 7, 20, 15, 0, tzinfo=UTC),
        coordinator=PostgresReviewCoordinator(
            engine,
            daily_max_requests=2,
            daily_max_estimated_usd=Decimal("1"),
            worker_id="service-worker-a",
            now=lambda: budget_now,
        ),
    )
    second = LLMReviewService(
        settings,
        provider,
        now=lambda: datetime(2026, 7, 20, 15, 0, tzinfo=UTC),
        coordinator=PostgresReviewCoordinator(
            engine,
            daily_max_requests=2,
            daily_max_estimated_usd=Decimal("1"),
            worker_id="service-worker-b",
            now=lambda: budget_now,
        ),
    )

    async def run() -> tuple[LLMReview, LLMReview]:
        leader = asyncio.create_task(first.review(request))
        await asyncio.wait_for(provider.entered.wait(), timeout=3)
        follower = asyncio.create_task(second.review(request))
        await asyncio.sleep(0.2)
        assert provider.calls == 1
        provider.release.set()
        return await asyncio.gather(leader, follower)

    try:
        first_review, second_review = asyncio.run(run())
        assert first_review == second_review
        assert first_review.review_status == "COMPLETED"
        assert provider.calls == 1
    finally:
        with engine.begin() as conn:
            conn.execute(
                delete(llm_request_leases).where(
                    llm_request_leases.c.request_id == request.request_id
                )
            )
            conn.execute(
                delete(llm_daily_budgets).where(
                    llm_daily_budgets.c.budget_date == budget_now.date()
                )
            )
        engine.dispose()
