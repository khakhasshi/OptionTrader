"""PostgreSQL-backed LLM budget, lease and global request deduplication."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
import os
import socket
from typing import Callable, Literal
from uuid import uuid4

from sqlalchemy import insert, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection, Engine

from app.llm.models import LLMReview
from app.persistence.tables import llm_daily_budgets, llm_request_leases


ClaimRole = Literal["LEADER", "WAIT", "COMPLETED", "INERT"]


@dataclass(frozen=True)
class CoordinationClaim:
    role: ClaimRole
    review: LLMReview | None = None
    failure_code: str | None = None


class CoordinationConflict(ValueError):
    """One request id was reused with a different canonical input."""


class PostgresReviewCoordinator:
    """Coordinate provider work across API workers without a shared process.

    A lease that expires while provider state is unknown is never allowed to
    call the provider again. The next worker owns an inert recovery instead.
    This trades availability for an at-most-once provider attempt.
    """

    def __init__(
        self,
        engine: Engine,
        *,
        daily_max_requests: int,
        daily_max_estimated_usd: Decimal,
        lease_seconds: int = 180,
        worker_id: str | None = None,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not 30 <= lease_seconds <= 300:
            raise ValueError("LLM coordination lease must be between 30 and 300 seconds")
        if daily_max_requests < 1 or daily_max_estimated_usd <= 0:
            raise ValueError("LLM coordination budget limits are invalid")
        self._engine = engine
        self._daily_max_requests = daily_max_requests
        self._daily_max_estimated_usd = daily_max_estimated_usd
        self._lease_seconds = lease_seconds
        self._worker_id = worker_id or _worker_identity()
        self._now = now

    @property
    def poll_interval_seconds(self) -> float:
        return 0.1

    @property
    def wait_timeout_seconds(self) -> float:
        return float(self._lease_seconds + 10)

    @property
    def heartbeat_interval_seconds(self) -> float:
        return max(10.0, self._lease_seconds / 3)

    def claim(
        self,
        request_id: str,
        identity_hash: str,
        estimated_cost_usd: Decimal,
    ) -> CoordinationClaim:
        if not request_id or not _is_hash(identity_hash):
            raise ValueError("LLM coordination identity is invalid")
        if not estimated_cost_usd.is_finite() or estimated_cost_usd < 0:
            raise ValueError("LLM coordination estimate is invalid")
        now = _aware_utc(self._now())
        lease_expires = now + timedelta(seconds=self._lease_seconds)
        with self._engine.begin() as conn:
            self._insert_pending(conn, request_id, identity_hash, now)
            row = (
                conn.execute(
                    select(llm_request_leases)
                    .where(llm_request_leases.c.request_id == request_id)
                    .with_for_update()
                )
                .mappings()
                .one()
            )
            if str(row["identity_hash"]) != identity_hash:
                raise CoordinationConflict("request id has a different coordination identity")
            state = str(row["state"])
            if state == "COMPLETED":
                return CoordinationClaim(
                    "COMPLETED", review=LLMReview.model_validate(row["result_payload"])
                )
            lease = _optional_aware_utc(row["lease_expires_at_utc"])
            if state in {"IN_FLIGHT", "INERT_PENDING"} and lease is not None and lease > now:
                return CoordinationClaim("WAIT")
            if state == "IN_FLIGHT":
                self._take_inert_lease(
                    conn,
                    request_id,
                    now,
                    lease_expires,
                    "COORDINATION_LEASE_EXPIRED",
                )
                return CoordinationClaim("INERT", failure_code="COORDINATION_LEASE_EXPIRED")
            if state == "INERT_PENDING":
                reason = str(row["failure_code"] or "COORDINATION_RECOVERY")
                self._take_inert_lease(conn, request_id, now, lease_expires, reason)
                return CoordinationClaim("INERT", failure_code=reason)
            if state != "PENDING":
                raise ValueError("LLM coordination state is invalid")

            budget = self._lock_budget(conn, now)
            request_count = int(str(budget["request_count"]))
            reserved = Decimal(str(budget["reserved_cost_usd"]))
            if (
                request_count >= self._daily_max_requests
                or reserved + estimated_cost_usd > self._daily_max_estimated_usd
            ):
                self._take_inert_lease(conn, request_id, now, lease_expires, "BUDGET_EXCEEDED")
                return CoordinationClaim("INERT", failure_code="BUDGET_EXCEEDED")

            conn.execute(
                update(llm_daily_budgets)
                .where(llm_daily_budgets.c.budget_date == now.date())
                .values(
                    request_count=llm_daily_budgets.c.request_count + 1,
                    reserved_cost_usd=llm_daily_budgets.c.reserved_cost_usd + estimated_cost_usd,
                    updated_at_utc=now,
                )
            )
            conn.execute(
                update(llm_request_leases)
                .where(llm_request_leases.c.request_id == request_id)
                .values(
                    state="IN_FLIGHT",
                    owner_id=self._worker_id,
                    lease_expires_at_utc=lease_expires,
                    reserved_cost_usd=estimated_cost_usd,
                    failure_code=None,
                    updated_at_utc=now,
                )
            )
        return CoordinationClaim("LEADER")

    def renew(self, request_id: str, identity_hash: str) -> bool:
        now = _aware_utc(self._now())
        with self._engine.begin() as conn:
            result = conn.execute(
                update(llm_request_leases)
                .where(
                    llm_request_leases.c.request_id == request_id,
                    llm_request_leases.c.identity_hash == identity_hash,
                    llm_request_leases.c.state == "IN_FLIGHT",
                    llm_request_leases.c.owner_id == self._worker_id,
                )
                .values(
                    lease_expires_at_utc=now + timedelta(seconds=self._lease_seconds),
                    updated_at_utc=now,
                )
            )
        return result.rowcount == 1

    def complete(self, request_id: str, identity_hash: str, review: LLMReview) -> bool:
        payload = review.model_dump(mode="json")
        actual_cost = Decimal(review.provider.estimated_cost_usd)
        if not actual_cost.is_finite() or actual_cost < 0:
            raise ValueError("LLM review actual cost is invalid")
        now = _aware_utc(self._now())
        with self._engine.begin() as conn:
            row = (
                conn.execute(
                    select(llm_request_leases)
                    .where(llm_request_leases.c.request_id == request_id)
                    .with_for_update()
                )
                .mappings()
                .one_or_none()
            )
            if row is None or str(row["identity_hash"]) != identity_hash:
                raise CoordinationConflict("request coordination identity disappeared")
            if str(row["state"]) == "COMPLETED":
                existing = LLMReview.model_validate(row["result_payload"])
                if existing != review:
                    raise CoordinationConflict("completed request has a different result")
                return False
            if str(row["owner_id"] or "") != self._worker_id:
                raise CoordinationConflict("request lease is no longer owned by this worker")
            conn.execute(
                update(llm_request_leases)
                .where(llm_request_leases.c.request_id == request_id)
                .values(
                    state="COMPLETED",
                    owner_id=None,
                    lease_expires_at_utc=None,
                    actual_cost_usd=actual_cost,
                    result_payload=payload,
                    updated_at_utc=now,
                    completed_at_utc=now,
                )
            )
            if Decimal(str(row["reserved_cost_usd"])) > 0:
                conn.execute(
                    update(llm_daily_budgets)
                    .where(llm_daily_budgets.c.budget_date == row["budget_date"])
                    .values(
                        actual_cost_usd=llm_daily_budgets.c.actual_cost_usd + actual_cost,
                        updated_at_utc=now,
                    )
                )
        return True

    def abandon(self, request_id: str, identity_hash: str, failure_code: str) -> bool:
        if not _valid_failure_code(failure_code):
            raise ValueError("LLM coordination failure code is invalid")
        now = _aware_utc(self._now())
        with self._engine.begin() as conn:
            result = conn.execute(
                update(llm_request_leases)
                .where(
                    llm_request_leases.c.request_id == request_id,
                    llm_request_leases.c.identity_hash == identity_hash,
                    llm_request_leases.c.state == "IN_FLIGHT",
                    llm_request_leases.c.owner_id == self._worker_id,
                )
                .values(
                    state="INERT_PENDING",
                    owner_id=None,
                    lease_expires_at_utc=None,
                    failure_code=failure_code,
                    updated_at_utc=now,
                )
            )
        return result.rowcount == 1

    def _insert_pending(
        self, conn: Connection, request_id: str, identity_hash: str, now: datetime
    ) -> None:
        values = {
            "request_id": request_id,
            "identity_hash": identity_hash,
            "state": "PENDING",
            "owner_id": None,
            "lease_expires_at_utc": None,
            "budget_date": now.date(),
            "reserved_cost_usd": Decimal("0"),
            "actual_cost_usd": Decimal("0"),
            "failure_code": None,
            "created_at_utc": now,
            "updated_at_utc": now,
            "completed_at_utc": None,
        }
        if conn.dialect.name == "postgresql":
            conn.execute(
                postgresql_insert(llm_request_leases)
                .values(**values)
                .on_conflict_do_nothing(index_elements=[llm_request_leases.c.request_id])
            )
        elif conn.dialect.name == "sqlite":
            conn.execute(
                sqlite_insert(llm_request_leases)
                .values(**values)
                .on_conflict_do_nothing(index_elements=[llm_request_leases.c.request_id])
            )
        else:
            try:
                conn.execute(insert(llm_request_leases).values(**values))
            except Exception:
                if (
                    conn.execute(
                        select(llm_request_leases.c.request_id).where(
                            llm_request_leases.c.request_id == request_id
                        )
                    ).first()
                    is None
                ):
                    raise

    def _lock_budget(self, conn: Connection, now: datetime) -> dict[str, object]:
        values = {
            "budget_date": now.date(),
            "request_count": 0,
            "reserved_cost_usd": Decimal("0"),
            "actual_cost_usd": Decimal("0"),
            "updated_at_utc": now,
        }
        if conn.dialect.name == "postgresql":
            conn.execute(
                postgresql_insert(llm_daily_budgets)
                .values(**values)
                .on_conflict_do_nothing(index_elements=[llm_daily_budgets.c.budget_date])
            )
        elif conn.dialect.name == "sqlite":
            conn.execute(
                sqlite_insert(llm_daily_budgets)
                .values(**values)
                .on_conflict_do_nothing(index_elements=[llm_daily_budgets.c.budget_date])
            )
        else:
            if (
                conn.execute(
                    select(llm_daily_budgets.c.budget_date).where(
                        llm_daily_budgets.c.budget_date == now.date()
                    )
                ).first()
                is None
            ):
                conn.execute(insert(llm_daily_budgets).values(**values))
        row = (
            conn.execute(
                select(llm_daily_budgets)
                .where(llm_daily_budgets.c.budget_date == now.date())
                .with_for_update()
            )
            .mappings()
            .one()
        )
        return dict(row)

    def _take_inert_lease(
        self,
        conn: Connection,
        request_id: str,
        now: datetime,
        lease_expires: datetime,
        failure_code: str,
    ) -> None:
        if not _valid_failure_code(failure_code):
            raise ValueError("LLM coordination failure code is invalid")
        conn.execute(
            update(llm_request_leases)
            .where(llm_request_leases.c.request_id == request_id)
            .values(
                state="INERT_PENDING",
                owner_id=self._worker_id,
                lease_expires_at_utc=lease_expires,
                failure_code=failure_code,
                updated_at_utc=now,
            )
        )


def coordination_identity(cache_key: str) -> str:
    return sha256(cache_key.encode("utf-8")).hexdigest()


def _worker_identity() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{uuid4().hex}"


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("LLM coordination clock must be timezone-aware")
    return value.astimezone(UTC)


def _optional_aware_utc(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise ValueError("LLM coordination lease timestamp is invalid")
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _is_hash(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _valid_failure_code(value: str) -> bool:
    return (
        1 <= len(value) <= 64
        and value[0].isalpha()
        and value[0].isupper()
        and all(
            character.isupper() or character.isdigit() or character == "_" for character in value
        )
    )


__all__ = [
    "CoordinationClaim",
    "CoordinationConflict",
    "PostgresReviewCoordinator",
    "coordination_identity",
]
