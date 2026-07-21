"""Durable inputs and queues for post-market and intraday LLM reviews."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from typing import Any, Literal, cast

from sqlalchemy import Connection, func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine

from app.llm.automation_context import (
    intraday_request,
    load_session_facts,
    post_market_inert_reason,
    post_market_request,
)
from app.llm.models import LLMReviewRequest
from app.persistence.repository import json_dumps_stable, write_outbox
from app.persistence.tables import (
    candidate_trade_plans,
    daily_reviews,
    event_contexts,
    llm_automation_runs,
    llm_event_cursors,
    llm_trigger_events,
    orders,
    outbox_events,
    signals,
    trading_sessions,
)


_CURSOR = "intraday-deterministic-state-v1"
AutomationState = Literal["WAITING_INERT", "ENQUEUED", "PROCESSING", "COMPLETED", "DEAD_LETTERED"]
_TRIGGER_TOPICS = frozenset(
    {
        "signal.persisted",
        "event_context.built",
        "candidate.staged",
        "risk.decision_recorded",
        "order.staged",
        "order.projected",
        "broker.fill_recorded",
        "broker.snapshot_reconciled",
        "broker.snapshot_diff",
        "broker.reconciliation_failed",
    }
)


@dataclass(frozen=True)
class AutomationRun:
    run_id: str
    kind: Literal["POST_MARKET", "INTRADAY"]
    request_id: str
    session_id: str
    trading_date: date | None
    state: AutomationState
    inert_reason_code: str | None
    trigger_hash: str
    outbox_event_id: str | None
    source_event_ids: tuple[str, ...]


def due_post_market_dates(engine: Engine, *, now: datetime, limit: int = 5) -> list[date]:
    now = _aware_utc(now)
    if not 1 <= limit <= 30:
        raise ValueError("post-market due-date limit is invalid")
    query = (
        select(trading_sessions.c.trading_date)
        .outerjoin(daily_reviews, daily_reviews.c.trading_date == trading_sessions.c.trading_date)
        .outerjoin(
            llm_automation_runs,
            (llm_automation_runs.c.trading_date == trading_sessions.c.trading_date)
            & (llm_automation_runs.c.kind == "POST_MARKET"),
        )
        .where(
            trading_sessions.c.trading_date <= now.date(),
            daily_reviews.c.trading_date.is_(None),
            (llm_automation_runs.c.run_id.is_(None))
            | (llm_automation_runs.c.state == "WAITING_INERT"),
        )
        .order_by(trading_sessions.c.trading_date.desc())
        .limit(limit)
    )
    with engine.connect() as conn:
        return list(conn.execute(query).scalars())


def schedule_post_market_review(
    engine: Engine,
    trading_day: date,
    *,
    now: datetime,
    rule_version: str,
    grace_seconds: int,
) -> AutomationRun:
    """Evaluate close/reconciliation gates and enqueue one deterministic review."""
    now = _aware_utc(now)
    if not rule_version or not 0 <= grace_seconds <= 3600:
        raise ValueError("post-market scheduler configuration is invalid")
    request_id = f"post_market:{trading_day.isoformat()}:v1"
    run_id = f"run:{request_id}"
    with engine.begin() as conn:
        existing = _automation_run(conn, run_id, lock=True)
        if existing is not None and existing.state != "WAITING_INERT":
            return existing
        session = (
            conn.execute(
                select(trading_sessions).where(trading_sessions.c.trading_date == trading_day)
            )
            .mappings()
            .one_or_none()
        )
        if session is None:
            return _record_waiting(
                conn,
                run_id,
                request_id,
                f"missing:{trading_day.isoformat()}",
                trading_day,
                "TRADING_SESSION_MISSING",
                now,
            )
        session_row = dict(session)
        session_id = str(session_row["session_id"])
        closed_at = _optional_aware_utc(session_row["closed_at_utc"])
        if str(session_row["status"]) != "CLOSED" or closed_at is None:
            return _record_waiting(
                conn,
                run_id,
                request_id,
                session_id,
                trading_day,
                "SESSION_NOT_CLOSED",
                now,
            )
        ready_at = closed_at + timedelta(seconds=grace_seconds)
        if now < ready_at:
            return _record_waiting(
                conn,
                run_id,
                request_id,
                session_id,
                trading_day,
                "POST_MARKET_GRACE_ACTIVE",
                now,
            )
        facts = load_session_facts(conn, session_row)
        reason = post_market_inert_reason(facts, closed_at)
        if reason is not None:
            return _record_waiting(conn, run_id, request_id, session_id, trading_day, reason, now)
        request = post_market_request(facts, ready_at, rule_version)
        trigger_hash = _request_hash(request)
        source_ids = tuple(source.source_id for source in request.source_refs)
        source_event_id = f"automation:{run_id}:{trigger_hash}"
        outbox_event_id = write_outbox(
            conn,
            source_event_id=source_event_id,
            topic="llm.review.requested",
            aggregate_type="LLMReviewRequest",
            aggregate_id=request.request_id,
            occurred_at_utc=ready_at,
            payload={"run_id": run_id, "request": request.model_dump(mode="json")},
            created_at_utc=now,
        )
        _upsert_run(
            conn,
            {
                "run_id": run_id,
                "kind": "POST_MARKET",
                "request_id": request_id,
                "session_id": session_id,
                "trading_date": trading_day,
                "state": "ENQUEUED",
                "inert_reason_code": None,
                "trigger_hash": trigger_hash,
                "outbox_event_id": outbox_event_id,
                "source_event_ids": list(source_ids),
                "created_at_utc": now,
                "updated_at_utc": now,
                "completed_at_utc": None,
            },
        )
        result = _automation_run(conn, run_id, lock=False)
        assert result is not None
        return result


def ingest_intraday_trigger_events(
    engine: Engine,
    *,
    now: datetime,
    debounce_seconds: int,
    limit: int = 200,
) -> int:
    """Copy deterministic outbox facts into an LLM-owned queue asynchronously."""
    now = _aware_utc(now)
    if not 1 <= debounce_seconds <= 300 or not 1 <= limit <= 1000:
        raise ValueError("intraday trigger ingestion parameters are invalid")
    with engine.begin() as conn:
        _ensure_cursor(conn, now)
        cursor_query = select(llm_event_cursors).where(llm_event_cursors.c.cursor_name == _CURSOR)
        if conn.dialect.name == "postgresql":
            cursor_query = cursor_query.with_for_update(skip_locked=True)
        cursor = conn.execute(cursor_query).mappings().one_or_none()
        if cursor is None:
            return 0
        rows = (
            conn.execute(
                select(outbox_events)
                .where(outbox_events.c.id > int(cursor["last_outbox_id"]))
                .order_by(outbox_events.c.id)
                .limit(limit)
            )
            .mappings()
            .all()
        )
        if not rows:
            return 0
        inserted = 0
        for row in rows:
            topic = str(row["topic"])
            if topic not in _TRIGGER_TOPICS:
                continue
            payload = dict(row["payload"])
            session_id = _resolve_session_id(conn, row, payload)
            fingerprint = sha256(
                json_dumps_stable(
                    {
                        "topic": topic,
                        "aggregate_type": str(row["aggregate_type"]),
                        "aggregate_id": str(row["aggregate_id"]),
                        "payload": payload,
                    }
                ).encode("utf-8")
            ).hexdigest()
            values = {
                "source_outbox_id": int(row["id"]),
                "source_event_id": str(row["event_id"]),
                "session_id": session_id,
                "topic": topic,
                "aggregate_type": str(row["aggregate_type"]),
                "aggregate_id": str(row["aggregate_id"]),
                "occurred_at_utc": _aware_utc(row["occurred_at_utc"]),
                "event_fingerprint": fingerprint,
                "payload": payload,
                "state": "PENDING" if session_id is not None else "IGNORED",
                "available_at_utc": now + timedelta(seconds=debounce_seconds),
                "merged_run_id": None,
                "created_at_utc": now,
            }
            inserted += _insert_trigger(conn, values)
        conn.execute(
            update(llm_event_cursors)
            .where(llm_event_cursors.c.cursor_name == _CURSOR)
            .values(last_outbox_id=int(rows[-1]["id"]), updated_at_utc=now)
        )
    return inserted


def enqueue_intraday_review(
    engine: Engine,
    *,
    now: datetime,
    rule_version: str,
    min_interval_seconds: int,
    batch_size: int = 50,
) -> AutomationRun | None:
    """Merge one debounced session batch and enqueue at most one review."""
    now = _aware_utc(now)
    if not rule_version or not 1 <= min_interval_seconds <= 3600 or not 1 <= batch_size <= 100:
        raise ValueError("intraday review enqueue parameters are invalid")
    with engine.begin() as conn:
        _ensure_cursor(conn, now)
        mutex_query = select(llm_event_cursors).where(llm_event_cursors.c.cursor_name == _CURSOR)
        if conn.dialect.name == "postgresql":
            mutex_query = mutex_query.with_for_update(skip_locked=True)
        if conn.execute(mutex_query).first() is None:
            return None
        first = (
            conn.execute(
                select(llm_trigger_events)
                .where(
                    llm_trigger_events.c.state == "PENDING",
                    llm_trigger_events.c.session_id.is_not(None),
                    llm_trigger_events.c.available_at_utc <= now,
                )
                .order_by(llm_trigger_events.c.available_at_utc, llm_trigger_events.c.id)
                .limit(1)
                .with_for_update(skip_locked=conn.dialect.name == "postgresql")
            )
            .mappings()
            .one_or_none()
        )
        if first is None:
            return None
        session_id = str(first["session_id"])
        latest_run_at = conn.execute(
            select(llm_automation_runs.c.created_at_utc)
            .where(
                llm_automation_runs.c.kind == "INTRADAY",
                llm_automation_runs.c.session_id == session_id,
                llm_automation_runs.c.state.in_(
                    ("ENQUEUED", "PROCESSING", "COMPLETED", "DEAD_LETTERED")
                ),
            )
            .order_by(llm_automation_runs.c.created_at_utc.desc())
            .limit(1)
        ).scalar_one_or_none()
        if latest_run_at is not None:
            next_allowed = _aware_utc(latest_run_at) + timedelta(seconds=min_interval_seconds)
            if now < next_allowed:
                conn.execute(
                    update(llm_trigger_events)
                    .where(
                        llm_trigger_events.c.state == "PENDING",
                        llm_trigger_events.c.session_id == session_id,
                    )
                    .values(available_at_utc=next_allowed)
                )
                return None
        event_rows = [
            dict(row)
            for row in conn.execute(
                select(llm_trigger_events)
                .where(
                    llm_trigger_events.c.state == "PENDING",
                    llm_trigger_events.c.session_id == session_id,
                    llm_trigger_events.c.available_at_utc <= now,
                )
                .order_by(llm_trigger_events.c.occurred_at_utc, llm_trigger_events.c.id)
                .limit(batch_size)
                .with_for_update(skip_locked=conn.dialect.name == "postgresql")
            ).mappings()
        ]
        if not event_rows:
            return None
        session = (
            conn.execute(
                select(trading_sessions).where(trading_sessions.c.session_id == session_id)
            )
            .mappings()
            .one_or_none()
        )
        if session is None:
            conn.execute(
                update(llm_trigger_events)
                .where(llm_trigger_events.c.id.in_([row["id"] for row in event_rows]))
                .values(state="IGNORED")
            )
            return None
        facts = load_session_facts(conn, dict(session))
        request = intraday_request(facts, event_rows, rule_version)
        trigger_hash = _request_hash(request)
        request_id = request.request_id
        run_id = f"run:{request_id}"
        outbox_event_id = write_outbox(
            conn,
            source_event_id=f"automation:{run_id}:{trigger_hash}",
            topic="llm.review.requested",
            aggregate_type="LLMReviewRequest",
            aggregate_id=request_id,
            occurred_at_utc=_parse_utc_z(request.occurred_at_utc),
            payload={"run_id": run_id, "request": request.model_dump(mode="json")},
            created_at_utc=now,
        )
        source_event_ids = [str(row["source_event_id"]) for row in event_rows]
        _upsert_run(
            conn,
            {
                "run_id": run_id,
                "kind": "INTRADAY",
                "request_id": request_id,
                "session_id": session_id,
                "trading_date": session["trading_date"],
                "state": "ENQUEUED",
                "inert_reason_code": None,
                "trigger_hash": trigger_hash,
                "outbox_event_id": outbox_event_id,
                "source_event_ids": source_event_ids,
                "created_at_utc": now,
                "updated_at_utc": now,
                "completed_at_utc": None,
            },
        )
        conn.execute(
            update(llm_trigger_events)
            .where(llm_trigger_events.c.id.in_([row["id"] for row in event_rows]))
            .values(state="MERGED", merged_run_id=run_id)
        )
        result = _automation_run(conn, run_id, lock=False)
        assert result is not None
        return result


def mark_automation_state(
    engine: Engine,
    run_id: str,
    state: Literal["PROCESSING", "COMPLETED", "DEAD_LETTERED"],
    *,
    now: datetime,
    reason_code: str | None = None,
) -> bool:
    now = _aware_utc(now)
    if not run_id:
        raise ValueError("automation run id is missing")
    with engine.begin() as conn:
        result = conn.execute(
            update(llm_automation_runs)
            .where(llm_automation_runs.c.run_id == run_id)
            .values(
                state=state,
                inert_reason_code=reason_code,
                updated_at_utc=now,
                completed_at_utc=now if state in {"COMPLETED", "DEAD_LETTERED"} else None,
            )
        )
    return result.rowcount == 1


def list_automation_runs(engine: Engine, *, limit: int = 50) -> list[AutomationRun]:
    if not 1 <= limit <= 200:
        raise ValueError("automation run list limit is invalid")
    with engine.connect() as conn:
        rows = conn.execute(
            select(llm_automation_runs)
            .order_by(llm_automation_runs.c.updated_at_utc.desc())
            .limit(limit)
        ).mappings()
    return [_run_from_row(row) for row in rows]


def validate_automation_delivery(
    engine: Engine,
    run_id: str,
    outbox_event_id: str,
    request: LLMReviewRequest,
) -> AutomationRun:
    """Bind one leased outbox payload to the immutable scheduler decision."""
    if not run_id or not outbox_event_id:
        raise ValueError("automation delivery identity is missing")
    with engine.connect() as conn:
        run = _automation_run(conn, run_id, lock=False)
    if run is None:
        raise ValueError("automation delivery has no durable run")
    expected_stage = "POST_MARKET" if run.kind == "POST_MARKET" else "INTRADAY"
    if (
        run.state == "DEAD_LETTERED"
        or run.outbox_event_id != outbox_event_id
        or run.request_id != request.request_id
        or run.session_id != request.session_id
        or run.trading_date
        != (date.fromisoformat(request.trading_date) if request.trading_date else None)
        or request.stage != expected_stage
        or run.trigger_hash != _request_hash(request)
    ):
        raise ValueError("automation delivery conflicts with its durable run")
    return run


def _resolve_session_id(conn: Connection, row: Any, payload: dict[str, Any]) -> str | None:
    session_id = payload.get("session_id")
    if isinstance(session_id, str) and session_id:
        return session_id
    aggregate_type = str(row["aggregate_type"])
    aggregate_id = str(row["aggregate_id"])
    table_and_column = {
        "Signal": (signals, signals.c.signal_id),
        "CandidateTradePlan": (candidate_trade_plans, candidate_trade_plans.c.plan_id),
        "ExecutionOrder": (orders, orders.c.order_id),
        "EventContext": (event_contexts, event_contexts.c.event_id),
    }.get(aggregate_type)
    if table_and_column is None:
        return None
    table, identity = table_and_column
    value = conn.execute(
        select(table.c.session_id).where(identity == aggregate_id)
    ).scalar_one_or_none()
    return str(value) if value else None


def _record_waiting(
    conn: Connection,
    run_id: str,
    request_id: str,
    session_id: str,
    trading_day: date,
    reason: str,
    now: datetime,
) -> AutomationRun:
    trigger_hash = sha256(
        f"{run_id}|{session_id}|{trading_day.isoformat()}|{reason}".encode("utf-8")
    ).hexdigest()
    _upsert_run(
        conn,
        {
            "run_id": run_id,
            "kind": "POST_MARKET",
            "request_id": request_id,
            "session_id": session_id,
            "trading_date": trading_day,
            "state": "WAITING_INERT",
            "inert_reason_code": reason,
            "trigger_hash": trigger_hash,
            "outbox_event_id": None,
            "source_event_ids": [],
            "created_at_utc": now,
            "updated_at_utc": now,
            "completed_at_utc": None,
        },
    )
    result = _automation_run(conn, run_id, lock=False)
    assert result is not None
    return result


def _upsert_run(conn: Connection, values: dict[str, Any]) -> None:
    update_values = {
        key: value
        for key, value in values.items()
        if key not in {"run_id", "request_id", "created_at_utc"}
    }
    if conn.dialect.name == "postgresql":
        conn.execute(
            postgresql_insert(llm_automation_runs)
            .values(**values)
            .on_conflict_do_update(
                index_elements=[llm_automation_runs.c.run_id], set_=update_values
            )
        )
    elif conn.dialect.name == "sqlite":
        conn.execute(
            sqlite_insert(llm_automation_runs)
            .values(**values)
            .on_conflict_do_update(
                index_elements=[llm_automation_runs.c.run_id], set_=update_values
            )
        )
    else:
        existing = conn.execute(
            select(llm_automation_runs.c.run_id).where(
                llm_automation_runs.c.run_id == values["run_id"]
            )
        ).first()
        if existing is None:
            conn.execute(insert(llm_automation_runs).values(**values))
        else:
            conn.execute(
                update(llm_automation_runs)
                .where(llm_automation_runs.c.run_id == values["run_id"])
                .values(**update_values)
            )


def _insert_trigger(conn: Connection, values: dict[str, Any]) -> int:
    if conn.dialect.name == "postgresql":
        inserted_id = conn.execute(
            postgresql_insert(llm_trigger_events)
            .values(**values)
            .on_conflict_do_nothing()
            .returning(llm_trigger_events.c.id)
        ).scalar_one_or_none()
        return int(inserted_id is not None)
    elif conn.dialect.name == "sqlite":
        result = conn.execute(
            sqlite_insert(llm_trigger_events).values(**values).on_conflict_do_nothing()
        )
    else:
        if (
            conn.execute(
                select(llm_trigger_events.c.id).where(
                    llm_trigger_events.c.source_event_id == values["source_event_id"]
                )
            ).first()
            is not None
        ):
            return 0
        result = conn.execute(insert(llm_trigger_events).values(**values))
    return int(result.rowcount == 1)


def _ensure_cursor(conn: Connection, now: datetime) -> None:
    current_outbox_id = int(
        conn.execute(select(func.coalesce(func.max(outbox_events.c.id), 0))).scalar_one()
    )
    values = {
        "cursor_name": _CURSOR,
        "last_outbox_id": current_outbox_id,
        "updated_at_utc": now,
    }
    if conn.dialect.name == "postgresql":
        conn.execute(
            postgresql_insert(llm_event_cursors)
            .values(**values)
            .on_conflict_do_nothing(index_elements=[llm_event_cursors.c.cursor_name])
        )
    elif conn.dialect.name == "sqlite":
        conn.execute(
            sqlite_insert(llm_event_cursors)
            .values(**values)
            .on_conflict_do_nothing(index_elements=[llm_event_cursors.c.cursor_name])
        )


def _automation_run(conn: Connection, run_id: str, *, lock: bool) -> AutomationRun | None:
    query = select(llm_automation_runs).where(llm_automation_runs.c.run_id == run_id)
    if lock:
        query = query.with_for_update()
    row = conn.execute(query).mappings().one_or_none()
    return _run_from_row(row) if row is not None else None


def _run_from_row(row: Any) -> AutomationRun:
    return AutomationRun(
        run_id=str(row["run_id"]),
        kind=cast(Literal["POST_MARKET", "INTRADAY"], str(row["kind"])),
        request_id=str(row["request_id"]),
        session_id=str(row["session_id"]),
        trading_date=row["trading_date"],
        state=cast(AutomationState, str(row["state"])),
        inert_reason_code=str(row["inert_reason_code"]) if row["inert_reason_code"] else None,
        trigger_hash=str(row["trigger_hash"]),
        outbox_event_id=str(row["outbox_event_id"]) if row["outbox_event_id"] else None,
        source_event_ids=tuple(str(value) for value in row["source_event_ids"]),
    )


def _request_hash(request: LLMReviewRequest) -> str:
    return sha256(json_dumps_stable(request.model_dump(mode="json")).encode("utf-8")).hexdigest()


def _aware_utc(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError("automation timestamp is invalid")
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _optional_aware_utc(value: Any) -> datetime | None:
    return None if value is None else _aware_utc(value)


def _parse_utc_z(value: str) -> datetime:
    if not value.endswith("Z"):
        raise ValueError("automation UTC timestamp must end in Z")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


__all__ = [
    "AutomationRun",
    "AutomationState",
    "due_post_market_dates",
    "enqueue_intraday_review",
    "ingest_intraday_trigger_events",
    "list_automation_runs",
    "mark_automation_state",
    "schedule_post_market_review",
    "validate_automation_delivery",
]
