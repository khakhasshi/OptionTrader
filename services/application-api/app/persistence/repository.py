"""Transactional write-path for signals + audit events (P1-7).

One public call, :func:`persist_signal`, writes the ``trading.signals`` row and
its ``audit.audit_events`` row inside a single transaction: either both land or
neither does, so a signal can never exist without its audit trail (DEVELOPMENT_
PLAN §7 — signal/audit writes share a transaction).

Re-persisting the same ``signal_id`` is idempotent: the insert is skipped if the
row already exists (replay re-runs must not duplicate or error).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
import re
from typing import Any
from uuid import uuid4

from google.protobuf.json_format import MessageToDict
from sqlalchemy import delete, insert, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine

from app.persistence.serialize import SignalContext, build_signal_rows
from app.persistence.tables import (
    audit_events,
    broker_snapshots,
    candidate_trade_plans,
    confirmation_capabilities,
    event_contexts,
    fills,
    order_events,
    orders,
    outbox_events,
    position_snapshots,
    risk_decisions,
    signals,
)
from app.grpc_gen import broker_pb2
from app.events import EventContext
from app.regime import RegimeState
from app.strategy import StrategyDecision
from app.vol import VolState
from app.trading.models import CandidateTradePlan, ExecutionOrder, StageCandidateResult
from app.trading.capability import ConfirmationCipher


def _now_utc() -> datetime:
    """created_at_utc stamp. Isolated so callers/tests can reason about it;
    occurred_at_utc (the decision instant) always comes from the caller."""
    return datetime.now(timezone.utc)


def _outbox_event_id(source_event_id: str, topic: str) -> str:
    digest = sha256(
        json_dumps_stable({"source_event_id": source_event_id, "topic": topic}).encode("utf-8")
    ).hexdigest()
    return f"outbox_{digest}"


def json_dumps_stable(value: dict[str, Any]) -> str:
    import json

    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _write_outbox(
    conn: Any,
    *,
    source_event_id: str,
    topic: str,
    aggregate_type: str,
    aggregate_id: str,
    occurred_at_utc: datetime,
    payload: dict[str, Any],
    created_at_utc: datetime,
) -> None:
    if not source_event_id or not topic or not aggregate_type or not aggregate_id:
        raise ValueError("outbox event identity is incomplete")
    row = {
        "event_id": _outbox_event_id(source_event_id, topic),
        "topic": topic,
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id,
        "occurred_at_utc": occurred_at_utc,
        "payload": payload,
        "attempts": 0,
        "available_at_utc": created_at_utc,
        "lease_owner": None,
        "lease_expires_at_utc": None,
        "published_at_utc": None,
        "dead_lettered_at_utc": None,
        "last_error_code": None,
        "created_at_utc": created_at_utc,
    }
    if conn.dialect.name == "postgresql":
        conn.execute(
            postgresql_insert(outbox_events)
            .values(**row)
            .on_conflict_do_nothing(index_elements=[outbox_events.c.event_id])
        )
    elif conn.dialect.name == "sqlite":
        conn.execute(
            sqlite_insert(outbox_events)
            .values(**row)
            .on_conflict_do_nothing(index_elements=[outbox_events.c.event_id])
        )
    else:
        exists = conn.execute(
            select(outbox_events.c.event_id).where(outbox_events.c.event_id == row["event_id"])
        ).first()
        if exists is None:
            conn.execute(outbox_events.insert().values(**row))


@dataclass(frozen=True)
class OutboxMessage:
    event_id: str
    topic: str
    aggregate_type: str
    aggregate_id: str
    occurred_at_utc: datetime
    payload: dict[str, Any]
    attempts: int


def claim_outbox_batch(
    engine: Engine,
    worker_id: str,
    *,
    limit: int = 50,
    lease_seconds: int = 30,
    now: datetime | None = None,
) -> list[OutboxMessage]:
    """Lease unpublished events for at-least-once delivery.

    PostgreSQL workers use ``FOR UPDATE SKIP LOCKED``. The event_id is the
    downstream idempotency key; consumers must deduplicate it.
    """
    if not worker_id or not 1 <= limit <= 500 or not 5 <= lease_seconds <= 300:
        raise ValueError("outbox claim parameters are invalid")
    claimed_at = now or _now_utc()
    if claimed_at.tzinfo is None:
        raise ValueError("outbox claim time must be timezone-aware")
    lease_expires = claimed_at + timedelta(seconds=lease_seconds)
    with engine.begin() as conn:
        query = (
            select(outbox_events)
            .where(
                outbox_events.c.published_at_utc.is_(None),
                outbox_events.c.dead_lettered_at_utc.is_(None),
                outbox_events.c.available_at_utc <= claimed_at,
                (
                    outbox_events.c.lease_expires_at_utc.is_(None)
                    | (outbox_events.c.lease_expires_at_utc <= claimed_at)
                ),
            )
            .order_by(outbox_events.c.available_at_utc, outbox_events.c.id)
            .limit(limit)
        )
        if conn.dialect.name == "postgresql":
            query = query.with_for_update(skip_locked=True)
        rows = conn.execute(query).mappings().all()
        claimed: list[OutboxMessage] = []
        for row in rows:
            updated = conn.execute(
                update(outbox_events)
                .where(
                    outbox_events.c.id == row["id"],
                    outbox_events.c.published_at_utc.is_(None),
                    outbox_events.c.dead_lettered_at_utc.is_(None),
                    (
                        outbox_events.c.lease_expires_at_utc.is_(None)
                        | (outbox_events.c.lease_expires_at_utc <= claimed_at)
                    ),
                )
                .values(
                    attempts=outbox_events.c.attempts + 1,
                    lease_owner=worker_id,
                    lease_expires_at_utc=lease_expires,
                    last_error_code=None,
                )
            )
            if updated.rowcount != 1:
                continue
            occurred = row["occurred_at_utc"]
            if occurred.tzinfo is None:
                occurred = occurred.replace(tzinfo=timezone.utc)
            claimed.append(
                OutboxMessage(
                    event_id=str(row["event_id"]),
                    topic=str(row["topic"]),
                    aggregate_type=str(row["aggregate_type"]),
                    aggregate_id=str(row["aggregate_id"]),
                    occurred_at_utc=occurred,
                    payload=dict(row["payload"]),
                    attempts=int(row["attempts"]) + 1,
                )
            )
    return claimed


def mark_outbox_published(
    engine: Engine,
    event_id: str,
    worker_id: str,
    *,
    now: datetime | None = None,
) -> bool:
    published_at = now or _now_utc()
    if not event_id or not worker_id or published_at.tzinfo is None:
        raise ValueError("outbox publish acknowledgement is invalid")
    with engine.begin() as conn:
        result = conn.execute(
            update(outbox_events)
            .where(
                outbox_events.c.event_id == event_id,
                outbox_events.c.lease_owner == worker_id,
                outbox_events.c.published_at_utc.is_(None),
                outbox_events.c.dead_lettered_at_utc.is_(None),
                outbox_events.c.lease_expires_at_utc > published_at,
            )
            .values(
                published_at_utc=published_at,
                lease_owner=None,
                lease_expires_at_utc=None,
                last_error_code=None,
            )
        )
    return result.rowcount == 1


def reschedule_outbox_message(
    engine: Engine,
    event_id: str,
    worker_id: str,
    error_code: str,
    *,
    retry_delay_seconds: int = 5,
    max_attempts: int = 8,
    now: datetime | None = None,
) -> bool:
    failed_at = now or _now_utc()
    if (
        not event_id
        or not worker_id
        or re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", error_code) is None
        or not 1 <= retry_delay_seconds <= 3600
        or not 1 <= max_attempts <= 100
        or failed_at.tzinfo is None
    ):
        raise ValueError("outbox retry acknowledgement is invalid")
    with engine.begin() as conn:
        row = (
            conn.execute(
                select(outbox_events.c.id, outbox_events.c.attempts)
                .where(
                    outbox_events.c.event_id == event_id,
                    outbox_events.c.lease_owner == worker_id,
                    outbox_events.c.published_at_utc.is_(None),
                    outbox_events.c.dead_lettered_at_utc.is_(None),
                    outbox_events.c.lease_expires_at_utc > failed_at,
                )
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return False
        exhausted = int(row["attempts"]) >= max_attempts
        conn.execute(
            update(outbox_events)
            .where(outbox_events.c.id == row["id"])
            .values(
                available_at_utc=failed_at + timedelta(seconds=retry_delay_seconds),
                lease_owner=None,
                lease_expires_at_utc=None,
                dead_lettered_at_utc=failed_at if exhausted else None,
                last_error_code=error_code,
            )
        )
    return True


def rotate_confirmation_capabilities(engine: Engine, cipher: ConfirmationCipher) -> int:
    """Atomically re-encrypt every durable capability with the primary key."""
    if cipher.key_count < 2:
        return 0
    with engine.begin() as conn:
        rows = conn.execute(
            select(
                confirmation_capabilities.c.order_id,
                confirmation_capabilities.c.token_ciphertext,
            ).with_for_update()
        ).mappings()
        rotated = 0
        for row in rows:
            current = str(row["token_ciphertext"])
            if not cipher.requires_rotation(current):
                continue
            ciphertext = cipher.rotate(current)
            conn.execute(
                update(confirmation_capabilities)
                .where(confirmation_capabilities.c.order_id == row["order_id"])
                .values(token_ciphertext=ciphertext)
            )
            rotated += 1
    return rotated


def persist_signal(
    engine: Engine,
    ctx: SignalContext,
    regime: RegimeState,
    vol: VolState,
    decision: StrategyDecision,
) -> bool:
    """Persist one signal + its audit event transactionally.

    Returns ``True`` if rows were written, ``False`` if this ``signal_id`` was
    already present (idempotent skip). Raises on serialization or DB errors —
    the caller decides whether a persistence failure should halt the pipeline.
    """
    signal_row, audit_row = build_signal_rows(ctx, regime, vol, decision)
    created = _now_utc()
    signal_row["created_at_utc"] = created
    audit_row["created_at_utc"] = created

    with engine.begin() as conn:
        if conn.dialect.name == "postgresql":
            inserted = (
                conn.execute(
                    postgresql_insert(signals)
                    .values(**signal_row)
                    .on_conflict_do_nothing(index_elements=[signals.c.signal_id])
                    .returning(signals.c.signal_id)
                ).scalar_one_or_none()
                is not None
            )
        elif conn.dialect.name == "sqlite":
            inserted = (
                conn.execute(
                    sqlite_insert(signals)
                    .values(**signal_row)
                    .on_conflict_do_nothing(index_elements=[signals.c.signal_id])
                ).rowcount
                == 1
            )
        else:
            existing = conn.execute(
                select(signals.c.signal_id).where(signals.c.signal_id == ctx.signal_id)
            ).first()
            if existing is not None:
                return False
            conn.execute(insert(signals).values(**signal_row))
            inserted = True
        if not inserted:
            return False
        conn.execute(audit_events.insert().values(**audit_row))
        _write_outbox(
            conn,
            source_event_id=str(audit_row["event_id"]),
            topic="signal.persisted",
            aggregate_type="Signal",
            aggregate_id=ctx.signal_id,
            occurred_at_utc=ctx.occurred_at_utc,
            payload={"signal_id": ctx.signal_id, "session_id": ctx.session_id},
            created_at_utc=created,
        )
    return True


def persist_event_context(engine: Engine, session_id: str, context: EventContext) -> bool:
    """Persist EventContext + immutable audit event in one idempotent transaction."""
    occurred = datetime.fromisoformat(context.generated_at_utc.replace("Z", "+00:00"))
    created = _now_utc()
    payload: dict[str, Any] = context.model_dump(mode="json")
    row = {
        "event_id": context.event_context_id,
        "session_id": session_id,
        "trading_date": date.fromisoformat(context.trading_date),
        "category": context.event_day_type,
        "occurred_at_utc": occurred,
        "source": "event-context-layer",
        "payload": payload,
        "created_at_utc": created,
    }
    audit = {
        "event_id": f"audit_{context.event_context_id}",
        "session_id": session_id,
        "occurred_at_utc": occurred,
        "actor": "application-service",
        "action": "EVENT_CONTEXT_BUILT",
        "entity_type": "EventContext",
        "entity_id": context.event_context_id,
        "from_status": None,
        "to_status": "AVAILABLE" if context.available else "UNAVAILABLE",
        "payload": payload,
        "created_at_utc": created,
    }

    with engine.begin() as conn:
        if conn.dialect.name == "postgresql":
            inserted = (
                conn.execute(
                    postgresql_insert(event_contexts)
                    .values(**row)
                    .on_conflict_do_nothing(index_elements=[event_contexts.c.event_id])
                    .returning(event_contexts.c.event_id)
                ).scalar_one_or_none()
                is not None
            )
        elif conn.dialect.name == "sqlite":
            inserted = (
                conn.execute(
                    sqlite_insert(event_contexts)
                    .values(**row)
                    .on_conflict_do_nothing(index_elements=[event_contexts.c.event_id])
                ).rowcount
                == 1
            )
        else:
            existing = conn.execute(
                select(event_contexts.c.event_id).where(
                    event_contexts.c.event_id == context.event_context_id
                )
            ).first()
            if existing is not None:
                return False
            conn.execute(insert(event_contexts).values(**row))
            inserted = True
        if not inserted:
            return False
        conn.execute(audit_events.insert().values(**audit))
        _write_outbox(
            conn,
            source_event_id=str(audit["event_id"]),
            topic="event_context.built",
            aggregate_type="EventContext",
            aggregate_id=context.event_context_id,
            occurred_at_utc=occurred,
            payload={"event_context_id": context.event_context_id, "available": context.available},
            created_at_utc=created,
        )
    return True


def _broker_name(value: int) -> str:
    names = {
        int(broker_pb2.BROKER_ID_LONGBRIDGE): "longbridge",
        int(broker_pb2.BROKER_ID_IBKR): "ibkr",
    }
    try:
        return names[value]
    except KeyError as exc:
        raise ValueError("broker reconciliation has an invalid broker id") from exc


def persist_broker_reconciliation(engine: Engine, batch: Any) -> list[str]:
    """Atomically persist and compare one Rust-issued full broker snapshot.

    The returned stable mismatch codes are sent back to Rust. An empty list is
    the only result that may reopen broker authority. The snapshot hash is
    independently recomputed from protobuf bytes before any database write.
    """
    if batch.schema_version != "1.0" or not batch.snapshot_protobuf:
        raise ValueError("broker reconciliation batch is incomplete")
    snapshot = broker_pb2.BrokerSnapshot.FromString(batch.snapshot_protobuf)
    broker = _broker_name(int(batch.broker_id))
    if (
        int(snapshot.account.broker_id) != int(batch.broker_id)
        or snapshot.snapshot_sequence != batch.snapshot_sequence
    ):
        raise ValueError("broker reconciliation identity is inconsistent")
    snapshot_hash = sha256(batch.snapshot_protobuf).hexdigest()
    if snapshot_hash != batch.snapshot_hash:
        raise ValueError("broker reconciliation snapshot hash mismatch")
    occurred = datetime.fromisoformat(snapshot.account.occurred_at_utc.replace("Z", "+00:00"))
    expires = datetime.fromisoformat(batch.expires_at_utc.replace("Z", "+00:00"))
    if occurred.tzinfo is None or expires.tzinfo is None or expires <= _now_utc():
        raise ValueError("broker reconciliation batch is expired or has naive time")

    payload = MessageToDict(snapshot, preserving_proto_field_name=True)
    created = _now_utc()
    mismatch_codes: set[str] = set()
    remote_order_ids = [
        broker_order_id
        for item in snapshot.orders
        for broker_order_id in [
            item.broker_order_id,
            *(child.broker_order_id for child in item.child_orders),
        ]
    ]
    remote_fill_ids = [item.fill_id for item in snapshot.fills]
    remote_position_ids = [item.contract_id for item in snapshot.positions]
    if len(remote_order_ids) != len(set(remote_order_ids)):
        mismatch_codes.add("DUPLICATE_BROKER_ORDER_ID")
    if len(remote_fill_ids) != len(set(remote_fill_ids)):
        mismatch_codes.add("DUPLICATE_BROKER_FILL_ID")
    if len(remote_position_ids) != len(set(remote_position_ids)):
        mismatch_codes.add("DUPLICATE_BROKER_POSITION_ID")

    with engine.begin() as conn:
        existing = conn.execute(
            select(broker_snapshots.c.payload).where(
                broker_snapshots.c.broker_id == broker,
                broker_snapshots.c.snapshot_sequence == batch.snapshot_sequence,
                broker_snapshots.c.snapshot_hash == snapshot_hash,
            )
        ).scalar_one_or_none()
        if existing is not None:
            if existing.get("snapshot") != payload:
                raise ValueError("persisted broker snapshot identity conflicts")
            return [str(code) for code in existing.get("mismatch_codes", [])]

        local_rows = conn.execute(
            select(
                orders.c.order_id,
                orders.c.session_id,
                orders.c.status,
                orders.c.broker_order_id,
                orders.c.payload,
            )
        ).mappings()
        local_by_broker_id: dict[str, tuple[str, str]] = {}
        active_local_ids: set[str] = set()
        active_states = {
            "SUBMITTING",
            "WORKING",
            "PARTIAL_FILL",
            "CANCEL_PENDING",
            "RECONCILE_PENDING",
        }
        for row in local_rows:
            order_payload = row["payload"] if isinstance(row["payload"], dict) else {}
            if order_payload.get("broker_id") != broker:
                continue
            ids = [str(row["broker_order_id"] or "")]
            child_ids = order_payload.get("broker_child_order_ids", [])
            if isinstance(child_ids, list):
                ids.extend(str(value) for value in child_ids)
            for broker_order_id in {value for value in ids if value}:
                existing_identity = local_by_broker_id.get(broker_order_id)
                if existing_identity is not None and existing_identity[0] != str(row["order_id"]):
                    mismatch_codes.add("LOCAL_BROKER_ORDER_ID_CONFLICT")
                local_by_broker_id[broker_order_id] = (
                    str(row["order_id"]),
                    str(row["session_id"]),
                )
                if str(row["status"]) in active_states:
                    active_local_ids.add(broker_order_id)

        remote_ids = set(remote_order_ids)
        if active_local_ids - remote_ids:
            mismatch_codes.add("LOCAL_ACTIVE_ORDER_MISSING_AT_BROKER")
        if remote_ids - set(local_by_broker_id):
            mismatch_codes.add("UNKNOWN_ACTIVE_BROKER_ORDER")
        if any(
            fill.broker_order_id not in local_by_broker_id
            and fill.broker_order_id not in remote_ids
            for fill in snapshot.fills
        ):
            mismatch_codes.add("UNKNOWN_BROKER_FILL")
        if snapshot.fills:
            persisted_fills = {
                str(row["fill_id"]): row
                for row in conn.execute(
                    select(fills).where(
                        fills.c.fill_id.in_([f"{broker}:{fill.fill_id}" for fill in snapshot.fills])
                    )
                ).mappings()
            }
            for fill in snapshot.fills:
                existing_fill = persisted_fills.get(f"{broker}:{fill.fill_id}")
                if existing_fill is None:
                    continue
                existing_time = existing_fill["occurred_at_utc"]
                if existing_time.tzinfo is None:
                    existing_time = existing_time.replace(tzinfo=timezone.utc)
                incoming_time = datetime.fromisoformat(fill.occurred_at_utc.replace("Z", "+00:00"))
                incoming_side = broker_pb2.OrderSide.Name(fill.side).removeprefix("ORDER_SIDE_")
                if (
                    str(existing_fill["broker_order_id"]) != fill.broker_order_id
                    or str(existing_fill["contract_id"]) != fill.contract_id
                    or str(existing_fill["side"]) != incoming_side
                    or Decimal(str(existing_fill["quantity"])) != Decimal(fill.quantity)
                    or Decimal(str(existing_fill["price"])) != Decimal(fill.price)
                    or existing_time != incoming_time
                ):
                    mismatch_codes.add("BROKER_FILL_IDENTITY_CONFLICT")

        mismatches = sorted(mismatch_codes)
        sessions = {session_id for _, session_id in local_by_broker_id.values()}
        session_id = next(iter(sessions)) if len(sessions) == 1 else None
        snapshot_payload = {"snapshot": payload, "mismatch_codes": mismatches}
        conn.execute(
            broker_snapshots.insert().values(
                session_id=session_id,
                occurred_at_utc=occurred,
                broker_health=broker_pb2.BrokerHealth.Name(snapshot.account.health).removeprefix(
                    "BROKER_HEALTH_"
                ),
                buying_power=snapshot.account.buying_power,
                payload=snapshot_payload,
                created_at_utc=created,
                broker_id=broker,
                snapshot_sequence=batch.snapshot_sequence,
                snapshot_hash=snapshot_hash,
                net_liquidation=snapshot.account.net_liquidation,
                reconciled=not mismatches,
                mismatch_codes=mismatches,
            )
        )
        for position in snapshot.positions:
            conn.execute(
                position_snapshots.insert().values(
                    session_id=session_id,
                    occurred_at_utc=occurred,
                    symbol=position.contract_id,
                    quantity=position.quantity,
                    avg_price=position.average_price,
                    unrealized_pnl=None,
                    payload=MessageToDict(position, preserving_proto_field_name=True),
                    created_at_utc=created,
                    broker_id=broker,
                    snapshot_sequence=batch.snapshot_sequence,
                    snapshot_hash=snapshot_hash,
                    contract_id=position.contract_id,
                )
            )
        for fill in snapshot.fills:
            identity = local_by_broker_id.get(fill.broker_order_id)
            fill_row = {
                "fill_id": f"{broker}:{fill.fill_id}",
                "order_id": identity[0] if identity else None,
                "session_id": identity[1] if identity else None,
                "occurred_at_utc": datetime.fromisoformat(
                    fill.occurred_at_utc.replace("Z", "+00:00")
                ),
                "quantity": fill.quantity,
                "price": fill.price,
                "payload": MessageToDict(fill, preserving_proto_field_name=True),
                "created_at_utc": created,
                "broker_id": broker,
                "broker_order_id": fill.broker_order_id,
                "contract_id": fill.contract_id,
                "side": broker_pb2.OrderSide.Name(fill.side).removeprefix("ORDER_SIDE_"),
                "snapshot_hash": snapshot_hash,
            }
            fill_inserted = False
            if conn.dialect.name == "postgresql":
                fill_inserted = (
                    conn.execute(
                        postgresql_insert(fills)
                        .values(**fill_row)
                        .on_conflict_do_nothing(index_elements=[fills.c.fill_id])
                    ).rowcount
                    == 1
                )
            elif conn.dialect.name == "sqlite":
                fill_inserted = (
                    conn.execute(
                        sqlite_insert(fills)
                        .values(**fill_row)
                        .on_conflict_do_nothing(index_elements=[fills.c.fill_id])
                    ).rowcount
                    == 1
                )
            else:
                if (
                    conn.execute(
                        select(fills.c.fill_id).where(fills.c.fill_id == fill_row["fill_id"])
                    ).first()
                    is None
                ):
                    conn.execute(fills.insert().values(**fill_row))
                    fill_inserted = True
            if fill_inserted:
                _write_outbox(
                    conn,
                    source_event_id=str(fill_row["fill_id"]),
                    topic="broker.fill_recorded",
                    aggregate_type="ExecutionOrder" if identity else "BrokerOrder",
                    aggregate_id=identity[0] if identity else fill.broker_order_id,
                    occurred_at_utc=fill_row["occurred_at_utc"],
                    payload={
                        "fill_id": fill_row["fill_id"],
                        "broker_id": broker,
                        "broker_order_id": fill.broker_order_id,
                        "contract_id": fill.contract_id,
                    },
                    created_at_utc=created,
                )
        audit_event_id = f"broker_reconcile_{broker}_{snapshot_hash[:24]}"
        conn.execute(
            audit_events.insert().values(
                event_id=audit_event_id,
                session_id=session_id,
                occurred_at_utc=occurred,
                actor="broker-reconciliation-supervisor",
                action="BROKER_SNAPSHOT_RECONCILED" if not mismatches else "BROKER_SNAPSHOT_DIFF",
                entity_type="BrokerSnapshot",
                entity_id=f"{broker}:{batch.snapshot_sequence}",
                from_status="RECONCILING",
                to_status="HEALTHY" if not mismatches else "RECONCILING",
                payload={"snapshot_hash": snapshot_hash, "mismatch_codes": mismatches},
                created_at_utc=created,
            )
        )
        _write_outbox(
            conn,
            source_event_id=audit_event_id,
            topic="broker.snapshot_reconciled" if not mismatches else "broker.snapshot_diff",
            aggregate_type="BrokerSnapshot",
            aggregate_id=f"{broker}:{batch.snapshot_sequence}",
            occurred_at_utc=occurred,
            payload={
                "broker_id": broker,
                "snapshot_sequence": batch.snapshot_sequence,
                "snapshot_hash": snapshot_hash,
                "mismatch_codes": mismatches,
            },
            created_at_utc=created,
        )
    return sorted(mismatch_codes)


def persist_broker_reconciliation_failure(
    engine: Engine, broker: str, reason_code: str, *, order_id: str | None = None
) -> None:
    """Append a sanitized failed-attempt record without storing exception text."""
    if broker not in {"ibkr", "longbridge"} or not reason_code.isupper():
        raise ValueError("broker reconciliation failure code is invalid")
    occurred = _now_utc()
    audit_event_id = f"broker_reconcile_failure_{uuid4().hex}"
    with engine.begin() as conn:
        conn.execute(
            audit_events.insert().values(
                event_id=audit_event_id,
                session_id=None,
                occurred_at_utc=occurred,
                actor="broker-reconciliation-supervisor",
                action="BROKER_RECONCILIATION_FAILED",
                entity_type="ExecutionOrder" if order_id else "BrokerAccount",
                entity_id=order_id or broker,
                from_status="RECONCILING",
                to_status="RECONCILING",
                payload={"broker_id": broker, "reason_code": reason_code},
                created_at_utc=occurred,
            )
        )
        _write_outbox(
            conn,
            source_event_id=audit_event_id,
            topic="broker.reconciliation_failed",
            aggregate_type="ExecutionOrder" if order_id else "BrokerAccount",
            aggregate_id=order_id or broker,
            occurred_at_utc=occurred,
            payload={"broker_id": broker, "reason_code": reason_code},
            created_at_utc=occurred,
        )


def pending_reconciliation_orders(engine: Engine) -> list[tuple[str, str]]:
    """Return durable unresolved order/broker identities deterministically."""
    with engine.connect() as conn:
        rows = conn.execute(
            select(orders.c.order_id, orders.c.payload)
            .where(orders.c.status == "RECONCILE_PENDING")
            .order_by(orders.c.order_id)
        ).mappings()
        result: list[tuple[str, str]] = []
        for row in rows:
            payload = row["payload"] if isinstance(row["payload"], dict) else {}
            broker = payload.get("broker_id")
            if broker not in {"ibkr", "longbridge"}:
                raise ValueError("pending order has no valid durable broker route")
            result.append((str(row["order_id"]), str(broker)))
        return result


def persist_staged_candidate(
    engine: Engine,
    plan: CandidateTradePlan,
    result: StageCandidateResult,
    cipher: ConfirmationCipher,
) -> bool:
    """Persist plan, initial Rust risk, optional order and audit atomically.

    The opaque confirmation token is encrypted into a short-lived shared store;
    plaintext never enters PostgreSQL, REST, audit payloads or logs.
    Repeating an identical deterministic plan is an idempotent no-op.
    """
    decision = result.initial_risk_decision
    if decision.plan_id != plan.plan_id or decision.plan_hash != plan.plan_hash:
        raise ValueError("risk decision does not match candidate plan")
    if result.order is not None and (
        result.order.plan_id != plan.plan_id
        or result.order.plan_hash != plan.plan_hash
        or result.order.idempotency_key != plan.idempotency_key
        or result.order.session_id != plan.session_id
        or result.order.broker_id != plan.broker_id
        or result.order.execution_mode != plan.execution_mode
        or result.order.total_quantity != plan.legs[0].quantity
    ):
        raise ValueError("execution order does not match candidate plan")
    occurred = datetime.fromisoformat(decision.occurred_at_utc.replace("Z", "+00:00"))
    created = _now_utc()
    plan_payload = plan.model_dump(mode="json", exclude_none=True)
    decision_payload = decision.model_dump(mode="json")
    status = result.order.state if result.order is not None else "RISK_REJECTED"

    with engine.begin() as conn:
        existing = conn.execute(
            select(candidate_trade_plans.c.payload).where(
                candidate_trade_plans.c.plan_id == plan.plan_id
            )
        ).scalar_one_or_none()
        if existing is not None:
            if existing != plan_payload:
                raise ValueError("plan_id already exists with different payload")
            return False
        conn.execute(
            candidate_trade_plans.insert().values(
                plan_id=plan.plan_id,
                signal_id=plan.signal_id,
                session_id=plan.session_id,
                status=status,
                strategy_kind=plan.strategy,
                plan_hash=plan.plan_hash,
                idempotency_key=plan.idempotency_key,
                execution_mode=plan.execution_mode,
                expires_at_utc=datetime.fromisoformat(plan.expires_at_utc.replace("Z", "+00:00")),
                created_at_utc=datetime.fromisoformat(plan.created_at_utc.replace("Z", "+00:00")),
                payload=plan_payload,
            )
        )
        conn.execute(
            risk_decisions.insert().values(
                plan_id=plan.plan_id,
                session_id=plan.session_id,
                occurred_at_utc=occurred,
                decision=decision.decision,
                reason_code=",".join(decision.reason_codes) or None,
                payload=decision_payload,
                created_at_utc=created,
            )
        )
        audit_event_id = f"audit_{decision.decision_id}"
        conn.execute(
            audit_events.insert().values(
                event_id=audit_event_id,
                session_id=plan.session_id,
                occurred_at_utc=occurred,
                actor="rust-risk-gateway",
                action="CANDIDATE_STAGED",
                entity_type="CandidateTradePlan",
                entity_id=plan.plan_id,
                from_status=None,
                to_status=status,
                payload={"plan_hash": plan.plan_hash, "decision": decision_payload},
                created_at_utc=created,
            )
        )
        _write_outbox(
            conn,
            source_event_id=audit_event_id,
            topic="candidate.staged",
            aggregate_type="CandidateTradePlan",
            aggregate_id=plan.plan_id,
            occurred_at_utc=occurred,
            payload={"plan_id": plan.plan_id, "plan_hash": plan.plan_hash, "status": status},
            created_at_utc=created,
        )
        _write_outbox(
            conn,
            source_event_id=decision.decision_id,
            topic="risk.decision_recorded",
            aggregate_type="CandidateTradePlan",
            aggregate_id=plan.plan_id,
            occurred_at_utc=occurred,
            payload={
                "decision_id": decision.decision_id,
                "decision": decision.decision,
                "reason_codes": decision.reason_codes,
            },
            created_at_utc=created,
        )
        if result.order is not None:
            order = result.order
            conn.execute(
                orders.insert().values(
                    order_id=order.order_id,
                    plan_id=order.plan_id,
                    session_id=order.session_id,
                    idempotency_key=order.idempotency_key,
                    status=order.state,
                    side="COMBO",
                    quantity=order.total_quantity,
                    filled_quantity=order.filled_quantity,
                    state_version=order.state_version,
                    limit_price=plan.limit_price,
                    broker_order_id=order.broker_order_id,
                    payload=order.model_dump(mode="json"),
                    created_at_utc=created,
                    updated_at_utc=datetime.fromisoformat(
                        order.updated_at_utc.replace("Z", "+00:00")
                    ),
                )
            )
            conn.execute(
                confirmation_capabilities.insert().values(
                    order_id=order.order_id,
                    plan_hash=order.plan_hash,
                    token_ciphertext=cipher.encrypt(result.confirmation_token),
                    expires_at_utc=datetime.fromisoformat(
                        order.expires_at_utc.replace("Z", "+00:00")
                    ),
                    claimed_at_utc=None,
                    created_at_utc=created,
                )
            )
            conn.execute(
                order_events.insert().values(
                    order_id=order.order_id,
                    occurred_at_utc=occurred,
                    event_type="CANDIDATE_STAGED",
                    from_status=None,
                    to_status=order.state,
                    payload=order.model_dump(mode="json"),
                    created_at_utc=created,
                )
            )
            _write_outbox(
                conn,
                source_event_id=f"{order.order_id}:state:{order.state_version}",
                topic="order.staged",
                aggregate_type="ExecutionOrder",
                aggregate_id=order.order_id,
                occurred_at_utc=occurred,
                payload={
                    "order_id": order.order_id,
                    "state": order.state,
                    "state_version": order.state_version,
                },
                created_at_utc=created,
            )
    return True


def claim_confirmation_intent(
    engine: Engine,
    order_id: str,
    plan_hash: str,
    actor: str,
    cipher: ConfirmationCipher,
) -> str | None:
    """Atomically claim a shared capability and audit operator intent.

    A claim is deliberately not released after an uncertain gRPC outcome. The
    caller must reconcile against Rust before another submission attempt.
    """
    if not actor or not order_id or len(plan_hash) != 64:
        raise ValueError("confirmation intent fields are invalid")
    event_id = f"confirm_{order_id}_{plan_hash[:16]}"
    occurred = _now_utc()
    with engine.begin() as conn:
        row = (
            conn.execute(
                select(
                    orders.c.status,
                    orders.c.session_id,
                    confirmation_capabilities.c.plan_hash,
                    confirmation_capabilities.c.token_ciphertext,
                    confirmation_capabilities.c.expires_at_utc,
                    confirmation_capabilities.c.claimed_at_utc,
                )
                .join(
                    confirmation_capabilities,
                    confirmation_capabilities.c.order_id == orders.c.order_id,
                )
                .where(orders.c.order_id == order_id)
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if row is None or row["status"] != "AWAITING_CONFIRMATION":
            return None
        if row["plan_hash"] != plan_hash:
            raise ValueError("confirmation plan hash conflicts with staged capability")
        expires_at = row["expires_at_utc"]
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at <= occurred:
            conn.execute(
                delete(confirmation_capabilities).where(
                    confirmation_capabilities.c.order_id == order_id
                )
            )
            return None
        if row["claimed_at_utc"] is not None:
            return None
        existing = conn.execute(
            select(audit_events.c.event_id).where(audit_events.c.event_id == event_id)
        ).first()
        if existing is not None:
            raise ValueError("confirmation intent exists without a claimed capability")
        token = cipher.decrypt(str(row["token_ciphertext"]))
        claimed = conn.execute(
            update(confirmation_capabilities)
            .where(
                confirmation_capabilities.c.order_id == order_id,
                confirmation_capabilities.c.claimed_at_utc.is_(None),
            )
            .values(claimed_at_utc=occurred)
        )
        if claimed.rowcount != 1:
            return None
        conn.execute(
            audit_events.insert().values(
                event_id=event_id,
                session_id=row["session_id"],
                occurred_at_utc=occurred,
                actor=actor,
                action="CONFIRMATION_REQUESTED",
                entity_type="ExecutionOrder",
                entity_id=order_id,
                from_status="AWAITING_CONFIRMATION",
                to_status="CONFIRMATION_PENDING",
                payload={"plan_hash": plan_hash},
                created_at_utc=occurred,
            )
        )
        _write_outbox(
            conn,
            source_event_id=event_id,
            topic="confirmation.requested",
            aggregate_type="ExecutionOrder",
            aggregate_id=order_id,
            occurred_at_utc=occurred,
            payload={"order_id": order_id, "plan_hash": plan_hash},
            created_at_utc=occurred,
        )
    return token


def persist_order_projection(
    engine: Engine,
    order: ExecutionOrder,
    *,
    action: str,
    actor: str,
) -> bool:
    """Update one Rust order projection and append matching order/audit events."""
    order = ExecutionOrder.model_validate(order.model_dump(mode="json"))
    occurred = datetime.fromisoformat(order.updated_at_utc.replace("Z", "+00:00"))
    created = _now_utc()
    payload = order.model_dump(mode="json")
    with engine.begin() as conn:
        current = (
            conn.execute(
                select(orders).where(orders.c.order_id == order.order_id).with_for_update()
            )
            .mappings()
            .one()
        )
        if (
            current["plan_id"] != order.plan_id
            or current["idempotency_key"] != order.idempotency_key
        ):
            raise ValueError("Rust order projection conflicts with persisted identity")
        current_order = ExecutionOrder.model_validate(current["payload"])
        if (
            current_order.plan_hash != order.plan_hash
            or current_order.session_id != order.session_id
            or current_order.broker_id != order.broker_id
            or current_order.execution_mode != order.execution_mode
            or current_order.total_quantity != order.total_quantity
            or current_order.expires_at_utc != order.expires_at_utc
        ):
            raise ValueError("Rust order projection changed immutable order fields")
        current_version = int(current["state_version"])
        if current_order.state_version != current_version:
            raise ValueError("order state_version column conflicts with payload")
        if order.state_version < current_version:
            return False
        if order.state_version == current_version:
            current_content = current_order.model_dump(mode="json", exclude={"updated_at_utc"})
            incoming_content = order.model_dump(mode="json", exclude={"updated_at_utc"})
            if current_content != incoming_content:
                raise ValueError("Rust reused an order state_version with conflicting content")
            return False
        if order.filled_quantity < current_order.filled_quantity:
            raise ValueError("Rust order projection reduced filled quantity")
        incoming_children = {child.broker_order_id: child for child in order.broker_child_orders}
        for child in current_order.broker_child_orders:
            incoming = incoming_children.get(child.broker_order_id)
            if incoming is None:
                raise ValueError("Rust order projection removed a broker child order")
            if incoming.filled_quantity < child.filled_quantity:
                raise ValueError("Rust order projection reduced child filled quantity")
        if current_order.residual_exposure and not order.residual_exposure:
            resolved_flat = (
                order.state == "FILLED"
                and order.broker_order_id is not None
                and order.filled_quantity == order.total_quantity
                and all(
                    child.state == "FILLED" and child.filled_quantity == child.quantity
                    for child in order.broker_child_orders
                )
            ) or (
                order.state in {"CANCELLED", "REJECTED"}
                and order.filled_quantity == 0
                and all(child.filled_quantity == 0 for child in order.broker_child_orders)
            )
            if not resolved_flat:
                raise ValueError("Rust cleared residual exposure without terminal flat proof")
        from_status = str(current["status"])
        updated = conn.execute(
            update(orders)
            .where(
                orders.c.order_id == order.order_id,
                orders.c.state_version == current_version,
            )
            .values(
                status=order.state,
                filled_quantity=order.filled_quantity,
                state_version=order.state_version,
                broker_order_id=order.broker_order_id,
                payload=payload,
                updated_at_utc=occurred,
            )
        )
        if updated.rowcount != 1:
            raise ValueError("concurrent order projection update lost version arbitration")
        conn.execute(
            update(candidate_trade_plans)
            .where(candidate_trade_plans.c.plan_id == order.plan_id)
            .values(status=order.state)
        )
        if order.state != "AWAITING_CONFIRMATION":
            conn.execute(
                delete(confirmation_capabilities).where(
                    confirmation_capabilities.c.order_id == order.order_id
                )
            )
        conn.execute(
            order_events.insert().values(
                order_id=order.order_id,
                occurred_at_utc=occurred,
                event_type=action,
                from_status=from_status,
                to_status=order.state,
                payload=payload,
                created_at_utc=created,
            )
        )
        audit_event_id = (
            "audit_"
            + sha256(
                f"{order.order_id}|{from_status}|{order.state}|{action}|{order.state_version}|{order.updated_at_utc}".encode()
            ).hexdigest()
        )
        conn.execute(
            audit_events.insert().values(
                event_id=audit_event_id,
                session_id=order.session_id,
                occurred_at_utc=occurred,
                actor=actor,
                action=action,
                entity_type="ExecutionOrder",
                entity_id=order.order_id,
                from_status=from_status,
                to_status=order.state,
                payload=payload,
                created_at_utc=created,
            )
        )
        _write_outbox(
            conn,
            source_event_id=audit_event_id,
            topic="order.projected",
            aggregate_type="ExecutionOrder",
            aggregate_id=order.order_id,
            occurred_at_utc=occurred,
            payload={
                "order_id": order.order_id,
                "state": order.state,
                "state_version": order.state_version,
                "action": action,
            },
            created_at_utc=created,
        )
    return True


def latest_order_projection(
    engine: Engine, *, session_id: str | None = None
) -> ExecutionOrder | None:
    """Read the newest durable Rust projection for cockpit recovery."""
    query = select(order_events.c.payload).order_by(
        order_events.c.occurred_at_utc.desc(), order_events.c.id.desc()
    )
    if session_id is not None:
        query = query.join(orders, orders.c.order_id == order_events.c.order_id).where(
            orders.c.session_id == session_id
        )
    with engine.connect() as conn:
        payload = conn.execute(query.limit(1)).scalar_one_or_none()
    return ExecutionOrder.model_validate(payload) if payload is not None else None


def latest_execution_ticket(
    engine: Engine, *, session_id: str | None = None
) -> tuple[CandidateTradePlan, ExecutionOrder] | None:
    """Return the newest plan plus Rust order projection for operator review."""
    query = (
        select(candidate_trade_plans.c.payload, orders.c.payload)
        .join(orders, orders.c.plan_id == candidate_trade_plans.c.plan_id)
        .where(orders.c.payload.is_not(None))
        .order_by(orders.c.updated_at_utc.desc())
    )
    if session_id is not None:
        query = query.where(orders.c.session_id == session_id)
    with engine.connect() as conn:
        row = conn.execute(query.limit(1)).one_or_none()
    if row is None:
        return None
    return CandidateTradePlan.model_validate(row[0]), ExecutionOrder.model_validate(row[1])


def staged_plan_projection(
    engine: Engine, plan_id: str
) -> tuple[str, ExecutionOrder | None] | None:
    """Return durable plan status/order before any attempt to restage it."""
    with engine.connect() as conn:
        status = conn.execute(
            select(candidate_trade_plans.c.status).where(candidate_trade_plans.c.plan_id == plan_id)
        ).scalar_one_or_none()
        if status is None:
            return None
        payload = conn.execute(
            select(orders.c.payload).where(orders.c.plan_id == plan_id)
        ).scalar_one_or_none()
    order = ExecutionOrder.model_validate(payload) if payload is not None else None
    return str(status), order


def restorable_execution_workflow(
    engine: Engine, cipher: ConfirmationCipher
) -> list[tuple[CandidateTradePlan, ExecutionOrder, str]]:
    """Load the durable workflow needed to rebuild Rust after a process restart.

    Only an unclaimed, unexpired confirmation capability is decrypted. Every
    other non-terminal order is restored without a capability and Rust forces
    it into reconciliation before any further action.
    """
    now = _now_utc()
    query = (
        select(
            candidate_trade_plans.c.payload.label("plan_payload"),
            orders.c.payload.label("order_payload"),
            confirmation_capabilities.c.token_ciphertext,
            confirmation_capabilities.c.expires_at_utc.label("capability_expires_at"),
            confirmation_capabilities.c.claimed_at_utc,
        )
        .join(orders, orders.c.plan_id == candidate_trade_plans.c.plan_id)
        .outerjoin(
            confirmation_capabilities,
            confirmation_capabilities.c.order_id == orders.c.order_id,
        )
        .order_by(orders.c.created_at_utc, orders.c.order_id)
    )
    restored: list[tuple[CandidateTradePlan, ExecutionOrder, str]] = []
    with engine.connect() as conn:
        rows = conn.execute(query).mappings().all()
    for row in rows:
        plan = CandidateTradePlan.model_validate(row["plan_payload"])
        order = ExecutionOrder.model_validate(row["order_payload"])
        token = ""
        expires_at = row["capability_expires_at"]
        if isinstance(expires_at, str):
            expires_at = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        if isinstance(expires_at, datetime) and expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if (
            order.state == "AWAITING_CONFIRMATION"
            and row["token_ciphertext"] is not None
            and row["claimed_at_utc"] is None
            and isinstance(expires_at, datetime)
            and expires_at > now
        ):
            token = cipher.decrypt(str(row["token_ciphertext"]))
        restored.append((plan, order, token))
    return restored


__all__ = [
    "OutboxMessage",
    "claim_outbox_batch",
    "claim_confirmation_intent",
    "mark_outbox_published",
    "persist_event_context",
    "persist_order_projection",
    "persist_signal",
    "persist_staged_candidate",
    "reschedule_outbox_message",
    "restorable_execution_workflow",
    "rotate_confirmation_capabilities",
    "latest_order_projection",
    "latest_execution_ticket",
    "staged_plan_projection",
]
