"""Deterministic PostgreSQL facts used to construct automated review requests."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from typing import Any, Literal

from sqlalchemy import select
from sqlalchemy.engine import Connection

from app.events.models import EventContext
from app.llm.models import LLMReviewRequest, ReviewContext, SourceReference
from app.persistence.tables import (
    broker_snapshots,
    candidate_trade_plans,
    event_contexts,
    fills,
    orders,
    risk_decisions,
    signals,
)


_TERMINAL_ORDER_STATES = frozenset(
    {"FILLED", "CANCELLED", "REJECTED", "EXPIRED", "RISK_REJECTED", "SHADOWED"}
)
SourceType = Literal[
    "market_snapshot",
    "option_snapshot",
    "event_context",
    "signal",
    "trade",
    "candidate_trade_plan",
    "risk_decision",
    "broker_snapshot",
    "session_metrics",
]


@dataclass(frozen=True)
class SessionFacts:
    session: dict[str, Any]
    signal_rows: list[dict[str, Any]]
    plan_rows: list[dict[str, Any]]
    order_rows: list[dict[str, Any]]
    fill_rows: list[dict[str, Any]]
    risk_rows: list[dict[str, Any]]
    event_rows: list[dict[str, Any]]
    broker_rows: list[dict[str, Any]]


def load_session_facts(conn: Connection, session: dict[str, Any]) -> SessionFacts:
    session_id = str(session["session_id"])
    trading_day = session["trading_date"]
    signal_rows = _rows(
        conn,
        select(signals)
        .where(signals.c.session_id == session_id)
        .order_by(signals.c.occurred_at_utc, signals.c.signal_id),
    )
    plan_rows = _rows(
        conn,
        select(candidate_trade_plans)
        .where(candidate_trade_plans.c.session_id == session_id)
        .order_by(candidate_trade_plans.c.created_at_utc, candidate_trade_plans.c.plan_id),
    )
    order_rows = _rows(
        conn,
        select(orders)
        .where(orders.c.session_id == session_id)
        .order_by(orders.c.updated_at_utc, orders.c.order_id),
    )
    fill_rows = _rows(
        conn,
        select(fills)
        .where(fills.c.session_id == session_id)
        .order_by(fills.c.occurred_at_utc, fills.c.fill_id),
    )
    risk_rows = _rows(
        conn,
        select(risk_decisions)
        .where(risk_decisions.c.session_id == session_id)
        .order_by(risk_decisions.c.occurred_at_utc, risk_decisions.c.id),
    )
    event_rows = _rows(
        conn,
        select(event_contexts)
        .where(
            (event_contexts.c.session_id == session_id)
            | (event_contexts.c.trading_date == trading_day)
        )
        .order_by(event_contexts.c.occurred_at_utc, event_contexts.c.event_id),
    )
    broker_rows: list[dict[str, Any]] = []
    broker_ids = sorted(
        {
            str((row.get("payload") or {}).get("broker_id"))
            for row in order_rows
            if (row.get("payload") or {}).get("broker_id")
        }
    )
    for broker in broker_ids:
        row = (
            conn.execute(
                select(broker_snapshots)
                .where(
                    broker_snapshots.c.broker_id == broker,
                    broker_snapshots.c.session_id == session_id,
                )
                .order_by(
                    broker_snapshots.c.occurred_at_utc.desc(),
                    broker_snapshots.c.snapshot_sequence.desc(),
                )
                .limit(1)
            )
            .mappings()
            .one_or_none()
        )
        if row is not None:
            broker_rows.append(dict(row))
    return SessionFacts(
        dict(session),
        signal_rows,
        plan_rows,
        order_rows,
        fill_rows,
        risk_rows,
        event_rows,
        broker_rows,
    )


def post_market_inert_reason(facts: SessionFacts, closed_at: datetime) -> str | None:
    if not facts.signal_rows:
        return "SIGNALS_MISSING"
    if not facts.event_rows:
        return "EVENT_CONTEXT_MISSING"
    try:
        event_context = EventContext.model_validate(facts.event_rows[-1]["payload"])
    except ValueError:
        return "EVENT_CONTEXT_INVALID"
    if not event_context.available:
        return "EVENT_CONTEXT_UNAVAILABLE"
    if any(str(row["status"]) not in _TERMINAL_ORDER_STATES for row in facts.order_rows):
        return "ORDERS_NOT_TERMINAL"
    if any(bool((row.get("payload") or {}).get("residual_exposure")) for row in facts.order_rows):
        return "RESIDUAL_EXPOSURE_UNRESOLVED"
    submitted_orders = [row for row in facts.order_rows if _has_broker_identity(row)]
    for order in submitted_orders:
        broker = str((order.get("payload") or {}).get("broker_id") or "")
        proof = next((row for row in facts.broker_rows if str(row["broker_id"]) == broker), None)
        if proof is None:
            return "BROKER_RECONCILIATION_MISSING"
        occurred = _aware_utc(proof["occurred_at_utc"])
        updated = _aware_utc(order["updated_at_utc"])
        if (
            not bool(proof["reconciled"])
            or str(proof["broker_health"]) != "HEALTHY"
            or list(proof["mismatch_codes"] or [])
            or occurred < max(closed_at, updated)
        ):
            return "BROKER_RECONCILIATION_INCOMPLETE"
    return None


def _has_broker_identity(order: dict[str, Any]) -> bool:
    payload = order.get("payload") or {}
    return bool(
        order.get("broker_order_id")
        or payload.get("broker_order_id")
        or payload.get("broker_child_order_ids")
        or payload.get("broker_child_orders")
    )


def post_market_request(
    facts: SessionFacts, ready_at: datetime, rule_version: str
) -> LLMReviewRequest:
    trading_day = facts.session["trading_date"]
    request_id = f"post_market:{trading_day.isoformat()}:v1"
    context, refs = _review_context(facts)
    return LLMReviewRequest(
        schema_version="1.0",
        request_id=request_id,
        correlation_id=str(facts.session["session_id"]),
        causation_id=None,
        session_id=str(facts.session["session_id"]),
        occurred_at_utc=_utc_z(ready_at),
        received_at_utc=_utc_z(ready_at),
        source="application-service",
        source_sequence=_source_sequence(facts),
        rule_version=rule_version,
        stage="POST_MARKET",
        trading_date=trading_day.isoformat(),
        plan_id=None,
        plan_hash=None,
        context=context,
        source_refs=refs,
    )


def intraday_request(
    facts: SessionFacts, event_rows: list[dict[str, Any]], rule_version: str
) -> LLMReviewRequest:
    identities = [str(row["source_event_id"]) for row in event_rows]
    digest = sha256("\n".join(identities).encode("utf-8")).hexdigest()
    context, fact_refs = _review_context(facts)
    trigger_refs = [
        SourceReference(
            source_id=str(row["source_event_id"]),
            source_type=_source_type(str(row["topic"])),
            source="postgresql-transactional-outbox",
            occurred_at_utc=_utc_z(_aware_utc(row["occurred_at_utc"])),
            raw_ref=f"outbox:{int(row['source_outbox_id'])}",
            confidence=1.0,
        )
        for row in event_rows
    ]
    refs = _dedupe_refs(trigger_refs + fact_refs)[:100]
    occurred = max(_aware_utc(row["occurred_at_utc"]) for row in event_rows)
    received = max(_aware_utc(row["available_at_utc"]) for row in event_rows)
    return LLMReviewRequest(
        schema_version="1.0",
        request_id=f"intraday:{facts.session['session_id']}:{digest[:32]}",
        correlation_id=str(facts.session["session_id"]),
        causation_id=identities[-1],
        session_id=str(facts.session["session_id"]),
        occurred_at_utc=_utc_z(occurred),
        received_at_utc=_utc_z(max(occurred, received)),
        source="application-service",
        source_sequence=max(int(row["source_outbox_id"]) for row in event_rows),
        rule_version=rule_version,
        stage="INTRADAY",
        trading_date=facts.session["trading_date"].isoformat(),
        plan_id=None,
        plan_hash=None,
        context=context,
        source_refs=refs,
    )


def _review_context(facts: SessionFacts) -> tuple[ReviewContext, list[SourceReference]]:
    recent_signals = [
        {
            "signal_id": str(row["signal_id"]),
            "occurred_at_utc": _utc_z(_aware_utc(row["occurred_at_utc"])),
            "regime": row["regime"],
            "vol_state": row["vol_state"],
            "strategy_kind": str(row["strategy_kind"]),
            "no_trade_reason": row["no_trade_reason"],
        }
        for row in facts.signal_rows[-20:]
    ]
    order_items = [
        {
            "kind": "order",
            "order_id": str(row["order_id"]),
            "plan_id": str(row["plan_id"]),
            "status": str(row["status"]),
            "side": str(row["side"]),
            "quantity": str(row["quantity"]),
            "filled_quantity": str(row["filled_quantity"]),
            "state_version": int(row["state_version"]),
            "updated_at_utc": _utc_z(_aware_utc(row["updated_at_utc"])),
        }
        for row in facts.order_rows[-25:]
    ]
    fill_items = [
        {
            "kind": "fill",
            "fill_id": str(row["fill_id"]),
            "order_id": row["order_id"],
            "side": str(row["side"]),
            "quantity": str(row["quantity"]),
            "price": str(row["price"]),
            "occurred_at_utc": _utc_z(_aware_utc(row["occurred_at_utc"])),
        }
        for row in facts.fill_rows[-25:]
    ]
    event_context = (
        EventContext.model_validate(facts.event_rows[-1]["payload"]) if facts.event_rows else None
    )
    metrics = _session_metrics(facts)
    broker_health = {
        str(row["broker_id"]): {
            "health": str(row["broker_health"]),
            "reconciled": bool(row["reconciled"]),
            "mismatch_codes": list(row["mismatch_codes"] or []),
            "occurred_at_utc": _utc_z(_aware_utc(row["occurred_at_utc"])),
        }
        for row in facts.broker_rows
    }
    context = ReviewContext(
        risk_state={"decisions": metrics["risk_decision_counts"]},
        broker_health=broker_health or None,
        candidate_trade_plan=facts.plan_rows[-1]["payload"] if facts.plan_rows else None,
        initial_risk_decision=facts.risk_rows[-1]["payload"] if facts.risk_rows else None,
        event_context=event_context,
        recent_signals=recent_signals,
        recent_trades=(order_items + fill_items)[-50:],
        session_metrics=metrics,
        deterministic_summary=(
            f"session={facts.session['session_id']}; signals={len(facts.signal_rows)}; "
            f"plans={len(facts.plan_rows)}; orders={len(facts.order_rows)}; "
            f"fills={len(facts.fill_rows)}; risks={len(facts.risk_rows)}; "
            f"events={len(facts.event_rows)}; no_trade={metrics['no_trade_count']}"
        ),
    )
    refs: list[SourceReference] = []
    for row in facts.signal_rows[-20:]:
        refs.append(_ref(row, "signal_id", "signal", "trading.signals"))
    for row in facts.plan_rows[-15:]:
        refs.append(
            _ref(
                row,
                "plan_id",
                "candidate_trade_plan",
                "trading.candidate_trade_plans",
            )
        )
    for row in facts.order_rows[-20:]:
        refs.append(_ref(row, "order_id", "trade", "trading.orders"))
    for row in facts.fill_rows[-20:]:
        refs.append(_ref(row, "fill_id", "trade", "trading.fills"))
    for row in facts.risk_rows[-15:]:
        refs.append(
            SourceReference(
                source_id=f"risk:{int(row['id'])}",
                source_type="risk_decision",
                source="risk.risk_decisions",
                occurred_at_utc=_utc_z(_aware_utc(row["occurred_at_utc"])),
                raw_ref=f"postgresql:risk_decisions:{int(row['id'])}",
                confidence=1.0,
            )
        )
    for row in facts.event_rows[-5:]:
        refs.append(_ref(row, "event_id", "event_context", "events.event_contexts"))
    return context, _dedupe_refs(refs)[:100]


def _session_metrics(facts: SessionFacts) -> dict[str, Any]:
    order_counts = Counter(str(row["status"]) for row in facts.order_rows)
    risk_counts = Counter(str(row["decision"]) for row in facts.risk_rows)
    strategy_counts = Counter(str(row["strategy_kind"]) for row in facts.signal_rows)
    no_trade_reasons = sorted(
        {str(row["no_trade_reason"]) for row in facts.signal_rows if row["no_trade_reason"]}
    )[:50]
    return {
        "signal_count": len(facts.signal_rows),
        "plan_count": len(facts.plan_rows),
        "order_count": len(facts.order_rows),
        "fill_count": len(facts.fill_rows),
        "risk_decision_count": len(facts.risk_rows),
        "event_context_count": len(facts.event_rows),
        "no_trade_count": sum(
            1 for row in facts.signal_rows if str(row["strategy_kind"]) == "NoTrade"
        ),
        "strategy_counts": dict(sorted(strategy_counts.items())),
        "order_status_counts": dict(sorted(order_counts.items())),
        "risk_decision_counts": dict(sorted(risk_counts.items())),
        "no_trade_reasons": no_trade_reasons,
        "filled_contracts": _decimal_text(
            sum((Decimal(str(row["quantity"])) for row in facts.fill_rows), Decimal("0"))
        ),
    }


def _ref(
    row: dict[str, Any], identity_key: str, source_type: SourceType, source: str
) -> SourceReference:
    occurred = row.get("occurred_at_utc") or row.get("updated_at_utc") or row.get("created_at_utc")
    return SourceReference(
        source_id=str(row[identity_key]),
        source_type=source_type,
        source=source,
        occurred_at_utc=_utc_z(_aware_utc(occurred)),
        raw_ref=f"postgresql:{source}:{row[identity_key]}",
        confidence=1.0,
    )


def _source_type(topic: str) -> SourceType:
    if topic == "signal.persisted":
        return "signal"
    if topic == "event_context.built":
        return "event_context"
    if topic == "candidate.staged":
        return "candidate_trade_plan"
    if topic == "risk.decision_recorded":
        return "risk_decision"
    if topic.startswith("broker.snapshot") or topic == "broker.reconciliation_failed":
        return "broker_snapshot"
    return "trade"


def _source_sequence(facts: SessionFacts) -> int:
    return sum(
        len(rows)
        for rows in (
            facts.signal_rows,
            facts.plan_rows,
            facts.order_rows,
            facts.fill_rows,
            facts.risk_rows,
            facts.event_rows,
            facts.broker_rows,
        )
    )


def _rows(conn: Connection, query: Any) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(query).mappings()]


def _dedupe_refs(refs: list[SourceReference]) -> list[SourceReference]:
    result: list[SourceReference] = []
    seen: set[str] = set()
    for ref in refs:
        if ref.source_id in seen:
            continue
        seen.add(ref.source_id)
        result.append(ref)
    return result


def _aware_utc(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError("automation timestamp is invalid")
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _utc_z(value: datetime) -> str:
    return _aware_utc(value).isoformat().replace("+00:00", "Z")


def _decimal_text(value: Decimal) -> str:
    return "0" if value == 0 else format(value.normalize(), "f")


__all__ = [
    "SessionFacts",
    "intraday_request",
    "load_session_facts",
    "post_market_inert_reason",
    "post_market_request",
]
