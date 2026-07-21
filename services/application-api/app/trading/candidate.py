"""Deterministic CandidateTradePlan construction and position sizing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from hashlib import sha256

from app.grpc_gen import execution_pb2
from app.trading.models import (
    AdaptiveLimitPolicy,
    CandidateLeg,
    CandidateTradePlan,
    OptionQuoteProof,
)

_MULTIPLIER = Decimal("100")


def _decimal(value: str, label: str, *, allow_zero: bool = False) -> Decimal:
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{label} must be a decimal") from exc
    if not parsed.is_finite() or parsed < 0 or (parsed == 0 and not allow_zero):
        raise ValueError(f"{label} is outside its valid range")
    return parsed


def _fixed(value: Decimal) -> str:
    text = format(value.quantize(Decimal("0.01")), "f")
    return text


@dataclass(frozen=True)
class QuotedLeg:
    side: str
    option_right: str
    contract_id: str
    expiry: str
    strike: str
    bid: str
    ask: str
    bid_size: int
    ask_size: int
    quote_at_utc: datetime
    delta: str
    gamma: str
    theta: str
    vega: str
    chain_snapshot_id: str
    broker_contract_id: str | None = None
    symbol: str = "QQQ"
    exchange: str | None = None
    quote_provider: str = "THETADATA"


@dataclass(frozen=True)
class CandidateInputs:
    session_id: str
    signal_id: str
    strategy: str
    broker_id: str
    execution_mode: str
    occurred_at_utc: datetime
    quoted_legs: tuple[QuotedLeg, ...]
    risk_budget: str
    max_contracts: int
    max_slippage: str
    ttl_seconds: int
    rule_version: str
    data_snapshot_ids: tuple[str, ...]
    order_type: str = "LIMIT"
    adaptive_initial_aggressiveness_bps: int = 3_000
    adaptive_max_attempts: int = 3
    adaptive_max_quote_age_ms: int = 500
    adaptive_max_spread_bps: int = 2_000
    market_data_provider: str = "THETADATA"


def _unit_economics(inputs: CandidateInputs) -> tuple[Decimal, Decimal]:
    buys = Decimal("0")
    sells = Decimal("0")
    for leg in inputs.quoted_legs:
        bid = _decimal(leg.bid, "bid", allow_zero=True)
        ask = _decimal(leg.ask, "ask")
        if ask < bid:
            raise ValueError("candidate leg market is crossed")
        if leg.side == "BUY":
            buys += ask
        elif leg.side == "SELL":
            sells += bid
        else:
            raise ValueError("candidate leg side is unmapped")

    if inputs.strategy == "LongGamma":
        if any(leg.side != "BUY" for leg in inputs.quoted_legs):
            raise ValueError("LongGamma candidate may contain only BUY legs")
        debit = buys - sells
        if debit <= 0:
            raise ValueError("LongGamma candidate must have a positive debit")
        return debit, debit * _MULTIPLIER

    if inputs.strategy not in {"ShortPremium", "EventVolCrush"}:
        raise ValueError("candidate strategy is unmapped")
    if len(inputs.quoted_legs) not in {2, 4}:
        raise ValueError("short-premium candidate must be a defined-risk spread")
    credit = sells - buys
    if credit <= 0:
        raise ValueError("short-premium candidate must have a positive credit")

    widths: list[Decimal] = []
    for short in (leg for leg in inputs.quoted_legs if leg.side == "SELL"):
        hedges = [
            leg
            for leg in inputs.quoted_legs
            if leg.side == "BUY"
            and leg.option_right == short.option_right
            and leg.expiry == short.expiry
        ]
        if len(hedges) != 1:
            raise ValueError("every short option requires exactly one matching hedge")
        short_strike = _decimal(short.strike, "short strike")
        hedge_strike = _decimal(hedges[0].strike, "hedge strike")
        if short.option_right == "CALL" and hedge_strike <= short_strike:
            raise ValueError("short call hedge must use a higher strike")
        if short.option_right == "PUT" and hedge_strike >= short_strike:
            raise ValueError("short put hedge must use a lower strike")
        widths.append(abs(hedge_strike - short_strike))
    if not widths:
        raise ValueError("short-premium candidate has no short option")
    max_loss = (max(widths) - credit) * _MULTIPLIER
    if max_loss <= 0:
        raise ValueError("spread credit must be smaller than its width")
    return credit, max_loss


def _to_proto(
    plan: CandidateTradePlan, *, clear_identity: bool = False
) -> execution_pb2.CandidateTradePlan:
    broker = {
        "longbridge": execution_pb2.BROKER_ID_LONGBRIDGE,
        "ibkr": execution_pb2.BROKER_ID_IBKR,
    }[plan.broker_id]
    strategy = {
        "LongGamma": execution_pb2.STRATEGY_KIND_LONG_GAMMA,
        "ShortPremium": execution_pb2.STRATEGY_KIND_SHORT_PREMIUM,
        "EventVolCrush": execution_pb2.STRATEGY_KIND_EVENT_VOL_CRUSH,
    }[plan.strategy]
    mode = {
        "REPLAY": execution_pb2.EXECUTION_MODE_REPLAY,
        "SHADOW": execution_pb2.EXECUTION_MODE_SHADOW,
        "PAPER": execution_pb2.EXECUTION_MODE_PAPER,
        "MANUAL_CONFIRM": execution_pb2.EXECUTION_MODE_MANUAL_CONFIRM,
        "CONTROLLED_AUTO": execution_pb2.EXECUTION_MODE_CONTROLLED_AUTO,
    }[plan.execution_mode]
    order_type = {
        "MARKET": execution_pb2.BROKER_ORDER_TYPE_MARKET,
        "LIMIT": execution_pb2.BROKER_ORDER_TYPE_LIMIT,
        "ADAPTIVE_LIMIT": execution_pb2.BROKER_ORDER_TYPE_ADAPTIVE_LIMIT,
    }[plan.order_type]
    adaptive = None
    if plan.adaptive_limit is not None:
        adaptive = execution_pb2.AdaptiveLimitPolicy(
            initial_aggressiveness_bps=plan.adaptive_limit.initial_aggressiveness_bps,
            max_attempts=plan.adaptive_limit.max_attempts,
            max_quote_age_ms=plan.adaptive_limit.max_quote_age_ms,
            max_spread_bps=plan.adaptive_limit.max_spread_bps,
        )
    return execution_pb2.CandidateTradePlan(
        schema_version=plan.schema_version,
        plan_id="" if clear_identity else plan.plan_id,
        plan_hash="" if clear_identity else plan.plan_hash,
        idempotency_key="" if clear_identity else plan.idempotency_key,
        session_id=plan.session_id,
        signal_id=plan.signal_id,
        broker_id=broker,
        strategy=strategy,
        execution_mode=mode,
        created_at_utc=plan.created_at_utc,
        legs=[
            execution_pb2.CandidateLeg(
                side=(
                    execution_pb2.ORDER_SIDE_BUY
                    if leg.side == "BUY"
                    else execution_pb2.ORDER_SIDE_SELL
                ),
                option_right=(
                    execution_pb2.OPTION_RIGHT_CALL
                    if leg.type == "CALL"
                    else execution_pb2.OPTION_RIGHT_PUT
                ),
                contract_id=leg.contract_id,
                expiry=leg.expiry,
                strike=leg.strike,
                quantity=leg.quantity,
                quote=execution_pb2.OptionQuoteProof(
                    bid=leg.quote.bid,
                    ask=leg.quote.ask,
                    bid_size=leg.quote.bid_size,
                    ask_size=leg.quote.ask_size,
                    occurred_at_utc=leg.quote.occurred_at_utc,
                    delta=leg.quote.delta,
                    gamma=leg.quote.gamma,
                    theta=leg.quote.theta,
                    vega=leg.quote.vega,
                    chain_snapshot_id=leg.quote.chain_snapshot_id,
                    provider=leg.quote.provider,
                ),
                broker_contract_id=leg.broker_contract_id or "",
                symbol=leg.symbol,
                exchange=leg.exchange or "",
            )
            for leg in plan.legs
        ],
        limit_price=plan.limit_price,
        max_slippage=plan.max_slippage or "",
        max_loss=plan.max_loss,
        take_profit=plan.take_profit or "",
        stop_loss=plan.stop_loss or "",
        time_stop_minutes=plan.time_stop_minutes or 0,
        invalidation_rules=plan.invalidation_rules,
        expires_at_utc=plan.expires_at_utc,
        rule_version=plan.rule_version,
        data_snapshot_ids=plan.data_snapshot_ids,
        manual_confirmation_required=plan.manual_confirmation_required,
        order_side=(
            execution_pb2.ORDER_SIDE_BUY
            if plan.order_side == "BUY"
            else execution_pb2.ORDER_SIDE_SELL
        ),
        order_type=order_type,
        market_data_provider=plan.market_data_provider,
        **({"adaptive_limit": adaptive} if adaptive is not None else {}),
    )


def plan_proto(plan: CandidateTradePlan) -> execution_pb2.CandidateTradePlan:
    return _to_proto(plan)


def canonical_plan_hash(plan: CandidateTradePlan) -> str:
    proto = _to_proto(plan, clear_identity=True)
    return sha256(proto.SerializeToString(deterministic=True)).hexdigest()


def build_candidate_plan(inputs: CandidateInputs) -> CandidateTradePlan:
    if inputs.occurred_at_utc.tzinfo is None:
        raise ValueError("candidate time must be timezone-aware")
    occurred = inputs.occurred_at_utc.astimezone(UTC)
    if not 1 <= len(inputs.quoted_legs) <= 4:
        raise ValueError("candidate requires one to four option legs")
    if not inputs.session_id or not inputs.signal_id or not inputs.rule_version:
        raise ValueError("candidate traceability fields are required")
    if not inputs.data_snapshot_ids or len(set(inputs.data_snapshot_ids)) != len(
        inputs.data_snapshot_ids
    ):
        raise ValueError("candidate requires unique snapshot proof")
    if inputs.execution_mode not in {"REPLAY", "SHADOW", "PAPER", "MANUAL_CONFIRM"}:
        raise ValueError("controlled-auto/live candidate generation is disabled")
    if not 1 <= inputs.ttl_seconds <= 120:
        raise ValueError("candidate TTL must be between 1 and 120 seconds")
    if inputs.order_type not in {"MARKET", "LIMIT", "ADAPTIVE_LIMIT"}:
        raise ValueError("candidate order type is unmapped")
    if inputs.market_data_provider != "THETADATA":
        raise ValueError("candidate market data provider must be THETADATA")
    for leg in inputs.quoted_legs:
        if leg.quote_at_utc.tzinfo is None:
            raise ValueError("option quote time must be timezone-aware")
        if leg.quote_at_utc.astimezone(UTC) > occurred:
            raise ValueError("option quote cannot be from the future")
        if leg.bid_size < 1 or leg.ask_size < 1:
            raise ValueError("option quote sizes must be positive")
        if leg.chain_snapshot_id not in inputs.data_snapshot_ids:
            raise ValueError("option chain snapshot must be part of plan proof")
        if leg.quote_provider != "THETADATA":
            raise ValueError("option quote and Greeks provider must be THETADATA")
        if not leg.broker_contract_id:
            raise ValueError("broker-native contract id is required")
        if inputs.broker_id == "ibkr" and not leg.broker_contract_id.isdigit():
            raise ValueError("IBKR contract id must be a numeric conId")
    limit_price, unit_max_loss = _unit_economics(inputs)
    risk_budget = _decimal(inputs.risk_budget, "risk budget")
    if inputs.max_contracts < 1:
        raise ValueError("max_contracts must be positive")
    quantity = min(
        int((risk_budget / unit_max_loss).to_integral_value(rounding=ROUND_FLOOR)),
        inputs.max_contracts,
    )
    if quantity < 1:
        raise ValueError("risk budget cannot fund one defined-risk unit")
    max_loss = unit_max_loss * quantity
    created = occurred.isoformat(timespec="seconds").replace("+00:00", "Z")
    expires = (
        (occurred + timedelta(seconds=inputs.ttl_seconds))
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )
    placeholder_hash = "0" * 64
    plan = CandidateTradePlan(
        schema_version="1.2",
        plan_id="pending",
        plan_hash=placeholder_hash,
        idempotency_key="pending",
        session_id=inputs.session_id,
        signal_id=inputs.signal_id,
        broker_id=inputs.broker_id,  # type: ignore[arg-type]
        strategy=inputs.strategy,  # type: ignore[arg-type]
        execution_mode=inputs.execution_mode,  # type: ignore[arg-type]
        created_at_utc=created,
        legs=[
            CandidateLeg(
                side=leg.side,  # type: ignore[arg-type]
                type=leg.option_right,  # type: ignore[arg-type]
                contract_id=leg.contract_id,
                expiry=leg.expiry,
                strike=leg.strike,
                quantity=quantity,
                quote=OptionQuoteProof(
                    bid=leg.bid,
                    ask=leg.ask,
                    bid_size=leg.bid_size,
                    ask_size=leg.ask_size,
                    occurred_at_utc=leg.quote_at_utc.astimezone(UTC)
                    .isoformat(timespec="milliseconds")
                    .replace("+00:00", "Z"),
                    delta=leg.delta,
                    gamma=leg.gamma,
                    theta=leg.theta,
                    vega=leg.vega,
                    chain_snapshot_id=leg.chain_snapshot_id,
                    provider="THETADATA",
                ),
                broker_contract_id=leg.broker_contract_id or "",
                symbol=leg.symbol,
                exchange=leg.exchange,
            )
            for leg in inputs.quoted_legs
        ],
        limit_price=_fixed(limit_price),
        max_slippage=_fixed(_decimal(inputs.max_slippage, "max slippage", allow_zero=True)),
        max_loss=_fixed(max_loss),
        take_profit=_fixed(max_loss * Decimal("0.40")),
        stop_loss=_fixed(max_loss * Decimal("0.30")),
        time_stop_minutes=30,
        invalidation_rules=["market_or_event_context_changes", "spread_exceeds_limit"],
        expires_at_utc=expires,
        rule_version=inputs.rule_version,
        data_snapshot_ids=list(inputs.data_snapshot_ids),
        manual_confirmation_required=True,
        order_side="BUY" if inputs.strategy == "LongGamma" else "SELL",
        order_type=inputs.order_type,  # type: ignore[arg-type]
        adaptive_limit=(
            AdaptiveLimitPolicy(
                initial_aggressiveness_bps=inputs.adaptive_initial_aggressiveness_bps,
                max_attempts=inputs.adaptive_max_attempts,
                max_quote_age_ms=inputs.adaptive_max_quote_age_ms,
                max_spread_bps=inputs.adaptive_max_spread_bps,
            )
            if inputs.order_type == "ADAPTIVE_LIMIT"
            else None
        ),
        market_data_provider="THETADATA",
    )
    digest = canonical_plan_hash(plan)
    return plan.model_copy(
        update={
            "plan_id": f"plan_{digest[:24]}",
            "plan_hash": digest,
            "idempotency_key": f"submit_{digest}",
        }
    )


__all__ = [
    "CandidateInputs",
    "QuotedLeg",
    "build_candidate_plan",
    "canonical_plan_hash",
    "plan_proto",
]
