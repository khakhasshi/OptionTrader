"""Strict Python mirrors of the Phase 3 execution JSON contracts."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_validator,
)


def _utc_z(value: str) -> str:
    if not value.endswith("Z"):
        raise ValueError("UTC timestamp must end in Z")
    datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value


UtcTimestamp = Annotated[str, AfterValidator(_utc_z)]
DecimalString = Annotated[str, StringConstraints(pattern=r"^-?[0-9]+(\.[0-9]+)?$")]
Hash = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class CandidateLeg(StrictModel):
    side: Literal["BUY", "SELL"]
    type: Literal["CALL", "PUT"]
    contract_id: str = Field(min_length=1)
    expiry: str
    strike: DecimalString
    quantity: int = Field(ge=1)
    quote: OptionQuoteProof
    broker_contract_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    exchange: str | None = Field(default=None, min_length=1)


class OptionQuoteProof(StrictModel):
    bid: DecimalString
    ask: DecimalString
    bid_size: int = Field(ge=1)
    ask_size: int = Field(ge=1)
    occurred_at_utc: UtcTimestamp
    delta: DecimalString
    gamma: DecimalString
    theta: DecimalString
    vega: DecimalString
    chain_snapshot_id: str = Field(min_length=1)


class AdaptiveLimitPolicy(StrictModel):
    initial_aggressiveness_bps: int = Field(ge=0, le=10_000)
    max_attempts: int = Field(ge=1, le=10)
    max_quote_age_ms: int = Field(ge=1, le=5_000)
    max_spread_bps: int = Field(ge=1, le=10_000)


class CandidateTradePlan(StrictModel):
    schema_version: Literal["1.1"]
    plan_id: str = Field(min_length=1)
    plan_hash: Hash
    idempotency_key: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    signal_id: str = Field(min_length=1)
    broker_id: Literal["longbridge", "ibkr"]
    strategy: Literal["LongGamma", "ShortPremium", "EventVolCrush"]
    execution_mode: Literal["REPLAY", "SHADOW", "PAPER", "MANUAL_CONFIRM", "CONTROLLED_AUTO"]
    created_at_utc: UtcTimestamp
    legs: list[CandidateLeg] = Field(min_length=1, max_length=4)
    limit_price: DecimalString
    max_slippage: DecimalString | None = None
    max_loss: DecimalString
    take_profit: DecimalString | None = None
    stop_loss: DecimalString | None = None
    time_stop_minutes: int | None = Field(default=None, ge=0)
    invalidation_rules: list[str] = Field(default_factory=list)
    expires_at_utc: UtcTimestamp
    rule_version: str = Field(min_length=1)
    data_snapshot_ids: list[str] = Field(min_length=1)
    manual_confirmation_required: Literal[True]
    order_side: Literal["BUY", "SELL"]
    order_type: Literal["MARKET", "LIMIT", "ADAPTIVE_LIMIT"]
    adaptive_limit: AdaptiveLimitPolicy | None = None

    @model_validator(mode="after")
    def identifiers_and_times_are_consistent(self) -> CandidateTradePlan:
        created = datetime.fromisoformat(self.created_at_utc.replace("Z", "+00:00"))
        expires = datetime.fromisoformat(self.expires_at_utc.replace("Z", "+00:00"))
        if expires <= created:
            raise ValueError("candidate expiration must be after creation")
        if len(set(self.data_snapshot_ids)) != len(self.data_snapshot_ids):
            raise ValueError("candidate snapshot ids must be unique")
        if len({leg.contract_id for leg in self.legs}) != len(self.legs):
            raise ValueError("candidate contract ids must be unique")
        if len({leg.quantity for leg in self.legs}) != 1:
            raise ValueError("all candidate legs must use the same combo-unit quantity")
        if self.order_type == "ADAPTIVE_LIMIT" and self.adaptive_limit is None:
            raise ValueError("adaptive-limit candidate requires pricing policy")
        if self.order_type != "ADAPTIVE_LIMIT" and self.adaptive_limit is not None:
            raise ValueError("adaptive pricing policy is only valid for adaptive limit")
        if any(leg.quote.chain_snapshot_id not in self.data_snapshot_ids for leg in self.legs):
            raise ValueError("every option chain snapshot must be part of plan proof")
        return self


RiskReason = Literal[
    "DATA_NOT_HEALTHY",
    "BROKER_NOT_HEALTHY",
    "BROKER_NOT_RECONCILED",
    "EVENT_CONTEXT_UNAVAILABLE",
    "EVENT_CONTEXT_INVALID",
    "EVENT_POLICY_BLOCK",
    "PLAN_EXPIRED",
    "PLAN_INVALID",
    "PLAN_HASH_MISMATCH",
    "SNAPSHOT_NOT_CURRENT",
    "EXECUTION_MODE_BLOCKED",
    "DUPLICATE_CONFLICT",
    "RISK_LIMITS_UNCONFIRMED",
    "KILL_SWITCH_ACTIVE",
    "DAILY_LOSS_LIMIT",
    "MAX_TRADES_REACHED",
    "LOSS_COOLDOWN_ACTIVE",
    "PLAN_RISK_LIMIT",
    "OPEN_RISK_LIMIT",
    "BUYING_POWER_INSUFFICIENT",
    "MAX_CONTRACTS_EXCEEDED",
    "RULE_VERSION_MISMATCH",
    "QUOTE_PROOF_INVALID",
    "QUOTE_STALE",
    "QUOTE_SPREAD_TOO_WIDE",
    "GREEKS_INVALID",
    "CHAIN_SNAPSHOT_MISMATCH",
    "STRATEGY_NOT_ALLOWED",
    "ENTRY_WINDOW_CLOSED",
    "MARKET_ORDER_BLOCKED",
]

OrderState = Literal[
    "AWAITING_CONFIRMATION",
    "RISK_REJECTED",
    "APPROVED",
    "SUBMITTING",
    "WORKING",
    "PARTIAL_FILL",
    "FILLED",
    "CANCEL_PENDING",
    "CANCELLED",
    "REJECTED",
    "EXPIRED",
    "RECONCILE_PENDING",
    "SHADOWED",
]


class RiskDecision(StrictModel):
    schema_version: Literal["1.0"]
    decision_id: str
    plan_id: str
    plan_hash: Hash
    session_id: str
    occurred_at_utc: UtcTimestamp
    decision: Literal["APPROVED", "REJECTED"]
    reason_codes: list[RiskReason]
    manual_confirmation_required: Literal[True]
    rule_version: str


class ExecutionOrder(StrictModel):
    schema_version: Literal["1.0"]
    order_id: str = Field(min_length=1)
    plan_id: str = Field(min_length=1)
    plan_hash: Hash
    idempotency_key: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    broker_id: Literal["longbridge", "ibkr"]
    execution_mode: Literal["REPLAY", "SHADOW", "PAPER", "MANUAL_CONFIRM", "CONTROLLED_AUTO"]
    state: OrderState
    total_quantity: int = Field(ge=1)
    filled_quantity: int = Field(ge=0)
    broker_order_id: str | None
    expires_at_utc: UtcTimestamp
    updated_at_utc: UtcTimestamp
    state_version: int = Field(ge=1)
    risk_reason_codes: list[RiskReason]

    @model_validator(mode="after")
    def fill_does_not_exceed_order(self) -> ExecutionOrder:
        if self.filled_quantity > self.total_quantity:
            raise ValueError("filled quantity exceeds total quantity")
        if len(set(self.risk_reason_codes)) != len(self.risk_reason_codes):
            raise ValueError("risk reason codes must be unique")
        return self


class StageCandidateResult(StrictModel):
    initial_risk_decision: RiskDecision
    order: ExecutionOrder | None
    confirmation_token: str

    @model_validator(mode="after")
    def token_matches_stage_outcome(self) -> StageCandidateResult:
        if self.order is None and self.confirmation_token:
            raise ValueError("rejected candidate cannot carry a confirmation token")
        if self.order is not None and not self.confirmation_token:
            raise ValueError("staged candidate requires a confirmation token")
        return self


__all__ = [
    "CandidateLeg",
    "CandidateTradePlan",
    "ExecutionOrder",
    "RiskDecision",
    "StageCandidateResult",
]
