//! Protocol-to-domain mapping and execution workflow projection.

use super::*;

pub(super) fn utc(value: &str, label: &'static str) -> Result<DateTime<Utc>, &'static str> {
    if !value.ends_with('Z') {
        return Err(label);
    }
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| label)
}

pub(super) fn decimal(value: &str, label: &'static str) -> Result<Decimal, &'static str> {
    if value.is_empty()
        || value.starts_with('+')
        || value.contains('e')
        || value.contains('E')
        || value
            .parse::<f64>()
            .map_or(true, |number| !number.is_finite())
    {
        return Err(label);
    }
    Decimal::from_str_exact(value).map_err(|_| label)
}

pub(super) fn broker_legs(
    raw_plan: &ProtoPlan,
    order_type: AdapterOrderType,
    now: DateTime<Utc>,
) -> Result<Vec<BrokerOrderLeg>, BrokerError> {
    raw_plan
        .legs
        .iter()
        .map(|leg| {
            let side = match ProtoSide::try_from(leg.side) {
                Ok(ProtoSide::Buy) => AdapterOrderSide::Buy,
                Ok(ProtoSide::Sell) => AdapterOrderSide::Sell,
                _ => return Err(BrokerError::InvalidOrderType),
            };
            let quote = leg.quote.as_ref().ok_or(BrokerError::QuoteUnavailable)?;
            let bid =
                decimal(&quote.bid, "quote bid").map_err(|_| BrokerError::QuoteUnavailable)?;
            let ask =
                decimal(&quote.ask, "quote ask").map_err(|_| BrokerError::QuoteUnavailable)?;
            let occurred_at = utc(&quote.occurred_at_utc, "quote time")
                .map_err(|_| BrokerError::QuoteUnavailable)?;
            let submitted_price = match order_type {
                AdapterOrderType::Market => None,
                AdapterOrderType::Limit => Some(match side {
                    AdapterOrderSide::Buy => ask,
                    AdapterOrderSide::Sell => bid,
                }),
                AdapterOrderType::AdaptiveLimit => {
                    let raw_policy = raw_plan
                        .adaptive_limit
                        .as_ref()
                        .ok_or(BrokerError::InvalidOrderType)?;
                    let policy = AdapterAdaptivePolicy {
                        initial_aggressiveness_bps: raw_policy.initial_aggressiveness_bps,
                        max_attempts: raw_policy.max_attempts,
                        max_quote_age_ms: raw_policy.max_quote_age_ms,
                        max_spread_bps: raw_policy.max_spread_bps,
                    };
                    let quote = QuoteProof {
                        bid,
                        ask,
                        tick_size: Decimal::new(1, 2),
                        occurred_at,
                    };
                    let protection = match side {
                        AdapterOrderSide::Buy => ask,
                        AdapterOrderSide::Sell => bid,
                    };
                    Some(
                        price_adaptive_limit(side, &quote, &policy, 0, protection, now)
                            .map_err(map_adaptive_error)?,
                    )
                }
            };
            Ok(BrokerOrderLeg {
                contract_id: leg.contract_id.clone(),
                side,
                quantity: leg.quantity,
                broker_contract_id: (!leg.broker_contract_id.is_empty())
                    .then(|| leg.broker_contract_id.clone()),
                symbol: (!leg.symbol.is_empty()).then(|| leg.symbol.clone()),
                exchange: (!leg.exchange.is_empty()).then(|| leg.exchange.clone()),
                submitted_price,
            })
        })
        .collect()
}

pub(super) fn priced_broker_order(
    raw: &ProtoPlan,
    now: DateTime<Utc>,
) -> Result<
    (
        AdapterOrderSide,
        AdapterOrderType,
        Option<Decimal>,
        Vec<BrokerOrderLeg>,
    ),
    BrokerError,
> {
    let side = match ProtoSide::try_from(raw.order_side).ok() {
        Some(ProtoSide::Buy) => AdapterOrderSide::Buy,
        Some(ProtoSide::Sell) => AdapterOrderSide::Sell,
        _ => return Err(BrokerError::InvalidOrderType),
    };
    let protection =
        decimal(&raw.limit_price, "limit_price").map_err(|_| BrokerError::InvalidPrice)?;
    let (order_type, submitted_price) = match ProtoOrderType::try_from(raw.order_type).ok() {
        Some(ProtoOrderType::Market) => (AdapterOrderType::Market, None),
        Some(ProtoOrderType::Limit) => (AdapterOrderType::Limit, Some(protection)),
        Some(ProtoOrderType::AdaptiveLimit) => {
            let policy = raw
                .adaptive_limit
                .as_ref()
                .ok_or(BrokerError::InvalidOrderType)?;
            let mut bids = Decimal::ZERO;
            let mut asks = Decimal::ZERO;
            let mut occurred_at = now;
            for leg in &raw.legs {
                let quote = leg.quote.as_ref().ok_or(BrokerError::QuoteUnavailable)?;
                let bid =
                    decimal(&quote.bid, "quote bid").map_err(|_| BrokerError::QuoteUnavailable)?;
                let ask =
                    decimal(&quote.ask, "quote ask").map_err(|_| BrokerError::QuoteUnavailable)?;
                let quote_at = utc(&quote.occurred_at_utc, "quote time")
                    .map_err(|_| BrokerError::QuoteUnavailable)?;
                occurred_at = occurred_at.min(quote_at);
                let leg_side =
                    ProtoSide::try_from(leg.side).map_err(|_| BrokerError::InvalidOrderType)?;
                match (side, leg_side) {
                    (AdapterOrderSide::Buy, ProtoSide::Buy)
                    | (AdapterOrderSide::Sell, ProtoSide::Sell) => {
                        bids += bid;
                        asks += ask;
                    }
                    (AdapterOrderSide::Buy, ProtoSide::Sell)
                    | (AdapterOrderSide::Sell, ProtoSide::Buy) => {
                        bids -= ask;
                        asks -= bid;
                    }
                    _ => return Err(BrokerError::InvalidOrderType),
                }
            }
            let quote = QuoteProof {
                bid: bids,
                ask: asks,
                tick_size: Decimal::new(1, 2),
                occurred_at,
            };
            let policy = AdapterAdaptivePolicy {
                initial_aggressiveness_bps: policy.initial_aggressiveness_bps,
                max_attempts: policy.max_attempts,
                max_quote_age_ms: policy.max_quote_age_ms,
                max_spread_bps: policy.max_spread_bps,
            };
            let price = price_adaptive_limit(side, &quote, &policy, 0, protection, now)
                .map_err(map_adaptive_error)?;
            (AdapterOrderType::AdaptiveLimit, Some(price))
        }
        _ => return Err(BrokerError::InvalidOrderType),
    };
    let legs = broker_legs(raw, order_type, now)?;
    if let Some(package_price) = submitted_price {
        let mut buys = Decimal::ZERO;
        let mut sells = Decimal::ZERO;
        for leg in &legs {
            let price = leg.submitted_price.ok_or(BrokerError::InvalidPrice)?;
            match leg.side {
                AdapterOrderSide::Buy => buys += price,
                AdapterOrderSide::Sell => sells += price,
            }
        }
        let legged_price = match side {
            AdapterOrderSide::Buy => buys - sells,
            AdapterOrderSide::Sell => sells - buys,
        };
        let violates_protection = match side {
            AdapterOrderSide::Buy => legged_price > package_price,
            AdapterOrderSide::Sell => legged_price < package_price,
        };
        if violates_protection {
            return Err(BrokerError::InvalidPrice);
        }
    }
    Ok((side, order_type, submitted_price, legs))
}

pub(super) fn recovery_pricing_time(raw: &ProtoPlan) -> Result<DateTime<Utc>, BrokerError> {
    raw.legs
        .iter()
        .map(|leg| {
            leg.quote
                .as_ref()
                .ok_or(BrokerError::QuoteUnavailable)
                .and_then(|quote| {
                    utc(&quote.occurred_at_utc, "quote time")
                        .map_err(|_| BrokerError::QuoteUnavailable)
                })
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .min()
        .ok_or(BrokerError::QuoteUnavailable)
}

pub(super) fn map_adaptive_error(error: AdaptivePriceError) -> BrokerError {
    match error {
        AdaptivePriceError::StaleQuote => BrokerError::QuoteStale,
        AdaptivePriceError::CrossedQuote => BrokerError::QuoteCrossed,
        AdaptivePriceError::SpreadTooWide => BrokerError::SpreadTooWide,
        AdaptivePriceError::InvalidQuote => BrokerError::QuoteUnavailable,
        AdaptivePriceError::InvalidPolicy => BrokerError::InvalidOrderType,
        AdaptivePriceError::InvalidProtectionPrice => BrokerError::InvalidPrice,
    }
}

pub(super) fn digest<T: Message + Clone>(message: &T, clear: impl FnOnce(&mut T)) -> String {
    let mut canonical = message.clone();
    clear(&mut canonical);
    format!("{:x}", Sha256::digest(canonical.encode_to_vec()))
}

pub(super) fn map_broker(value: i32) -> Result<BrokerId, &'static str> {
    match ProtoBrokerId::try_from(value).ok() {
        Some(ProtoBrokerId::Longbridge) => Ok(BrokerId::Longbridge),
        Some(ProtoBrokerId::Ibkr) => Ok(BrokerId::Ibkr),
        _ => Err("broker_id"),
    }
}

pub(super) fn map_strategy(value: i32) -> Result<StrategyKind, &'static str> {
    match ProtoStrategy::try_from(value).ok() {
        Some(ProtoStrategy::LongGamma) => Ok(StrategyKind::LongGamma),
        Some(ProtoStrategy::ShortPremium) => Ok(StrategyKind::ShortPremium),
        Some(ProtoStrategy::EventVolCrush) => Ok(StrategyKind::EventVolCrush),
        _ => Err("strategy"),
    }
}

pub(super) fn map_mode(value: i32) -> Result<ExecutionMode, &'static str> {
    match ProtoMode::try_from(value).ok() {
        Some(ProtoMode::Replay) => Ok(ExecutionMode::Replay),
        Some(ProtoMode::Shadow) => Ok(ExecutionMode::Shadow),
        Some(ProtoMode::Paper) => Ok(ExecutionMode::Paper),
        Some(ProtoMode::ManualConfirm) => Ok(ExecutionMode::ManualConfirm),
        Some(ProtoMode::ControlledAuto) => Ok(ExecutionMode::ControlledAuto),
        _ => Err("execution_mode"),
    }
}

pub(super) fn map_side(value: i32) -> Result<OrderSide, &'static str> {
    match ProtoSide::try_from(value).ok() {
        Some(ProtoSide::Buy) => Ok(OrderSide::Buy),
        Some(ProtoSide::Sell) => Ok(OrderSide::Sell),
        _ => Err("side"),
    }
}

pub(super) fn map_right(value: i32) -> Result<OptionRight, &'static str> {
    match ProtoRight::try_from(value).ok() {
        Some(ProtoRight::Call) => Ok(OptionRight::Call),
        Some(ProtoRight::Put) => Ok(OptionRight::Put),
        _ => Err("option_right"),
    }
}

pub(super) fn map_order_type(value: i32) -> Result<BrokerOrderType, &'static str> {
    match ProtoOrderType::try_from(value).ok() {
        Some(ProtoOrderType::Market) => Ok(BrokerOrderType::Market),
        Some(ProtoOrderType::Limit) => Ok(BrokerOrderType::Limit),
        Some(ProtoOrderType::AdaptiveLimit) => Ok(BrokerOrderType::AdaptiveLimit),
        _ => Err("order_type"),
    }
}

pub(super) fn map_position_effect(value: i32) -> Result<PositionEffect, &'static str> {
    match ProtoPositionEffect::try_from(value).ok() {
        Some(ProtoPositionEffect::Open) => Ok(PositionEffect::Open),
        Some(ProtoPositionEffect::Close) => Ok(PositionEffect::Close),
        _ => Err("position_effect"),
    }
}

pub(super) fn adaptive_policy_valid(raw: &ProtoPlan, order_type: BrokerOrderType) -> bool {
    match (order_type, raw.adaptive_limit.as_ref()) {
        (BrokerOrderType::Market | BrokerOrderType::Limit, None) => true,
        (BrokerOrderType::AdaptiveLimit, Some(policy)) => {
            policy.initial_aggressiveness_bps <= 10_000
                && (1..=10).contains(&policy.max_attempts)
                && (1..=5_000).contains(&policy.max_quote_age_ms)
                && (1..=10_000).contains(&policy.max_spread_bps)
        }
        _ => false,
    }
}

pub(super) fn plan(raw: &ProtoPlan) -> Result<CandidatePlan, &'static str> {
    if raw.schema_version != "1.3"
        || raw.plan_hash.len() != 64
        || raw.market_data_provider != "THETADATA"
    {
        return Err("plan contract");
    }
    let expected_hash = digest(raw, |value| {
        value.plan_id.clear();
        value.plan_hash.clear();
        value.idempotency_key.clear();
    });
    let legs = raw
        .legs
        .iter()
        .map(|leg| {
            if chrono::NaiveDate::from_str(&leg.expiry).is_err() {
                return Err("expiry");
            }
            let quote = leg.quote.as_ref().ok_or("option quote proof")?;
            if quote.provider != "THETADATA" {
                return Err("option quote provider");
            }
            Ok(CandidateLeg {
                side: map_side(leg.side)?,
                option_right: map_right(leg.option_right)?,
                contract_id: leg.contract_id.clone(),
                expiry: leg.expiry.clone(),
                strike: decimal(&leg.strike, "strike")?,
                quantity: leg.quantity,
                quote: OptionQuoteProof {
                    bid: decimal(&quote.bid, "quote bid")?,
                    ask: decimal(&quote.ask, "quote ask")?,
                    bid_size: quote.bid_size,
                    ask_size: quote.ask_size,
                    occurred_at: utc(&quote.occurred_at_utc, "quote occurred_at_utc")?,
                    delta: decimal(&quote.delta, "delta")?,
                    gamma: decimal(&quote.gamma, "gamma")?,
                    theta: decimal(&quote.theta, "theta")?,
                    vega: decimal(&quote.vega, "vega")?,
                    chain_snapshot_id: quote.chain_snapshot_id.clone(),
                    provider: quote.provider.clone(),
                },
                broker_contract_id: (!leg.broker_contract_id.is_empty())
                    .then(|| leg.broker_contract_id.clone()),
                symbol: leg.symbol.clone(),
                exchange: (!leg.exchange.is_empty()).then(|| leg.exchange.clone()),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let order_type = map_order_type(raw.order_type)?;
    Ok(CandidatePlan {
        plan_id: raw.plan_id.clone(),
        plan_hash: raw.plan_hash.clone(),
        idempotency_key: raw.idempotency_key.clone(),
        session_id: raw.session_id.clone(),
        signal_id: raw.signal_id.clone(),
        broker_id: map_broker(raw.broker_id)?,
        strategy: map_strategy(raw.strategy)?,
        execution_mode: map_mode(raw.execution_mode)?,
        created_at: utc(&raw.created_at_utc, "created_at_utc")?,
        expires_at: utc(&raw.expires_at_utc, "expires_at_utc")?,
        legs,
        limit_price: decimal(&raw.limit_price, "limit_price")?,
        max_loss: decimal(&raw.max_loss, "max_loss")?,
        rule_version: raw.rule_version.clone(),
        data_snapshot_ids: raw.data_snapshot_ids.clone(),
        manual_confirmation_required: raw.manual_confirmation_required,
        hash_verified: expected_hash == raw.plan_hash,
        order_side: map_side(raw.order_side)?,
        order_type,
        adaptive_policy_valid: adaptive_policy_valid(raw, order_type),
        market_data_provider: raw.market_data_provider.clone(),
        position_effect: map_position_effect(raw.position_effect)?,
    })
}

pub(super) fn unavailable_event_context(now: DateTime<Utc>) -> EventRiskContext {
    EventRiskContext {
        event_context_id: String::new(),
        trading_date: String::new(),
        generated_at: now,
        available: false,
        source_documents: Vec::new(),
        risk_flags: BTreeSet::new(),
        event_released: false,
        context_hash: String::new(),
        hash_verified: false,
    }
}

pub(super) fn event_flag(value: i32) -> Result<EventRiskFlag, &'static str> {
    match ProtoEventFlag::try_from(value).ok() {
        Some(ProtoEventFlag::NoShortPremiumBeforeEvent) => {
            Ok(EventRiskFlag::NoShortPremiumBeforeEvent)
        }
        Some(ProtoEventFlag::SizeHalf) => Ok(EventRiskFlag::SizeHalf),
        Some(ProtoEventFlag::WaitAfterRelease) => Ok(EventRiskFlag::WaitAfterRelease),
        Some(ProtoEventFlag::ElevatedEventRisk) => Ok(EventRiskFlag::ElevatedEventRisk),
        Some(ProtoEventFlag::NoNaked0dte) => Ok(EventRiskFlag::NoNaked0Dte),
        _ => Err("event risk flag"),
    }
}

pub(super) fn event_context(raw: &ProtoEventContext) -> Result<EventRiskContext, &'static str> {
    let expected_hash = digest(raw, |value| value.context_hash.clear());
    let source_documents = raw
        .source_documents
        .iter()
        .map(|source| {
            if !matches!(
                source.category.as_str(),
                "macro" | "holdings" | "earnings" | "news"
            ) {
                return Err("event source category");
            }
            Ok(EventSourceProof {
                category: source.category.clone(),
                source_timestamp: utc(&source.source_timestamp_utc, "source_timestamp_utc")?,
                received_at: utc(&source.received_at_utc, "received_at_utc")?,
                confidence: source.confidence,
                raw_ref: source.raw_ref.clone(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let flags = raw
        .risk_flags
        .iter()
        .map(|value| event_flag(*value))
        .collect::<Result<BTreeSet<_>, _>>()?;
    if flags.len() != raw.risk_flags.len() {
        return Err("duplicate event risk flag");
    }
    Ok(EventRiskContext {
        event_context_id: raw.event_context_id.clone(),
        trading_date: raw.trading_date.clone(),
        generated_at: utc(&raw.generated_at_utc, "generated_at_utc")?,
        available: raw.available,
        source_documents,
        risk_flags: flags,
        event_released: raw.event_released,
        context_hash: raw.context_hash.clone(),
        hash_verified: raw.context_hash.len() == 64 && expected_hash == raw.context_hash,
    })
}

pub(super) fn reason_proto(reason: RiskReasonCode) -> ProtoReason {
    match reason {
        RiskReasonCode::DataNotHealthy => ProtoReason::DataNotHealthy,
        RiskReasonCode::BrokerNotHealthy => ProtoReason::BrokerNotHealthy,
        RiskReasonCode::BrokerNotReconciled => ProtoReason::BrokerNotReconciled,
        RiskReasonCode::EventContextUnavailable => ProtoReason::EventContextUnavailable,
        RiskReasonCode::EventContextInvalid => ProtoReason::EventContextInvalid,
        RiskReasonCode::EventPolicyBlock => ProtoReason::EventPolicyBlock,
        RiskReasonCode::PlanExpired => ProtoReason::PlanExpired,
        RiskReasonCode::PlanInvalid => ProtoReason::PlanInvalid,
        RiskReasonCode::PlanHashMismatch => ProtoReason::PlanHashMismatch,
        RiskReasonCode::SnapshotNotCurrent => ProtoReason::SnapshotNotCurrent,
        RiskReasonCode::ExecutionModeBlocked => ProtoReason::ExecutionModeBlocked,
        RiskReasonCode::DuplicateConflict => ProtoReason::DuplicateConflict,
        RiskReasonCode::RiskLimitsUnconfirmed => ProtoReason::RiskLimitsUnconfirmed,
        RiskReasonCode::KillSwitchActive => ProtoReason::KillSwitchActive,
        RiskReasonCode::DailyLossLimit => ProtoReason::DailyLossLimit,
        RiskReasonCode::MaxTradesReached => ProtoReason::MaxTradesReached,
        RiskReasonCode::LossCooldownActive => ProtoReason::LossCooldownActive,
        RiskReasonCode::PlanRiskLimit => ProtoReason::PlanRiskLimit,
        RiskReasonCode::OpenRiskLimit => ProtoReason::OpenRiskLimit,
        RiskReasonCode::BuyingPowerInsufficient => ProtoReason::BuyingPowerInsufficient,
        RiskReasonCode::MaxContractsExceeded => ProtoReason::MaxContractsExceeded,
        RiskReasonCode::RuleVersionMismatch => ProtoReason::RuleVersionMismatch,
        RiskReasonCode::QuoteProofInvalid => ProtoReason::QuoteProofInvalid,
        RiskReasonCode::QuoteStale => ProtoReason::QuoteStale,
        RiskReasonCode::QuoteSpreadTooWide => ProtoReason::QuoteSpreadTooWide,
        RiskReasonCode::GreeksInvalid => ProtoReason::GreeksInvalid,
        RiskReasonCode::ChainSnapshotMismatch => ProtoReason::ChainSnapshotMismatch,
        RiskReasonCode::StrategyNotAllowed => ProtoReason::StrategyNotAllowed,
        RiskReasonCode::EntryWindowClosed => ProtoReason::EntryWindowClosed,
        RiskReasonCode::MarketOrderBlocked => ProtoReason::MarketOrderBlocked,
        RiskReasonCode::PositionNotReducible => ProtoReason::PositionNotReducible,
    }
}

pub(super) fn rejected(
    raw: Option<&ProtoPlan>,
    now: DateTime<Utc>,
    reason: ProtoReason,
) -> ProtoDecision {
    let plan_hash = raw
        .map(|plan| plan.plan_hash.as_str())
        .filter(|value| {
            value.len() == 64
                && value
                    .chars()
                    .all(|ch| ch.is_ascii_digit() || ('a'..='f').contains(&ch))
        })
        .unwrap_or("0000000000000000000000000000000000000000000000000000000000000000");
    let plan_id = raw.map_or("unparseable", |plan| plan.plan_id.as_str());
    ProtoDecision {
        schema_version: "1.0".into(),
        decision_id: format!("risk_{plan_id}_{}", now.timestamp_millis()),
        plan_id: plan_id.into(),
        plan_hash: plan_hash.into(),
        session_id: raw
            .map_or("unknown", |plan| plan.session_id.as_str())
            .into(),
        occurred_at_utc: now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        decision: RiskDecisionKind::Rejected as i32,
        reason_codes: vec![reason as i32],
        manual_confirmation_required: true,
        rule_version: raw
            .map_or("unknown", |plan| plan.rule_version.as_str())
            .into(),
    }
}

pub(super) fn proto_order_state(state: OrderState) -> ProtoOrderState {
    match state {
        OrderState::Proposed | OrderState::AwaitingConfirmation => {
            ProtoOrderState::AwaitingConfirmation
        }
        OrderState::RiskRejected => ProtoOrderState::RiskRejected,
        OrderState::Approved => ProtoOrderState::Approved,
        OrderState::Submitting => ProtoOrderState::Submitting,
        OrderState::Working => ProtoOrderState::Working,
        OrderState::PartialFill => ProtoOrderState::PartialFill,
        OrderState::Filled => ProtoOrderState::Filled,
        OrderState::CancelPending => ProtoOrderState::CancelPending,
        OrderState::Cancelled => ProtoOrderState::Cancelled,
        OrderState::Rejected => ProtoOrderState::Rejected,
        OrderState::Expired => ProtoOrderState::Expired,
        OrderState::ReconcilePending => ProtoOrderState::ReconcilePending,
        OrderState::Shadowed => ProtoOrderState::Shadowed,
    }
}

pub(super) fn proto_child_state(state: AdapterOrderStatus) -> ProtoChildState {
    match state {
        AdapterOrderStatus::Working => ProtoChildState::Working,
        AdapterOrderStatus::PartialFill => ProtoChildState::PartialFill,
        AdapterOrderStatus::Filled => ProtoChildState::Filled,
        AdapterOrderStatus::Cancelled => ProtoChildState::Cancelled,
        AdapterOrderStatus::Rejected => ProtoChildState::Rejected,
        AdapterOrderStatus::ReconcilePending => ProtoChildState::ReconcilePending,
    }
}

pub(super) fn proto_adapter_side(side: AdapterOrderSide) -> ProtoSide {
    match side {
        AdapterOrderSide::Buy => ProtoSide::Buy,
        AdapterOrderSide::Sell => ProtoSide::Sell,
    }
}

pub(super) fn restored_order_state(state: i32) -> Result<(OrderState, bool), &'static str> {
    match ProtoOrderState::try_from(state).ok() {
        Some(ProtoOrderState::AwaitingConfirmation) => {
            Ok((OrderState::AwaitingConfirmation, false))
        }
        Some(ProtoOrderState::RiskRejected) => Ok((OrderState::RiskRejected, false)),
        Some(ProtoOrderState::Filled) => Ok((OrderState::Filled, false)),
        Some(ProtoOrderState::Cancelled) => Ok((OrderState::Cancelled, false)),
        Some(ProtoOrderState::Rejected) => Ok((OrderState::Rejected, false)),
        Some(ProtoOrderState::Expired) => Ok((OrderState::Expired, false)),
        Some(ProtoOrderState::Shadowed) => Ok((OrderState::Shadowed, false)),
        Some(
            ProtoOrderState::Approved
            | ProtoOrderState::Submitting
            | ProtoOrderState::Working
            | ProtoOrderState::PartialFill
            | ProtoOrderState::CancelPending
            | ProtoOrderState::ReconcilePending,
        ) => Ok((OrderState::ReconcilePending, true)),
        _ => Err("order state"),
    }
}

pub(super) fn restored_child_state(state: i32) -> Result<AdapterOrderStatus, &'static str> {
    match ProtoChildState::try_from(state).ok() {
        Some(ProtoChildState::Working) => Ok(AdapterOrderStatus::Working),
        Some(ProtoChildState::PartialFill) => Ok(AdapterOrderStatus::PartialFill),
        Some(ProtoChildState::Filled) => Ok(AdapterOrderStatus::Filled),
        Some(ProtoChildState::Cancelled) => Ok(AdapterOrderStatus::Cancelled),
        Some(ProtoChildState::Rejected) => Ok(AdapterOrderStatus::Rejected),
        Some(ProtoChildState::ReconcilePending) => Ok(AdapterOrderStatus::ReconcilePending),
        _ => Err("child state"),
    }
}

pub(super) fn restore_entry(
    entry: RestorableExecutionOrder,
    now: DateTime<Utc>,
) -> Result<(StagedOrder, bool), &'static str> {
    let raw_plan = entry.plan.ok_or("missing plan")?;
    let raw_order = entry.order.ok_or("missing order")?;
    let domain_plan = plan(&raw_plan)?;
    if raw_order.schema_version != "1.1"
        || raw_order.order_id.is_empty()
        || raw_order.plan_id != raw_plan.plan_id
        || raw_order.plan_hash != raw_plan.plan_hash
        || raw_order.idempotency_key != raw_plan.idempotency_key
        || raw_order.session_id != raw_plan.session_id
        || raw_order.broker_id != raw_plan.broker_id
        || raw_order.execution_mode != raw_plan.execution_mode
        || raw_order.total_quantity == 0
        || raw_order.state_version == 0
        || raw_order.broker_child_order_ids.len() != raw_order.broker_child_orders.len()
        || raw_order.broker_child_order_ids
            != raw_order
                .broker_child_orders
                .iter()
                .map(|child| child.broker_order_id.clone())
                .collect::<Vec<_>>()
        || domain_plan
            .legs
            .iter()
            .any(|leg| leg.quantity != raw_order.total_quantity)
    {
        return Err("order identity");
    }
    let (mut state, mut reconciliation_required) = restored_order_state(raw_order.state)?;
    if state == OrderState::AwaitingConfirmation
        && (entry.confirmation_token.is_empty() || domain_plan.expires_at <= now)
    {
        state = OrderState::ReconcilePending;
        reconciliation_required = true;
    } else if state != OrderState::AwaitingConfirmation && !entry.confirmation_token.is_empty() {
        return Err("confirmation capability");
    }
    let child_orders = raw_order
        .broker_child_orders
        .iter()
        .map(|child| {
            let leg_index = usize::try_from(child.leg_index).map_err(|_| "leg index")?;
            let leg = raw_plan.legs.get(leg_index).ok_or("leg index")?;
            let side = match ProtoSide::try_from(child.side).ok() {
                Some(ProtoSide::Buy) => AdapterOrderSide::Buy,
                Some(ProtoSide::Sell) => AdapterOrderSide::Sell,
                _ => return Err("child side"),
            };
            if child.broker_order_id.is_empty()
                || child.contract_id != leg.contract_id
                || child.side != leg.side
                || child.quantity != leg.quantity
                || child.filled_quantity > child.quantity
            {
                return Err("child projection");
            }
            Ok(broker::BrokerChildOrder {
                broker_order_id: child.broker_order_id.clone(),
                leg_index,
                contract_id: child.contract_id.clone(),
                side,
                quantity: child.quantity,
                filled_quantity: child.filled_quantity,
                status: restored_child_state(child.state)?,
                submitted_price: if child.submitted_price.is_empty() {
                    None
                } else {
                    Some(decimal(&child.submitted_price, "child price")?)
                },
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let restored_updated_at = utc(&raw_order.updated_at_utc, "updated_at")?;
    let state_version = if reconciliation_required {
        raw_order
            .state_version
            .checked_add(1)
            .ok_or("state version")?
    } else {
        raw_order.state_version
    };
    let updated_at = if reconciliation_required {
        now
    } else {
        restored_updated_at
    };
    let mut record = OrderRecord::restored(
        raw_order.order_id,
        raw_order.plan_id,
        raw_order.plan_hash,
        raw_order.idempotency_key,
        state,
        domain_plan.expires_at,
        raw_order.total_quantity,
        raw_order.filled_quantity,
        (!raw_order.broker_order_id.is_empty()).then_some(raw_order.broker_order_id),
        child_orders,
        raw_order.residual_exposure || reconciliation_required,
        state_version,
        updated_at,
    )
    .map_err(|_| "order record")?;
    if reconciliation_required {
        record.residual_exposure = true;
    }
    Ok((
        StagedOrder {
            raw_plan,
            record,
            confirmation_token: entry.confirmation_token,
            risk_reasons: raw_order.risk_reason_codes,
        },
        reconciliation_required,
    ))
}

pub(super) fn order_proto(staged: &StagedOrder, now: DateTime<Utc>) -> ProtoOrder {
    let updated_at = staged.record.updated_at(now);
    ProtoOrder {
        schema_version: "1.1".into(),
        order_id: staged.record.order_id.clone(),
        plan_id: staged.record.plan_id.clone(),
        plan_hash: staged.record.plan_hash.clone(),
        idempotency_key: staged.record.idempotency_key.clone(),
        session_id: staged.raw_plan.session_id.clone(),
        broker_id: staged.raw_plan.broker_id,
        execution_mode: staged.raw_plan.execution_mode,
        state: proto_order_state(staged.record.state) as i32,
        total_quantity: staged.record.total_quantity,
        filled_quantity: staged.record.filled_quantity,
        broker_order_id: staged.record.broker_order_id.clone().unwrap_or_default(),
        expires_at_utc: staged
            .record
            .expires_at
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        updated_at_utc: updated_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        risk_reason_codes: staged.risk_reasons.clone(),
        state_version: staged.record.state_version(),
        broker_child_order_ids: staged.record.broker_child_order_ids.clone(),
        residual_exposure: staged.record.residual_exposure,
        broker_child_orders: staged
            .record
            .broker_child_orders
            .iter()
            .map(|child| ProtoChildOrder {
                broker_order_id: child.broker_order_id.clone(),
                leg_index: child.leg_index as u32,
                contract_id: child.contract_id.clone(),
                side: proto_adapter_side(child.side) as i32,
                quantity: child.quantity,
                filled_quantity: child.filled_quantity,
                state: proto_child_state(child.status) as i32,
                submitted_price: child
                    .submitted_price
                    .map(|price| price.to_string())
                    .unwrap_or_default(),
            })
            .collect(),
    }
}

pub(super) fn workflow_lock(
    service: &RiskExecutionServiceImpl,
) -> Result<std::sync::MutexGuard<'_, Workflow>, ()> {
    service.workflow.lock().map_err(|_| ())
}
