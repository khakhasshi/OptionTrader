//! Rust Final Risk Check. Python, React and LLM may propose a candidate, but
//! only this crate can approve it for the execution state machine.

use std::collections::BTreeSet;

use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BrokerHealth {
    Healthy,
    Degraded,
    Disconnected,
    Reconciling,
}

impl BrokerHealth {
    pub fn allows_new_position(self) -> bool {
        matches!(self, BrokerHealth::Healthy)
    }
}

pub fn new_position_allowed(data_healthy: bool, broker: BrokerHealth, reconciled: bool) -> bool {
    data_healthy && broker.allows_new_position() && reconciled
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerId {
    Longbridge,
    Ibkr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyKind {
    LongGamma,
    ShortPremium,
    EventVolCrush,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    Replay,
    Shadow,
    Paper,
    ManualConfirm,
    ControlledAuto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionRight {
    Call,
    Put,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateLeg {
    pub side: OrderSide,
    pub option_right: OptionRight,
    pub contract_id: String,
    pub expiry: String,
    pub strike: Decimal,
    pub quantity: u32,
}

#[derive(Debug, Clone)]
pub struct CandidatePlan {
    pub plan_id: String,
    pub plan_hash: String,
    pub idempotency_key: String,
    pub session_id: String,
    pub signal_id: String,
    pub broker_id: BrokerId,
    pub strategy: StrategyKind,
    pub execution_mode: ExecutionMode,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub legs: Vec<CandidateLeg>,
    pub limit_price: Decimal,
    pub max_loss: Decimal,
    pub rule_version: String,
    pub data_snapshot_ids: Vec<String>,
    pub manual_confirmation_required: bool,
    pub hash_verified: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EventSourceProof {
    pub category: String,
    pub source_timestamp: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub confidence: f64,
    pub raw_ref: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EventRiskFlag {
    NoShortPremiumBeforeEvent,
    SizeHalf,
    WaitAfterRelease,
    ElevatedEventRisk,
    NoNaked0Dte,
}

#[derive(Debug, Clone)]
pub struct EventRiskContext {
    pub event_context_id: String,
    pub trading_date: String,
    pub generated_at: DateTime<Utc>,
    pub available: bool,
    pub source_documents: Vec<EventSourceProof>,
    pub risk_flags: BTreeSet<EventRiskFlag>,
    pub event_released: bool,
    pub context_hash: String,
    pub hash_verified: bool,
}

#[derive(Debug, Clone)]
pub struct AuthorityState {
    pub data_healthy: bool,
    pub broker_health: BrokerHealth,
    pub broker_reconciled: bool,
    pub latest_snapshot_id: String,
    pub market_time: DateTime<Utc>,
    pub trading_date: String,
    pub risk_limits_confirmed: bool,
    pub kill_switch_active: bool,
    pub daily_realized_pnl: Decimal,
    pub open_risk: Decimal,
    pub daily_trade_count: u32,
    pub consecutive_losses: u32,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub buying_power: Decimal,
    pub active_rule_version: String,
}

#[derive(Debug, Clone)]
pub struct RiskLimits {
    pub max_plan_loss: Decimal,
    pub max_daily_loss: Decimal,
    pub max_open_risk: Decimal,
    pub max_daily_trades: u32,
    pub max_contracts: u32,
}

#[derive(Debug, Clone)]
pub struct FinalRiskInput {
    pub plan: CandidatePlan,
    pub event_context: EventRiskContext,
    pub authority: AuthorityState,
    pub evaluated_at: DateTime<Utc>,
    pub limits: RiskLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RiskReasonCode {
    DataNotHealthy,
    BrokerNotHealthy,
    BrokerNotReconciled,
    EventContextUnavailable,
    EventContextInvalid,
    EventPolicyBlock,
    PlanExpired,
    PlanInvalid,
    PlanHashMismatch,
    SnapshotNotCurrent,
    ExecutionModeBlocked,
    DuplicateConflict,
    RiskLimitsUnconfirmed,
    KillSwitchActive,
    DailyLossLimit,
    MaxTradesReached,
    LossCooldownActive,
    PlanRiskLimit,
    OpenRiskLimit,
    BuyingPowerInsufficient,
    MaxContractsExceeded,
    RuleVersionMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalRiskDecision {
    pub approved: bool,
    pub reasons: Vec<RiskReasonCode>,
}

fn event_context_valid(context: &EventRiskContext, authority: &AuthorityState) -> bool {
    if !context.hash_verified
        || context.context_hash.len() != 64
        || context.event_context_id.is_empty()
        || context.trading_date != authority.trading_date
        || context.generated_at > authority.market_time + Duration::minutes(5)
        || authority.market_time - context.generated_at > Duration::minutes(5)
    {
        return false;
    }
    let categories: BTreeSet<&str> = context
        .source_documents
        .iter()
        .map(|source| source.category.as_str())
        .collect();
    if categories != BTreeSet::from(["earnings", "holdings", "macro", "news"])
        || context.source_documents.len() != 4
    {
        return false;
    }
    context.source_documents.iter().all(|source| {
        let minimum = if source.category == "holdings" {
            0.9
        } else {
            0.8
        };
        let max_age = if source.category == "holdings" {
            Duration::days(14)
        } else {
            Duration::days(1)
        };
        source.confidence.is_finite()
            && source.confidence >= minimum
            && !source.raw_ref.is_empty()
            && source.source_timestamp <= source.received_at
            && source.received_at <= authority.market_time + Duration::minutes(5)
            && authority.market_time - source.received_at <= max_age
    })
}

fn candidate_shape_valid(plan: &CandidatePlan) -> bool {
    if plan.plan_id.is_empty()
        || plan.idempotency_key.is_empty()
        || plan.session_id.is_empty()
        || plan.signal_id.is_empty()
        || plan.rule_version.is_empty()
        || plan.legs.is_empty()
        || plan.legs.len() > 4
        || plan.limit_price <= Decimal::ZERO
        || plan.max_loss <= Decimal::ZERO
        || plan.expires_at <= plan.created_at
        || !plan.manual_confirmation_required
    {
        return false;
    }
    let contracts: BTreeSet<&str> = plan
        .legs
        .iter()
        .map(|leg| leg.contract_id.as_str())
        .collect();
    let quantities: BTreeSet<u32> = plan.legs.iter().map(|leg| leg.quantity).collect();
    contracts.len() == plan.legs.len()
        && quantities.len() == 1
        && plan.legs.iter().all(|leg| {
            !leg.contract_id.is_empty()
                && !leg.expiry.is_empty()
                && leg.strike > Decimal::ZERO
                && leg.quantity > 0
        })
}

fn sell_is_hedged(plan: &CandidatePlan, sold: &CandidateLeg) -> bool {
    plan.legs.iter().any(|hedge| {
        hedge.side == OrderSide::Buy
            && hedge.option_right == sold.option_right
            && hedge.expiry == sold.expiry
            && hedge.quantity >= sold.quantity
            && match sold.option_right {
                OptionRight::Call => hedge.strike > sold.strike,
                OptionRight::Put => hedge.strike < sold.strike,
            }
    })
}

fn event_policy_allows(plan: &CandidatePlan, event: &EventRiskContext) -> bool {
    if event.risk_flags.contains(&EventRiskFlag::WaitAfterRelease) || event.event_released {
        return false;
    }
    if matches!(
        plan.strategy,
        StrategyKind::ShortPremium | StrategyKind::EventVolCrush
    ) && event
        .risk_flags
        .contains(&EventRiskFlag::NoShortPremiumBeforeEvent)
    {
        return false;
    }
    if event.risk_flags.contains(&EventRiskFlag::NoNaked0Dte)
        && plan
            .legs
            .iter()
            .filter(|leg| leg.side == OrderSide::Sell)
            .any(|sold| !sell_is_hedged(plan, sold))
    {
        return false;
    }
    true
}

pub fn final_risk_check(input: &FinalRiskInput) -> FinalRiskDecision {
    let mut reasons = BTreeSet::new();
    if !input.authority.data_healthy {
        reasons.insert(RiskReasonCode::DataNotHealthy);
    }
    if !input.authority.broker_health.allows_new_position() {
        reasons.insert(RiskReasonCode::BrokerNotHealthy);
    }
    if !input.authority.broker_reconciled {
        reasons.insert(RiskReasonCode::BrokerNotReconciled);
    }
    if !input.authority.risk_limits_confirmed {
        reasons.insert(RiskReasonCode::RiskLimitsUnconfirmed);
    }
    if input.authority.kill_switch_active {
        reasons.insert(RiskReasonCode::KillSwitchActive);
    }
    if input.authority.daily_realized_pnl <= -input.limits.max_daily_loss {
        reasons.insert(RiskReasonCode::DailyLossLimit);
    }
    if input.authority.daily_trade_count >= input.limits.max_daily_trades {
        reasons.insert(RiskReasonCode::MaxTradesReached);
    }
    if input.authority.consecutive_losses >= 3
        && input
            .authority
            .cooldown_until
            .is_none_or(|until| input.evaluated_at < until)
    {
        reasons.insert(RiskReasonCode::LossCooldownActive);
    }
    if input.plan.max_loss > input.limits.max_plan_loss {
        reasons.insert(RiskReasonCode::PlanRiskLimit);
    }
    if input.authority.open_risk + input.plan.max_loss > input.limits.max_open_risk {
        reasons.insert(RiskReasonCode::OpenRiskLimit);
    }
    if input.authority.buying_power < input.plan.max_loss {
        reasons.insert(RiskReasonCode::BuyingPowerInsufficient);
    }
    if input
        .plan
        .legs
        .iter()
        .any(|leg| leg.quantity > input.limits.max_contracts)
    {
        reasons.insert(RiskReasonCode::MaxContractsExceeded);
    }
    if input.authority.active_rule_version != input.plan.rule_version {
        reasons.insert(RiskReasonCode::RuleVersionMismatch);
    }
    if !input.event_context.available {
        reasons.insert(RiskReasonCode::EventContextUnavailable);
    }
    if !event_context_valid(&input.event_context, &input.authority) {
        reasons.insert(RiskReasonCode::EventContextInvalid);
    }
    if !event_policy_allows(&input.plan, &input.event_context) {
        reasons.insert(RiskReasonCode::EventPolicyBlock);
    }
    if input.evaluated_at >= input.plan.expires_at {
        reasons.insert(RiskReasonCode::PlanExpired);
    }
    if !candidate_shape_valid(&input.plan) {
        reasons.insert(RiskReasonCode::PlanInvalid);
    }
    if !input.plan.hash_verified || input.plan.plan_hash.len() != 64 {
        reasons.insert(RiskReasonCode::PlanHashMismatch);
    }
    if !input
        .plan
        .data_snapshot_ids
        .iter()
        .any(|snapshot| snapshot == &input.authority.latest_snapshot_id)
    {
        reasons.insert(RiskReasonCode::SnapshotNotCurrent);
    }
    if !matches!(
        input.plan.execution_mode,
        ExecutionMode::Replay
            | ExecutionMode::Shadow
            | ExecutionMode::Paper
            | ExecutionMode::ManualConfirm
    ) {
        reasons.insert(RiskReasonCode::ExecutionModeBlocked);
    }
    FinalRiskDecision {
        approved: reasons.is_empty(),
        reasons: reasons.into_iter().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn now() -> DateTime<Utc> {
        "2026-07-20T14:30:00Z".parse().unwrap()
    }

    fn input() -> FinalRiskInput {
        let source = |category: &str, confidence: f64| EventSourceProof {
            category: category.into(),
            source_timestamp: "2026-07-20T12:00:00Z".parse().unwrap(),
            received_at: "2026-07-20T12:05:00Z".parse().unwrap(),
            confidence,
            raw_ref: format!("fixture://{category}"),
        };
        FinalRiskInput {
            plan: CandidatePlan {
                plan_id: "plan-1".into(),
                plan_hash: "a".repeat(64),
                idempotency_key: "idem-1".into(),
                session_id: "session-1".into(),
                signal_id: "signal-1".into(),
                broker_id: BrokerId::Ibkr,
                strategy: StrategyKind::LongGamma,
                execution_mode: ExecutionMode::Paper,
                created_at: now(),
                expires_at: now() + Duration::minutes(1),
                legs: vec![CandidateLeg {
                    side: OrderSide::Buy,
                    option_right: OptionRight::Call,
                    contract_id: "QQQ-C".into(),
                    expiry: "2026-07-20".into(),
                    strike: Decimal::from_str("500").unwrap(),
                    quantity: 1,
                }],
                limit_price: Decimal::from_str("2.50").unwrap(),
                max_loss: Decimal::from_str("250").unwrap(),
                rule_version: "rules-p3".into(),
                data_snapshot_ids: vec!["mkt-1".into(), "opt-1".into()],
                manual_confirmation_required: true,
                hash_verified: true,
            },
            event_context: EventRiskContext {
                event_context_id: "event-1".into(),
                trading_date: "2026-07-20".into(),
                generated_at: now(),
                available: true,
                source_documents: vec![
                    source("macro", 0.8),
                    source("holdings", 0.9),
                    source("earnings", 0.8),
                    source("news", 0.8),
                ],
                risk_flags: BTreeSet::from([EventRiskFlag::NoNaked0Dte]),
                event_released: false,
                context_hash: "b".repeat(64),
                hash_verified: true,
            },
            authority: AuthorityState {
                data_healthy: true,
                broker_health: BrokerHealth::Healthy,
                broker_reconciled: true,
                latest_snapshot_id: "mkt-1".into(),
                market_time: now(),
                trading_date: "2026-07-20".into(),
                risk_limits_confirmed: true,
                kill_switch_active: false,
                daily_realized_pnl: Decimal::ZERO,
                open_risk: Decimal::ZERO,
                daily_trade_count: 0,
                consecutive_losses: 0,
                cooldown_until: None,
                buying_power: Decimal::from_str("100000").unwrap(),
                active_rule_version: "rules-p3".into(),
            },
            evaluated_at: now() + Duration::seconds(1),
            limits: RiskLimits {
                max_plan_loss: Decimal::from_str("500").unwrap(),
                max_daily_loss: Decimal::from_str("1000").unwrap(),
                max_open_risk: Decimal::from_str("1000").unwrap(),
                max_daily_trades: 3,
                max_contracts: 2,
            },
        }
    }

    #[test]
    fn healthy_defined_risk_candidate_is_approved() {
        let decision = final_risk_check(&input());
        assert!(decision.approved);
        assert!(decision.reasons.is_empty());
    }

    #[test]
    fn every_authority_dimension_is_required() {
        let mut value = input();
        value.authority.data_healthy = false;
        value.authority.broker_health = BrokerHealth::Disconnected;
        value.authority.broker_reconciled = false;
        value.event_context.available = false;
        let decision = final_risk_check(&value);
        assert!(!decision.approved);
        assert!(decision.reasons.contains(&RiskReasonCode::DataNotHealthy));
        assert!(decision.reasons.contains(&RiskReasonCode::BrokerNotHealthy));
        assert!(decision
            .reasons
            .contains(&RiskReasonCode::BrokerNotReconciled));
        assert!(decision
            .reasons
            .contains(&RiskReasonCode::EventContextUnavailable));
    }

    #[test]
    fn stale_low_confidence_or_wrong_day_event_context_is_rejected() {
        let mut value = input();
        value.event_context.generated_at = now() - Duration::minutes(6);
        value.event_context.source_documents[0].confidence = 0.1;
        value.event_context.trading_date = "2026-07-21".into();
        let decision = final_risk_check(&value);
        assert_eq!(decision.reasons, vec![RiskReasonCode::EventContextInvalid]);
    }

    #[test]
    fn event_policy_blocks_short_premium_and_naked_sales() {
        let mut value = input();
        value.plan.strategy = StrategyKind::ShortPremium;
        value.plan.legs[0].side = OrderSide::Sell;
        value
            .event_context
            .risk_flags
            .insert(EventRiskFlag::NoShortPremiumBeforeEvent);
        assert!(final_risk_check(&value)
            .reasons
            .contains(&RiskReasonCode::EventPolicyBlock));
    }

    #[test]
    fn expired_hash_mismatch_snapshot_mismatch_and_auto_are_rejected() {
        let mut value = input();
        value.evaluated_at = value.plan.expires_at;
        value.plan.hash_verified = false;
        value.plan.data_snapshot_ids = vec!["old".into()];
        value.plan.execution_mode = ExecutionMode::ControlledAuto;
        let decision = final_risk_check(&value);
        assert!(decision.reasons.contains(&RiskReasonCode::PlanExpired));
        assert!(decision.reasons.contains(&RiskReasonCode::PlanHashMismatch));
        assert!(decision
            .reasons
            .contains(&RiskReasonCode::SnapshotNotCurrent));
        assert!(decision
            .reasons
            .contains(&RiskReasonCode::ExecutionModeBlocked));
    }

    #[test]
    fn unequal_leg_quantities_are_not_valid_combo_units() {
        let mut value = input();
        let mut second = value.plan.legs[0].clone();
        second.contract_id = "QQQ-P".into();
        second.option_right = OptionRight::Put;
        second.quantity = 2;
        value.plan.legs.push(second);
        assert!(final_risk_check(&value)
            .reasons
            .contains(&RiskReasonCode::PlanInvalid));
    }

    #[test]
    fn legacy_health_gate_requires_all_conditions() {
        assert!(new_position_allowed(true, BrokerHealth::Healthy, true));
        assert!(!new_position_allowed(false, BrokerHealth::Healthy, true));
        assert!(!new_position_allowed(true, BrokerHealth::Degraded, true));
        assert!(!new_position_allowed(true, BrokerHealth::Healthy, false));
    }

    #[test]
    fn account_limits_kill_switch_and_rule_version_are_authoritative() {
        let mut value = input();
        value.authority.risk_limits_confirmed = false;
        value.authority.kill_switch_active = true;
        value.authority.daily_realized_pnl = Decimal::from_str("-1000").unwrap();
        value.authority.open_risk = Decimal::from_str("900").unwrap();
        value.authority.daily_trade_count = 3;
        value.authority.consecutive_losses = 3;
        value.authority.cooldown_until = Some(now() + Duration::minutes(30));
        value.authority.buying_power = Decimal::from_str("100").unwrap();
        value.authority.active_rule_version = "old-rules".into();
        value.plan.max_loss = Decimal::from_str("600").unwrap();
        value.plan.legs[0].quantity = 3;
        let reasons = final_risk_check(&value).reasons;
        for expected in [
            RiskReasonCode::RiskLimitsUnconfirmed,
            RiskReasonCode::KillSwitchActive,
            RiskReasonCode::DailyLossLimit,
            RiskReasonCode::MaxTradesReached,
            RiskReasonCode::LossCooldownActive,
            RiskReasonCode::PlanRiskLimit,
            RiskReasonCode::OpenRiskLimit,
            RiskReasonCode::BuyingPowerInsufficient,
            RiskReasonCode::MaxContractsExceeded,
            RiskReasonCode::RuleVersionMismatch,
        ] {
            assert!(reasons.contains(&expected), "missing {expected:?}");
        }
    }
}
