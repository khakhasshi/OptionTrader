//! gRPC boundary for Rust Final Risk Check.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::{Arc, Mutex, RwLock};

use broker::{
    BrokerAdapter, BrokerError, BrokerId as AdapterBrokerId, BrokerOrderLeg,
    OrderSide as AdapterOrderSide, PaperBroker,
};
use chrono::{DateTime, Utc};
use execution::{submit_to_broker, OrderRecord, OrderState};
use market_core::{DataHealth, MarketSnapshot};
use optiontrader_proto::execution_v1::{
    risk_execution_service_server::RiskExecutionService, BrokerId as ProtoBrokerId,
    CancelOrderRequest, CandidateTradePlan as ProtoPlan, ConfirmCandidateRequest,
    EvaluateCandidateRequest, EventRiskContext as ProtoEventContext,
    EventRiskFlag as ProtoEventFlag, ExecutionMode as ProtoMode, ExecutionOrder as ProtoOrder,
    ExecutionOrderState as ProtoOrderState, GetOrderRequest, OptionRight as ProtoRight,
    OrderSide as ProtoSide, RiskDecision as ProtoDecision, RiskDecisionKind,
    RiskReasonCode as ProtoReason, StageCandidateResponse, StrategyKind as ProtoStrategy,
};
use prost::Message;
use risk_gateway::{
    final_risk_check, new_position_allowed, AuthorityState, BrokerHealth, BrokerId, CandidateLeg,
    CandidatePlan, EventRiskContext, EventRiskFlag, EventSourceProof, ExecutionMode,
    FinalRiskInput, OptionRight, OrderSide, RiskLimits, RiskReasonCode, StrategyKind,
};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::grpc::{LiveMarketServiceImpl, MarketServiceImpl};

fn parse_boolean(name: &str, value: Option<&str>, default: bool) -> Result<bool, String> {
    match value {
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        Some(_) => Err(format!("{name} must be exactly true or false")),
        None => Ok(default),
    }
}

fn boolean_env(name: &str, default: bool) -> Result<bool, String> {
    match std::env::var(name) {
        Ok(value) => parse_boolean(name, Some(&value), default),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!("{name} must be valid UTF-8")),
    }
}

#[derive(Clone)]
pub enum MarketAuthority {
    Replay(MarketServiceImpl),
    Live(LiveMarketServiceImpl),
    #[cfg(test)]
    Fixed(DataHealth, MarketSnapshot),
}

impl MarketAuthority {
    async fn current(&self) -> (DataHealth, Option<MarketSnapshot>) {
        match self {
            MarketAuthority::Replay(service) => (
                service.current_health_value(),
                service.latest_snapshot_value(),
            ),
            MarketAuthority::Live(service) => (
                service.current_health().await,
                service.latest_snapshot().await,
            ),
            #[cfg(test)]
            MarketAuthority::Fixed(health, snapshot) => (*health, Some(snapshot.clone())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BrokerAuthority {
    pub health: BrokerHealth,
    pub reconciled: bool,
    pub risk_limits_confirmed: bool,
    pub kill_switch_active: bool,
    pub daily_realized_pnl: Decimal,
    pub open_risk: Decimal,
    pub daily_trade_count: u32,
    pub consecutive_losses: u32,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub buying_power: Decimal,
    pub active_rule_version: String,
    pub limits: RiskLimits,
}

impl BrokerAuthority {
    pub fn from_env(health: BrokerHealth, reconciled: bool) -> Result<Self, String> {
        let decimal_env = |name: &str, default: &str| {
            let value = std::env::var(name).unwrap_or_else(|_| default.into());
            Decimal::from_str_exact(&value).map_err(|_| format!("{name} must be a decimal"))
        };
        let integer_env = |name: &str, default: u32| {
            std::env::var(name).ok().map_or(Ok(default), |value| {
                value
                    .parse::<u32>()
                    .map_err(|_| format!("{name} must be an unsigned integer"))
            })
        };
        let cooldown_until = std::env::var("OPTIONTRADER_COOLDOWN_UNTIL_UTC")
            .ok()
            .filter(|value| !value.is_empty())
            .map(|value| utc(&value, "OPTIONTRADER_COOLDOWN_UNTIL_UTC"))
            .transpose()
            .map_err(str::to_owned)?;
        let authority = Self {
            health,
            reconciled,
            risk_limits_confirmed: boolean_env("OPTIONTRADER_RISK_LIMITS_CONFIRMED", false)?,
            kill_switch_active: boolean_env("OPTIONTRADER_KILL_SWITCH", false)?,
            daily_realized_pnl: decimal_env("OPTIONTRADER_DAILY_REALIZED_PNL", "0")?,
            open_risk: decimal_env("OPTIONTRADER_OPEN_RISK", "0")?,
            daily_trade_count: integer_env("OPTIONTRADER_DAILY_TRADE_COUNT", 0)?,
            consecutive_losses: integer_env("OPTIONTRADER_CONSECUTIVE_LOSSES", 0)?,
            cooldown_until,
            buying_power: decimal_env("OPTIONTRADER_BUYING_POWER", "0")?,
            active_rule_version: std::env::var("OPTIONTRADER_RULE_VERSION")
                .unwrap_or_else(|_| "UNCONFIRMED".into()),
            limits: RiskLimits {
                max_plan_loss: decimal_env("OPTIONTRADER_MAX_PLAN_LOSS", "250")?,
                max_daily_loss: decimal_env("OPTIONTRADER_MAX_DAILY_LOSS", "500")?,
                max_open_risk: decimal_env("OPTIONTRADER_MAX_OPEN_RISK", "500")?,
                max_daily_trades: integer_env("OPTIONTRADER_MAX_DAILY_TRADES", 3)?,
                max_contracts: integer_env("OPTIONTRADER_MAX_CONTRACTS", 2)?,
            },
        };
        authority.validate()?;
        Ok(authority)
    }

    fn validate(&self) -> Result<(), String> {
        if self.open_risk.is_sign_negative() {
            return Err("OPTIONTRADER_OPEN_RISK must be non-negative".into());
        }
        if self.buying_power.is_sign_negative() {
            return Err("OPTIONTRADER_BUYING_POWER must be non-negative".into());
        }
        if self.active_rule_version.trim().is_empty() {
            return Err("OPTIONTRADER_RULE_VERSION must not be empty".into());
        }
        if self.risk_limits_confirmed && self.active_rule_version == "UNCONFIRMED" {
            return Err("confirmed risk limits require an explicit rule version".into());
        }
        if self.limits.max_plan_loss <= Decimal::ZERO
            || self.limits.max_daily_loss <= Decimal::ZERO
            || self.limits.max_open_risk <= Decimal::ZERO
        {
            return Err("risk limits must be positive".into());
        }
        if self.limits.max_daily_trades == 0 || self.limits.max_contracts == 0 {
            return Err("trade and contract limits must be positive".into());
        }
        Ok(())
    }

    pub fn allows_new_position(&self, data_healthy: bool, now: DateTime<Utc>) -> bool {
        let loss_cooldown_active = self.consecutive_losses >= 3
            && self
                .cooldown_until
                .is_none_or(|cooldown_until| cooldown_until > now);
        new_position_allowed(data_healthy, self.health, self.reconciled)
            && self.risk_limits_confirmed
            && !self.kill_switch_active
            && self.daily_realized_pnl > -self.limits.max_daily_loss
            && self.open_risk < self.limits.max_open_risk
            && self.daily_trade_count < self.limits.max_daily_trades
            && !loss_cooldown_active
            && self.buying_power > Decimal::ZERO
            && !self.active_rule_version.trim().is_empty()
    }
}

#[derive(Clone)]
pub struct RiskExecutionServiceImpl {
    market: MarketAuthority,
    broker: Arc<RwLock<BrokerAuthority>>,
    workflow: Arc<Mutex<Workflow>>,
    clock: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
}

struct StagedOrder {
    raw_plan: ProtoPlan,
    record: OrderRecord,
    confirmation_token: String,
    risk_reasons: Vec<i32>,
}

struct Workflow {
    orders: BTreeMap<String, StagedOrder>,
    order_by_key: BTreeMap<String, (String, String)>,
    longbridge_paper: PaperBroker,
    ibkr_paper: PaperBroker,
}

impl Default for Workflow {
    fn default() -> Self {
        Self {
            orders: BTreeMap::new(),
            order_by_key: BTreeMap::new(),
            longbridge_paper: PaperBroker::new(AdapterBrokerId::Longbridge),
            ibkr_paper: PaperBroker::new(AdapterBrokerId::Ibkr),
        }
    }
}

impl RiskExecutionServiceImpl {
    pub fn new(market: MarketAuthority, broker: BrokerAuthority) -> Self {
        Self {
            market,
            broker: Arc::new(RwLock::new(broker)),
            workflow: Arc::new(Mutex::new(Workflow::default())),
            clock: Arc::new(Utc::now),
        }
    }

    #[cfg(test)]
    fn with_clock(
        market: MarketAuthority,
        broker: BrokerAuthority,
        clock: impl Fn() -> DateTime<Utc> + Send + Sync + 'static,
    ) -> Self {
        Self {
            market,
            broker: Arc::new(RwLock::new(broker)),
            workflow: Arc::new(Mutex::new(Workflow::default())),
            clock: Arc::new(clock),
        }
    }

    async fn evaluate_raw(
        &self,
        raw_plan: Option<&ProtoPlan>,
        raw_event: Option<&ProtoEventContext>,
        now: DateTime<Utc>,
    ) -> Result<ProtoDecision, Status> {
        let Some(raw_plan) = raw_plan else {
            return Ok(rejected(None, now, ProtoReason::PlanInvalid));
        };
        let domain_plan = match plan(raw_plan) {
            Ok(value) => value,
            Err(_) => return Ok(rejected(Some(raw_plan), now, ProtoReason::PlanInvalid)),
        };
        let Some(raw_event) = raw_event else {
            return Ok(rejected(
                Some(raw_plan),
                now,
                ProtoReason::EventContextUnavailable,
            ));
        };
        let domain_event = match event_context(raw_event) {
            Ok(value) => value,
            Err(_) => {
                return Ok(rejected(
                    Some(raw_plan),
                    now,
                    ProtoReason::EventContextInvalid,
                ))
            }
        };
        let (data_health, snapshot) = self.market.current().await;
        let Some(snapshot) = snapshot else {
            return Ok(rejected(Some(raw_plan), now, ProtoReason::DataNotHealthy));
        };
        let market_time = utc(&snapshot.occurred_at_utc, "snapshot time")
            .map_err(|_| Status::internal("authoritative snapshot timestamp is invalid"))?;
        let trading_date = DateTime::parse_from_rfc3339(&snapshot.timestamp_et)
            .map_err(|_| Status::internal("authoritative snapshot ET timestamp is invalid"))?
            .date_naive()
            .to_string();
        let broker = self
            .broker
            .read()
            .map_err(|_| Status::internal("broker authority lock poisoned"))?
            .clone();
        let decision = final_risk_check(&FinalRiskInput {
            plan: domain_plan,
            event_context: domain_event,
            authority: AuthorityState {
                data_healthy: data_health == DataHealth::Healthy
                    && snapshot.data_health == DataHealth::Healthy,
                broker_health: broker.health,
                broker_reconciled: broker.reconciled,
                latest_snapshot_id: snapshot.snapshot_id,
                market_time,
                trading_date,
                risk_limits_confirmed: broker.risk_limits_confirmed,
                kill_switch_active: broker.kill_switch_active,
                daily_realized_pnl: broker.daily_realized_pnl,
                open_risk: broker.open_risk,
                daily_trade_count: broker.daily_trade_count,
                consecutive_losses: broker.consecutive_losses,
                cooldown_until: broker.cooldown_until,
                buying_power: broker.buying_power,
                active_rule_version: broker.active_rule_version,
            },
            evaluated_at: now,
            limits: broker.limits,
        });
        let kind = if decision.approved {
            RiskDecisionKind::Approved
        } else {
            RiskDecisionKind::Rejected
        };
        Ok(ProtoDecision {
            schema_version: "1.0".into(),
            decision_id: format!("risk_{}_{}", raw_plan.plan_id, now.timestamp_millis()),
            plan_id: raw_plan.plan_id.clone(),
            plan_hash: raw_plan.plan_hash.clone(),
            session_id: raw_plan.session_id.clone(),
            occurred_at_utc: now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            decision: kind as i32,
            reason_codes: decision
                .reasons
                .into_iter()
                .map(|reason| reason_proto(reason) as i32)
                .collect(),
            manual_confirmation_required: true,
            rule_version: raw_plan.rule_version.clone(),
        })
    }
}

fn utc(value: &str, label: &'static str) -> Result<DateTime<Utc>, &'static str> {
    if !value.ends_with('Z') {
        return Err(label);
    }
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| label)
}

fn decimal(value: &str, label: &'static str) -> Result<Decimal, &'static str> {
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

fn broker_legs(raw_plan: &ProtoPlan) -> Result<Vec<BrokerOrderLeg>, &'static str> {
    raw_plan
        .legs
        .iter()
        .map(|leg| {
            let side = match ProtoSide::try_from(leg.side) {
                Ok(ProtoSide::Buy) => AdapterOrderSide::Buy,
                Ok(ProtoSide::Sell) => AdapterOrderSide::Sell,
                _ => return Err("staged leg side is invalid"),
            };
            Ok(BrokerOrderLeg {
                contract_id: leg.contract_id.clone(),
                side,
                quantity: leg.quantity,
            })
        })
        .collect()
}

fn digest<T: Message + Clone>(message: &T, clear: impl FnOnce(&mut T)) -> String {
    let mut canonical = message.clone();
    clear(&mut canonical);
    format!("{:x}", Sha256::digest(canonical.encode_to_vec()))
}

fn map_broker(value: i32) -> Result<BrokerId, &'static str> {
    match ProtoBrokerId::try_from(value).ok() {
        Some(ProtoBrokerId::Longbridge) => Ok(BrokerId::Longbridge),
        Some(ProtoBrokerId::Ibkr) => Ok(BrokerId::Ibkr),
        _ => Err("broker_id"),
    }
}

fn map_strategy(value: i32) -> Result<StrategyKind, &'static str> {
    match ProtoStrategy::try_from(value).ok() {
        Some(ProtoStrategy::LongGamma) => Ok(StrategyKind::LongGamma),
        Some(ProtoStrategy::ShortPremium) => Ok(StrategyKind::ShortPremium),
        Some(ProtoStrategy::EventVolCrush) => Ok(StrategyKind::EventVolCrush),
        _ => Err("strategy"),
    }
}

fn map_mode(value: i32) -> Result<ExecutionMode, &'static str> {
    match ProtoMode::try_from(value).ok() {
        Some(ProtoMode::Replay) => Ok(ExecutionMode::Replay),
        Some(ProtoMode::Shadow) => Ok(ExecutionMode::Shadow),
        Some(ProtoMode::Paper) => Ok(ExecutionMode::Paper),
        Some(ProtoMode::ManualConfirm) => Ok(ExecutionMode::ManualConfirm),
        Some(ProtoMode::ControlledAuto) => Ok(ExecutionMode::ControlledAuto),
        _ => Err("execution_mode"),
    }
}

fn map_side(value: i32) -> Result<OrderSide, &'static str> {
    match ProtoSide::try_from(value).ok() {
        Some(ProtoSide::Buy) => Ok(OrderSide::Buy),
        Some(ProtoSide::Sell) => Ok(OrderSide::Sell),
        _ => Err("side"),
    }
}

fn map_right(value: i32) -> Result<OptionRight, &'static str> {
    match ProtoRight::try_from(value).ok() {
        Some(ProtoRight::Call) => Ok(OptionRight::Call),
        Some(ProtoRight::Put) => Ok(OptionRight::Put),
        _ => Err("option_right"),
    }
}

fn plan(raw: &ProtoPlan) -> Result<CandidatePlan, &'static str> {
    if raw.schema_version != "1.0" || raw.plan_hash.len() != 64 {
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
            Ok(CandidateLeg {
                side: map_side(leg.side)?,
                option_right: map_right(leg.option_right)?,
                contract_id: leg.contract_id.clone(),
                expiry: leg.expiry.clone(),
                strike: decimal(&leg.strike, "strike")?,
                quantity: leg.quantity,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
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
    })
}

fn event_flag(value: i32) -> Result<EventRiskFlag, &'static str> {
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

fn event_context(raw: &ProtoEventContext) -> Result<EventRiskContext, &'static str> {
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

fn reason_proto(reason: RiskReasonCode) -> ProtoReason {
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
    }
}

fn rejected(raw: Option<&ProtoPlan>, now: DateTime<Utc>, reason: ProtoReason) -> ProtoDecision {
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

fn proto_order_state(state: OrderState) -> ProtoOrderState {
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

fn order_proto(staged: &StagedOrder, now: DateTime<Utc>) -> ProtoOrder {
    let updated_at = staged
        .record
        .events
        .last()
        .map_or(now, |event| event.occurred_at);
    ProtoOrder {
        schema_version: "1.0".into(),
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
        state_version: staged.record.events.len() as u64,
    }
}

fn workflow_lock(
    service: &RiskExecutionServiceImpl,
) -> Result<std::sync::MutexGuard<'_, Workflow>, ()> {
    service.workflow.lock().map_err(|_| ())
}

#[tonic::async_trait]
impl RiskExecutionService for RiskExecutionServiceImpl {
    async fn evaluate_candidate(
        &self,
        request: Request<EvaluateCandidateRequest>,
    ) -> Result<Response<ProtoDecision>, Status> {
        let now = (self.clock)();
        let raw = request.into_inner();
        Ok(Response::new(
            self.evaluate_raw(raw.plan.as_ref(), raw.event_context.as_ref(), now)
                .await?,
        ))
    }

    async fn stage_candidate(
        &self,
        request: Request<EvaluateCandidateRequest>,
    ) -> Result<Response<StageCandidateResponse>, Status> {
        let now = (self.clock)();
        let raw = request.into_inner();
        let decision = self
            .evaluate_raw(raw.plan.as_ref(), raw.event_context.as_ref(), now)
            .await?;
        if decision.decision != RiskDecisionKind::Approved as i32 {
            return Ok(Response::new(StageCandidateResponse {
                initial_risk_decision: Some(decision),
                order: None,
                confirmation_token: String::new(),
            }));
        }
        let raw_plan = raw
            .plan
            .ok_or_else(|| Status::internal("approved decision had no plan"))?;
        let domain_plan = plan(&raw_plan)
            .map_err(|_| Status::internal("approved decision had an invalid plan"))?;
        let order_id = format!("order_{}", &raw_plan.plan_hash[..24]);
        let mut workflow = workflow_lock(self)
            .map_err(|()| Status::internal("execution workflow lock poisoned"))?;
        if let Some((existing_hash, existing_order_id)) =
            workflow.order_by_key.get(&raw_plan.idempotency_key)
        {
            if existing_hash != &raw_plan.plan_hash {
                let conflict = rejected(Some(&raw_plan), now, ProtoReason::DuplicateConflict);
                return Ok(Response::new(StageCandidateResponse {
                    initial_risk_decision: Some(conflict),
                    order: None,
                    confirmation_token: String::new(),
                }));
            }
            let existing = workflow
                .orders
                .get(existing_order_id)
                .ok_or_else(|| Status::internal("idempotency index references missing order"))?;
            return Ok(Response::new(StageCandidateResponse {
                initial_risk_decision: Some(decision),
                order: Some(order_proto(existing, now)),
                confirmation_token: existing.confirmation_token.clone(),
            }));
        }
        let total_quantity = domain_plan
            .legs
            .first()
            .ok_or_else(|| Status::internal("approved plan had no legs"))?
            .quantity;
        let mut record = OrderRecord::proposed(
            order_id.clone(),
            raw_plan.plan_id.clone(),
            raw_plan.plan_hash.clone(),
            raw_plan.idempotency_key.clone(),
            domain_plan.expires_at,
            total_quantity,
        )
        .map_err(|_| Status::internal("approved plan could not create order"))?;
        record
            .initial_risk(true, now)
            .map_err(|_| Status::internal("initial risk transition failed"))?;
        let confirmation_token = Uuid::new_v4().simple().to_string();
        let staged = StagedOrder {
            raw_plan: raw_plan.clone(),
            record,
            confirmation_token: confirmation_token.clone(),
            risk_reasons: Vec::new(),
        };
        workflow.order_by_key.insert(
            raw_plan.idempotency_key,
            (raw_plan.plan_hash, order_id.clone()),
        );
        let order = order_proto(&staged, now);
        workflow.orders.insert(order_id, staged);
        Ok(Response::new(StageCandidateResponse {
            initial_risk_decision: Some(decision),
            order: Some(order),
            confirmation_token,
        }))
    }

    async fn confirm_candidate(
        &self,
        request: Request<ConfirmCandidateRequest>,
    ) -> Result<Response<ProtoOrder>, Status> {
        let now = (self.clock)();
        let raw = request.into_inner();
        let stored_plan = {
            let workflow = workflow_lock(self)
                .map_err(|()| Status::internal("execution workflow lock poisoned"))?;
            let staged = workflow
                .orders
                .get(&raw.order_id)
                .ok_or_else(|| Status::not_found("order not found"))?;
            if staged.record.plan_hash != raw.confirmed_plan_hash
                || staged.confirmation_token != raw.confirmation_token
            {
                return Err(Status::permission_denied(
                    "confirmation does not match staged plan",
                ));
            }
            if staged.record.state != OrderState::AwaitingConfirmation {
                return Ok(Response::new(order_proto(staged, now)));
            }
            staged.raw_plan.clone()
        };
        let decision = self
            .evaluate_raw(Some(&stored_plan), raw.event_context.as_ref(), now)
            .await?;
        let adapter_legs = broker_legs(&stored_plan).map_err(Status::internal)?;
        let mut workflow = workflow_lock(self)
            .map_err(|()| Status::internal("execution workflow lock poisoned"))?;
        let Workflow {
            orders,
            longbridge_paper,
            ibkr_paper,
            ..
        } = &mut *workflow;
        let staged = orders
            .get_mut(&raw.order_id)
            .ok_or_else(|| Status::not_found("order disappeared during confirmation"))?;
        if staged.record.state != OrderState::AwaitingConfirmation {
            return Ok(Response::new(order_proto(staged, now)));
        }
        if decision.decision != RiskDecisionKind::Approved as i32 {
            staged.risk_reasons = decision.reason_codes;
            staged
                .record
                .final_risk_rejected(now)
                .map_err(|_| Status::failed_precondition("order no longer confirmable"))?;
        } else {
            staged
                .record
                .confirm(
                    format!("confirm-{}", Uuid::new_v4().simple()),
                    &raw.confirmed_plan_hash,
                    now,
                )
                .map_err(|error| {
                    Status::failed_precondition(format!("confirmation failed: {error:?}"))
                })?;
            let mode = ProtoMode::try_from(staged.raw_plan.execution_mode)
                .map_err(|_| Status::internal("staged execution mode is invalid"))?;
            if matches!(mode, ProtoMode::Replay | ProtoMode::Shadow) {
                staged
                    .record
                    .complete_shadow(now)
                    .map_err(|_| Status::internal("shadow transition failed"))?;
            } else {
                staged.record.begin_submit(now).map_err(|error| {
                    Status::failed_precondition(format!("submit blocked: {error:?}"))
                })?;
                let limit_price = decimal(&staged.raw_plan.limit_price, "limit_price")
                    .map_err(|_| Status::internal("staged limit price is invalid"))?;
                let adapter = match ProtoBrokerId::try_from(staged.raw_plan.broker_id) {
                    Ok(ProtoBrokerId::Longbridge) => longbridge_paper,
                    Ok(ProtoBrokerId::Ibkr) => ibkr_paper,
                    _ => return Err(Status::internal("staged broker is invalid")),
                };
                if let Err(error) =
                    submit_to_broker(&mut staged.record, adapter, limit_price, adapter_legs, now)
                {
                    if matches!(
                        error,
                        execution::ExecutionError::Broker(BrokerError::Disconnected)
                            | execution::ExecutionError::Broker(BrokerError::NotReconciled)
                    ) {
                        staged
                            .record
                            .broker_disconnected(now)
                            .map_err(|_| Status::internal("broker disconnect transition failed"))?;
                    } else {
                        staged
                            .record
                            .submission_rejected(now)
                            .map_err(|_| Status::internal("broker rejection transition failed"))?;
                    }
                }
            }
        }
        let response = order_proto(staged, now);
        Ok(Response::new(response))
    }

    async fn cancel_order(
        &self,
        request: Request<CancelOrderRequest>,
    ) -> Result<Response<ProtoOrder>, Status> {
        let now = (self.clock)();
        let order_id = request.into_inner().order_id;
        let mut workflow = workflow_lock(self)
            .map_err(|()| Status::internal("execution workflow lock poisoned"))?;
        let Workflow {
            orders,
            longbridge_paper,
            ibkr_paper,
            ..
        } = &mut *workflow;
        let staged = orders
            .get_mut(&order_id)
            .ok_or_else(|| Status::not_found("order not found"))?;
        match staged.record.state {
            OrderState::AwaitingConfirmation => staged
                .record
                .cancel_unsubmitted(now)
                .map_err(|_| Status::internal("pre-submit cancel transition failed"))?,
            OrderState::Working | OrderState::PartialFill => {
                let Some(broker_order_id) = staged.record.broker_order_id.clone() else {
                    staged.record.broker_disconnected(now).map_err(|_| {
                        Status::internal("invalid working order could not enter reconciliation")
                    })?;
                    return Err(Status::internal("working order lacks broker id"));
                };
                staged
                    .record
                    .request_cancel(now)
                    .map_err(|_| Status::failed_precondition("order cannot be cancelled"))?;
                let adapter = match ProtoBrokerId::try_from(staged.raw_plan.broker_id) {
                    Ok(ProtoBrokerId::Longbridge) => longbridge_paper,
                    Ok(ProtoBrokerId::Ibkr) => ibkr_paper,
                    _ => return Err(Status::internal("staged broker is invalid")),
                };
                match adapter.cancel(&broker_order_id) {
                    Ok(order) => {
                        if staged.record.apply_broker_order(&order, now).is_err() {
                            staged.record.broker_disconnected(now).map_err(|_| {
                                Status::internal(
                                    "conflicting cancel could not enter reconciliation",
                                )
                            })?;
                            return Err(Status::internal("broker cancel result conflicted"));
                        }
                    }
                    Err(BrokerError::Disconnected | BrokerError::NotReconciled) => staged
                        .record
                        .broker_disconnected(now)
                        .map_err(|_| Status::internal("cancel reconciliation transition failed"))?,
                    Err(error) => {
                        return Err(Status::failed_precondition(format!(
                            "broker cancel failed: {error:?}"
                        )))
                    }
                }
            }
            _ if staged.record.state.is_terminal() => {}
            _ => return Err(Status::failed_precondition("order cannot be cancelled")),
        }
        let response = order_proto(staged, now);
        Ok(Response::new(response))
    }

    async fn get_order(
        &self,
        request: Request<GetOrderRequest>,
    ) -> Result<Response<ProtoOrder>, Status> {
        let now = (self.clock)();
        let order_id = request.into_inner().order_id;
        let workflow = workflow_lock(self)
            .map_err(|()| Status::internal("execution workflow lock poisoned"))?;
        let staged = workflow
            .orders
            .get(&order_id)
            .ok_or_else(|| Status::not_found("order not found"))?;
        Ok(Response::new(order_proto(staged, now)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use optiontrader_proto::execution_v1::{
        CandidateLeg as ProtoLeg, EventSourceProof as ProtoSource, OptionRight as ProtoRight,
        OrderSide as ProtoSide,
    };

    fn now() -> DateTime<Utc> {
        "2026-07-20T13:45:01Z".parse().unwrap()
    }

    fn broker_authority() -> BrokerAuthority {
        BrokerAuthority {
            health: BrokerHealth::Healthy,
            reconciled: true,
            risk_limits_confirmed: true,
            kill_switch_active: false,
            daily_realized_pnl: Decimal::ZERO,
            open_risk: Decimal::ZERO,
            daily_trade_count: 0,
            consecutive_losses: 0,
            cooldown_until: None,
            buying_power: Decimal::new(100_000, 0),
            active_rule_version: "rules-p3".into(),
            limits: RiskLimits {
                max_plan_loss: Decimal::new(1_000, 0),
                max_daily_loss: Decimal::new(1_000, 0),
                max_open_risk: Decimal::new(1_000, 0),
                max_daily_trades: 3,
                max_contracts: 2,
            },
        }
    }

    fn service() -> RiskExecutionServiceImpl {
        RiskExecutionServiceImpl::with_clock(
            MarketAuthority::Fixed(DataHealth::Healthy, MarketSnapshot::fixture()),
            broker_authority(),
            now,
        )
    }

    #[test]
    fn health_gate_includes_account_limits_and_kill_switch() {
        let authority = broker_authority();
        assert!(authority.allows_new_position(true, now()));

        let mut changed = authority.clone();
        changed.kill_switch_active = true;
        assert!(!changed.allows_new_position(true, now()));

        let mut changed = authority.clone();
        changed.daily_realized_pnl = -changed.limits.max_daily_loss;
        assert!(!changed.allows_new_position(true, now()));

        let mut changed = authority.clone();
        changed.daily_trade_count = changed.limits.max_daily_trades;
        assert!(!changed.allows_new_position(true, now()));

        let mut changed = authority.clone();
        changed.consecutive_losses = 3;
        assert!(!changed.allows_new_position(true, now()));

        let mut changed = authority;
        changed.buying_power = Decimal::ZERO;
        assert!(!changed.allows_new_position(true, now()));
    }

    #[test]
    fn boolean_configuration_rejects_ambiguous_values() {
        assert_eq!(parse_boolean("FLAG", None, false), Ok(false));
        assert_eq!(parse_boolean("FLAG", Some("true"), false), Ok(true));
        assert_eq!(parse_boolean("FLAG", Some("false"), true), Ok(false));
        assert!(parse_boolean("FLAG", Some("TRUE"), false).is_err());
        assert!(parse_boolean("FLAG", Some("0"), false).is_err());
    }

    fn candidate(mode: ProtoMode) -> ProtoPlan {
        let mut value = ProtoPlan {
            schema_version: "1.0".into(),
            plan_id: String::new(),
            plan_hash: String::new(),
            idempotency_key: String::new(),
            session_id: "session-20260720".into(),
            signal_id: "signal-1".into(),
            broker_id: ProtoBrokerId::Ibkr as i32,
            strategy: ProtoStrategy::LongGamma as i32,
            execution_mode: mode as i32,
            created_at_utc: "2026-07-20T13:45:00Z".into(),
            legs: vec![ProtoLeg {
                side: ProtoSide::Buy as i32,
                option_right: ProtoRight::Call as i32,
                contract_id: "QQQ-20260720-C-500".into(),
                expiry: "2026-07-20".into(),
                strike: "500".into(),
                quantity: 2,
            }],
            limit_price: "2.50".into(),
            max_slippage: "0.10".into(),
            max_loss: "500".into(),
            take_profit: "200".into(),
            stop_loss: "150".into(),
            time_stop_minutes: 30,
            invalidation_rules: vec!["market_context_changes".into()],
            expires_at_utc: "2026-07-20T13:46:00Z".into(),
            rule_version: "rules-p3".into(),
            data_snapshot_ids: vec![MarketSnapshot::fixture().snapshot_id],
            manual_confirmation_required: true,
        };
        let hash = digest(&value, |plan| {
            plan.plan_id.clear();
            plan.plan_hash.clear();
            plan.idempotency_key.clear();
        });
        value.plan_id = format!("plan_{}", &hash[..24]);
        value.plan_hash = hash.clone();
        value.idempotency_key = format!("submit_{hash}");
        value
    }

    fn context() -> ProtoEventContext {
        let source = |category: &str, confidence: f64| ProtoSource {
            category: category.into(),
            source_timestamp_utc: "2026-07-20T12:00:00Z".into(),
            received_at_utc: "2026-07-20T12:05:00Z".into(),
            confidence,
            raw_ref: format!("fixture://{category}"),
        };
        let mut value = ProtoEventContext {
            event_context_id: "event-1".into(),
            trading_date: "2026-07-20".into(),
            generated_at_utc: "2026-07-20T13:45:00Z".into(),
            available: true,
            source_documents: vec![
                source("macro", 0.8),
                source("holdings", 0.9),
                source("earnings", 0.8),
                source("news", 0.8),
            ],
            risk_flags: vec![ProtoEventFlag::NoNaked0dte as i32],
            minutes_to_major_event: None,
            event_released: false,
            context_hash: String::new(),
        };
        value.context_hash = digest(&value, |context| context.context_hash.clear());
        value
    }

    async fn stage(service: &RiskExecutionServiceImpl, mode: ProtoMode) -> StageCandidateResponse {
        service
            .stage_candidate(Request::new(EvaluateCandidateRequest {
                plan: Some(candidate(mode)),
                event_context: Some(context()),
            }))
            .await
            .unwrap()
            .into_inner()
    }

    #[tokio::test]
    async fn shadow_confirmation_is_hash_bound_idempotent_and_never_working() {
        let service = service();
        let staged = stage(&service, ProtoMode::Shadow).await;
        let order = staged.order.as_ref().unwrap();
        assert_eq!(
            ProtoOrderState::try_from(order.state).unwrap(),
            ProtoOrderState::AwaitingConfirmation
        );

        let repeated = stage(&service, ProtoMode::Shadow).await;
        assert_eq!(repeated.confirmation_token, staged.confirmation_token);
        let repeated_order = repeated.order.unwrap();
        assert_eq!(repeated_order.order_id, order.order_id);
        assert_eq!(repeated_order.state_version, order.state_version);
        assert_eq!(repeated_order.updated_at_utc, order.updated_at_utc);

        let denied = service
            .confirm_candidate(Request::new(ConfirmCandidateRequest {
                order_id: order.order_id.clone(),
                confirmed_plan_hash: order.plan_hash.clone(),
                confirmation_token: "wrong".into(),
                event_context: Some(context()),
            }))
            .await
            .unwrap_err();
        assert_eq!(denied.code(), tonic::Code::PermissionDenied);

        let confirmed = service
            .confirm_candidate(Request::new(ConfirmCandidateRequest {
                order_id: order.order_id.clone(),
                confirmed_plan_hash: order.plan_hash.clone(),
                confirmation_token: staged.confirmation_token.clone(),
                event_context: Some(context()),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            ProtoOrderState::try_from(confirmed.state).unwrap(),
            ProtoOrderState::Shadowed
        );
        let again = service
            .confirm_candidate(Request::new(ConfirmCandidateRequest {
                order_id: order.order_id.clone(),
                confirmed_plan_hash: order.plan_hash.clone(),
                confirmation_token: staged.confirmation_token,
                event_context: Some(context()),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(again.state, confirmed.state);
        assert!(again.broker_order_id.is_empty());
    }

    #[tokio::test]
    async fn final_risk_change_rejects_without_broker_submission() {
        let service = service();
        let staged = stage(&service, ProtoMode::Paper).await;
        let order = staged.order.unwrap();
        let mut unavailable = context();
        unavailable.available = false;
        unavailable.context_hash = digest(&unavailable, |value| value.context_hash.clear());
        let rejected = service
            .confirm_candidate(Request::new(ConfirmCandidateRequest {
                order_id: order.order_id,
                confirmed_plan_hash: order.plan_hash,
                confirmation_token: staged.confirmation_token,
                event_context: Some(unavailable),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            ProtoOrderState::try_from(rejected.state).unwrap(),
            ProtoOrderState::RiskRejected
        );
        assert!(rejected
            .risk_reason_codes
            .contains(&(ProtoReason::EventContextUnavailable as i32)));
        assert!(rejected.broker_order_id.is_empty());
    }

    #[tokio::test]
    async fn paper_confirmation_submits_once_and_cancel_is_idempotent() {
        let service = service();
        let staged = stage(&service, ProtoMode::Paper).await;
        let order = staged.order.unwrap();
        let working = service
            .confirm_candidate(Request::new(ConfirmCandidateRequest {
                order_id: order.order_id.clone(),
                confirmed_plan_hash: order.plan_hash,
                confirmation_token: staged.confirmation_token,
                event_context: Some(context()),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            ProtoOrderState::try_from(working.state).unwrap(),
            ProtoOrderState::Working
        );
        assert!(!working.broker_order_id.is_empty());
        let cancelled = service
            .cancel_order(Request::new(CancelOrderRequest {
                order_id: order.order_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            ProtoOrderState::try_from(cancelled.state).unwrap(),
            ProtoOrderState::Cancelled
        );
        let again = service
            .cancel_order(Request::new(CancelOrderRequest {
                order_id: order.order_id,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(again.state, cancelled.state);
    }

    #[tokio::test]
    async fn cancel_internal_error_preserves_order_and_requires_reconciliation() {
        let service = service();
        let staged = stage(&service, ProtoMode::Paper).await;
        let order = staged.order.unwrap();
        service
            .confirm_candidate(Request::new(ConfirmCandidateRequest {
                order_id: order.order_id.clone(),
                confirmed_plan_hash: order.plan_hash,
                confirmation_token: staged.confirmation_token,
                event_context: Some(context()),
            }))
            .await
            .unwrap();
        {
            let mut workflow = workflow_lock(&service).unwrap();
            workflow
                .orders
                .get_mut(&order.order_id)
                .unwrap()
                .record
                .broker_order_id = None;
        }

        let error = service
            .cancel_order(Request::new(CancelOrderRequest {
                order_id: order.order_id.clone(),
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal);

        let preserved = service
            .get_order(Request::new(GetOrderRequest {
                order_id: order.order_id,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            ProtoOrderState::try_from(preserved.state).unwrap(),
            ProtoOrderState::ReconcilePending
        );
    }
}
