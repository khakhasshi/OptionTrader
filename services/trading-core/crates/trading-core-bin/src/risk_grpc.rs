//! gRPC boundary for Rust Final Risk Check.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::{Arc, Mutex, RwLock};

use broker::{
    price_adaptive_limit, AdaptiveLimitPolicy as AdapterAdaptivePolicy, AdaptivePriceError,
    BrokerAdapter, BrokerError, BrokerId as AdapterBrokerId, BrokerOrderLeg,
    BrokerOrderStatus as AdapterOrderStatus, BrokerOrderType as AdapterOrderType,
    OrderSide as AdapterOrderSide, PaperBroker, QuoteProof,
};
use chrono::{DateTime, Utc};
use execution::{submit_to_broker, OrderRecord, OrderState};
use market_core::{DataHealth, MarketSnapshot};
use optiontrader_proto::execution_v1::{
    risk_execution_service_server::RiskExecutionService, BeginBrokerReconciliationRequest,
    BrokerId as ProtoBrokerId, BrokerOrderType as ProtoOrderType, BrokerReconciliationBatch,
    CancelOrderRequest, CandidateTradePlan as ProtoPlan, CommitBrokerReconciliationRequest,
    CommitBrokerReconciliationResponse, ConfirmCandidateRequest, EvaluateCandidateRequest,
    EventRiskContext as ProtoEventContext, EventRiskFlag as ProtoEventFlag,
    ExecutionChildOrder as ProtoChildOrder, ExecutionChildOrderState as ProtoChildState,
    ExecutionMode as ProtoMode, ExecutionOrder as ProtoOrder,
    ExecutionOrderState as ProtoOrderState, GetOrderRequest, OptionRight as ProtoRight,
    OrderSide as ProtoSide, PositionEffect as ProtoPositionEffect, ReconcileExecutionOrderRequest,
    RestorableExecutionOrder, RestoreWorkflowRequest, RestoreWorkflowResponse,
    RiskDecision as ProtoDecision, RiskDecisionKind, RiskReasonCode as ProtoReason,
    StageCandidateResponse, StrategyKind as ProtoStrategy,
};
use prost::Message;
use risk_gateway::{
    final_risk_check, new_position_allowed, AuthorityState, BrokerHealth, BrokerId,
    BrokerOrderType, CandidateLeg, CandidatePlan, EventRiskContext, EventRiskFlag,
    EventSourceProof, ExecutionMode, FinalRiskInput, OptionQuoteProof, OptionRight, OrderSide,
    PositionEffect, RiskLimits, RiskReasonCode, StrategyKind,
};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex as AsyncMutex;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::broker_registry::{
    expected_request, BrokerMutationError, BrokerRecoveryError, BrokerSnapshotAuthority,
    ValidatedBrokerSnapshot,
};
use crate::grpc::{LiveMarketServiceImpl, MarketServiceImpl};
use crate::option_registry::OptionRegistryAuthority;

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
    pub allowed_strategies: BTreeSet<StrategyKind>,
    pub positions: BTreeMap<String, i32>,
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
        let allowed_strategies = std::env::var("OPTIONTRADER_ALLOWED_STRATEGIES")
            .unwrap_or_default()
            .split(',')
            .filter(|value| !value.trim().is_empty())
            .map(|value| match value.trim() {
                "LongGamma" => Ok(StrategyKind::LongGamma),
                "ShortPremium" => Ok(StrategyKind::ShortPremium),
                "EventVolCrush" => Ok(StrategyKind::EventVolCrush),
                _ => Err("OPTIONTRADER_ALLOWED_STRATEGIES contains an unknown strategy"),
            })
            .collect::<Result<BTreeSet<_>, _>>()
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
            allowed_strategies,
            positions: BTreeMap::new(),
            limits: RiskLimits {
                max_plan_loss: decimal_env("OPTIONTRADER_MAX_PLAN_LOSS", "250")?,
                max_daily_loss: decimal_env("OPTIONTRADER_MAX_DAILY_LOSS", "500")?,
                max_open_risk: decimal_env("OPTIONTRADER_MAX_OPEN_RISK", "500")?,
                max_daily_trades: integer_env("OPTIONTRADER_MAX_DAILY_TRADES", 3)?,
                max_contracts: integer_env("OPTIONTRADER_MAX_CONTRACTS", 2)?,
                max_quote_age_ms: u64::from(integer_env("OPTIONTRADER_MAX_QUOTE_AGE_MS", 120_000)?),
                max_option_spread_bps: integer_env("OPTIONTRADER_MAX_OPTION_SPREAD_BPS", 3_000)?,
                entry_start_minute_et: u16::try_from(integer_env(
                    "OPTIONTRADER_ENTRY_START_MINUTE_ET",
                    9 * 60 + 35,
                )?)
                .map_err(|_| "OPTIONTRADER_ENTRY_START_MINUTE_ET is too large")?,
                entry_cutoff_minute_et: u16::try_from(integer_env(
                    "OPTIONTRADER_ENTRY_CUTOFF_MINUTE_ET",
                    15 * 60 + 30,
                )?)
                .map_err(|_| "OPTIONTRADER_ENTRY_CUTOFF_MINUTE_ET is too large")?,
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
        if self.risk_limits_confirmed && self.allowed_strategies.is_empty() {
            return Err("confirmed risk limits require an explicit strategy whitelist".into());
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
        if self.limits.max_quote_age_ms == 0
            || self.limits.max_option_spread_bps == 0
            || self.limits.max_option_spread_bps > 10_000
            || self.limits.entry_start_minute_et >= self.limits.entry_cutoff_minute_et
            || self.limits.entry_cutoff_minute_et > 24 * 60
        {
            return Err("quote and entry-window limits are invalid".into());
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
    options: OptionRegistryAuthority,
    broker_snapshots: BrokerSnapshotAuthority,
    broker_mutations: BrokerSnapshotAuthority,
    execution_backend: BrokerExecutionBackend,
    broker_reconciliations: Arc<AsyncMutex<BTreeMap<i32, PendingBrokerReconciliation>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BrokerExecutionBackend {
    Disabled,
    SimulatedPaper,
    IbkrPaper,
    LongbridgePaper,
}

impl BrokerExecutionBackend {
    fn from_env() -> Result<Self, String> {
        let backend = std::env::var("OPTIONTRADER_BROKER_EXECUTION_BACKEND")
            .unwrap_or_else(|_| "simulated-paper".into());
        let environment = std::env::var("OPTIONTRADER_ENV").unwrap_or_else(|_| "local".into());
        let live_enabled = boolean_env("LIVE_TRADING_ENABLED", false)?;
        let paper_opt_in = boolean_env("OPTIONTRADER_BROKER_PAPER_SUBMISSION_ENABLED", false)?;
        let ibkr_paper = boolean_env("OPTIONTRADER_IBKR_PAPER", true)?;
        let ibkr_submission = boolean_env("OPTIONTRADER_IBKR_SUBMISSION_ENABLED", false)?;
        let longbridge_paper = boolean_env("OPTIONTRADER_LONGBRIDGE_PAPER", false)?;
        let reconciliation_enabled =
            boolean_env("OPTIONTRADER_BROKER_RECONCILIATION_ENABLED", true)?;
        let reconciliation_broker = std::env::var("OPTIONTRADER_BROKER_RECONCILIATION_BROKERS")
            .unwrap_or_else(|_| "ibkr".into());
        Self::from_config(
            &backend,
            &environment,
            live_enabled,
            paper_opt_in,
            ibkr_paper,
            ibkr_submission,
            longbridge_paper,
        )
        .and_then(|backend| {
            backend.require_reconciliation_route(reconciliation_enabled, &reconciliation_broker)
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn from_config(
        backend: &str,
        environment: &str,
        live_enabled: bool,
        paper_opt_in: bool,
        ibkr_paper: bool,
        ibkr_submission: bool,
        longbridge_paper: bool,
    ) -> Result<Self, String> {
        if live_enabled {
            return Err("Phase 3 requires LIVE_TRADING_ENABLED=false".into());
        }
        match backend {
            "disabled" => Ok(Self::Disabled),
            "simulated-paper" => Ok(Self::SimulatedPaper),
            "ibkr-paper"
                if environment == "paper"
                    && paper_opt_in
                    && ibkr_paper
                    && ibkr_submission =>
            {
                Ok(Self::IbkrPaper)
            }
            "longbridge-paper" if environment == "paper" && paper_opt_in && longbridge_paper => {
                Ok(Self::LongbridgePaper)
            }
            "ibkr-paper" | "longbridge-paper" => Err(
                "real paper execution requires paper environment and every broker opt-in".into(),
            ),
            _ => Err(
                "OPTIONTRADER_BROKER_EXECUTION_BACKEND must be disabled, simulated-paper, ibkr-paper, or longbridge-paper"
                    .into(),
            ),
        }
    }

    fn allows(self, mode: ProtoMode, broker: ProtoBrokerId) -> bool {
        if matches!(mode, ProtoMode::Replay | ProtoMode::Shadow) {
            return true;
        }
        match self {
            Self::Disabled => false,
            Self::SimulatedPaper => matches!(mode, ProtoMode::Paper | ProtoMode::ManualConfirm),
            Self::IbkrPaper => {
                broker == ProtoBrokerId::Ibkr
                    && matches!(mode, ProtoMode::Paper | ProtoMode::ManualConfirm)
            }
            Self::LongbridgePaper => {
                broker == ProtoBrokerId::Longbridge
                    && matches!(mode, ProtoMode::Paper | ProtoMode::ManualConfirm)
            }
        }
    }

    fn is_external(self) -> bool {
        matches!(self, Self::IbkrPaper | Self::LongbridgePaper)
    }

    fn require_reconciliation_route(
        self,
        reconciliation_enabled: bool,
        reconciliation_broker: &str,
    ) -> Result<Self, String> {
        let expected = match self {
            Self::IbkrPaper => Some("ibkr"),
            Self::LongbridgePaper => Some("longbridge"),
            Self::Disabled | Self::SimulatedPaper => None,
        };
        if expected
            .is_some_and(|broker| !reconciliation_enabled || reconciliation_broker.trim() != broker)
        {
            return Err(
                "real paper execution requires enabled reconciliation for the same broker".into(),
            );
        }
        Ok(self)
    }

    fn is_simulated(self) -> bool {
        self == Self::SimulatedPaper
    }

    fn broker_route(self) -> Option<ProtoBrokerId> {
        match self {
            Self::IbkrPaper => Some(ProtoBrokerId::Ibkr),
            Self::LongbridgePaper => Some(ProtoBrokerId::Longbridge),
            Self::Disabled | Self::SimulatedPaper => None,
        }
    }
}

#[derive(Clone)]
struct PendingBrokerReconciliation {
    snapshot_sequence: u64,
    snapshot_hash: String,
    expires_at: DateTime<Utc>,
    buying_power: Decimal,
    positions: BTreeMap<String, i32>,
    committed: bool,
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
    pub fn new(market: MarketAuthority, broker: BrokerAuthority) -> Result<Self, String> {
        let execution_backend = BrokerExecutionBackend::from_env()?;
        Ok(Self {
            market,
            broker: Arc::new(RwLock::new(broker)),
            workflow: Arc::new(Mutex::new(Workflow::default())),
            clock: Arc::new(Utc::now),
            options: OptionRegistryAuthority::from_endpoint(
                std::env::var("THETADATA_SDK_GRPC")
                    .unwrap_or_else(|_| "http://127.0.0.1:50052".into()),
            ),
            broker_snapshots: BrokerSnapshotAuthority::from_env(false),
            broker_mutations: BrokerSnapshotAuthority::from_env(
                execution_backend == BrokerExecutionBackend::LongbridgePaper,
            ),
            execution_backend,
            broker_reconciliations: Arc::new(AsyncMutex::new(BTreeMap::new())),
        })
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
            options: OptionRegistryAuthority::Fixture,
            broker_snapshots: BrokerSnapshotAuthority::Fixed(Err(BrokerRecoveryError::Unavailable)),
            broker_mutations: BrokerSnapshotAuthority::Fixed(Err(BrokerRecoveryError::Unavailable)),
            execution_backend: BrokerExecutionBackend::SimulatedPaper,
            broker_reconciliations: Arc::new(AsyncMutex::new(BTreeMap::new())),
        }
    }

    pub fn broker_handle(&self) -> Arc<RwLock<BrokerAuthority>> {
        Arc::clone(&self.broker)
    }

    async fn evaluate_raw(
        &self,
        raw_plan: Option<&ProtoPlan>,
        raw_event: Option<&ProtoEventContext>,
        now: DateTime<Utc>,
        refresh_options: bool,
    ) -> Result<ProtoDecision, Status> {
        let Some(raw_plan) = raw_plan else {
            return Ok(rejected(None, now, ProtoReason::PlanInvalid));
        };
        let domain_plan = match plan(raw_plan) {
            Ok(value) => value,
            Err(_) => return Ok(rejected(Some(raw_plan), now, ProtoReason::PlanInvalid)),
        };
        let mode = ProtoMode::try_from(raw_plan.execution_mode).ok();
        let broker = ProtoBrokerId::try_from(raw_plan.broker_id).ok();
        if !matches!((mode, broker), (Some(mode), Some(broker)) if self.execution_backend.allows(mode, broker))
        {
            return Ok(rejected(
                Some(raw_plan),
                now,
                ProtoReason::ExecutionModeBlocked,
            ));
        }
        if !self.options.verify(raw_plan, now, refresh_options).await {
            return Ok(rejected(
                Some(raw_plan),
                now,
                ProtoReason::SnapshotNotCurrent,
            ));
        }
        let closing = domain_plan.position_effect == PositionEffect::Close;
        let domain_event = match raw_event.map(event_context) {
            Some(Ok(value)) => value,
            Some(Err(_)) if closing => unavailable_event_context(now),
            Some(Err(_)) => {
                return Ok(rejected(
                    Some(raw_plan),
                    now,
                    ProtoReason::EventContextInvalid,
                ))
            }
            None if closing => unavailable_event_context(now),
            None => {
                return Ok(rejected(
                    Some(raw_plan),
                    now,
                    ProtoReason::EventContextUnavailable,
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
                allowed_strategies: broker.allowed_strategies,
                positions: broker.positions,
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

fn broker_legs(
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

fn recovery_pricing_time(raw: &ProtoPlan) -> Result<DateTime<Utc>, BrokerError> {
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

fn map_adaptive_error(error: AdaptivePriceError) -> BrokerError {
    match error {
        AdaptivePriceError::StaleQuote => BrokerError::QuoteStale,
        AdaptivePriceError::CrossedQuote => BrokerError::QuoteCrossed,
        AdaptivePriceError::SpreadTooWide => BrokerError::SpreadTooWide,
        AdaptivePriceError::InvalidQuote => BrokerError::QuoteUnavailable,
        AdaptivePriceError::InvalidPolicy => BrokerError::InvalidOrderType,
        AdaptivePriceError::InvalidProtectionPrice => BrokerError::InvalidPrice,
    }
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

fn map_order_type(value: i32) -> Result<BrokerOrderType, &'static str> {
    match ProtoOrderType::try_from(value).ok() {
        Some(ProtoOrderType::Market) => Ok(BrokerOrderType::Market),
        Some(ProtoOrderType::Limit) => Ok(BrokerOrderType::Limit),
        Some(ProtoOrderType::AdaptiveLimit) => Ok(BrokerOrderType::AdaptiveLimit),
        _ => Err("order_type"),
    }
}

fn map_position_effect(value: i32) -> Result<PositionEffect, &'static str> {
    match ProtoPositionEffect::try_from(value).ok() {
        Some(ProtoPositionEffect::Open) => Ok(PositionEffect::Open),
        Some(ProtoPositionEffect::Close) => Ok(PositionEffect::Close),
        _ => Err("position_effect"),
    }
}

fn adaptive_policy_valid(raw: &ProtoPlan, order_type: BrokerOrderType) -> bool {
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

fn plan(raw: &ProtoPlan) -> Result<CandidatePlan, &'static str> {
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

fn unavailable_event_context(now: DateTime<Utc>) -> EventRiskContext {
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

fn proto_child_state(state: AdapterOrderStatus) -> ProtoChildState {
    match state {
        AdapterOrderStatus::Working => ProtoChildState::Working,
        AdapterOrderStatus::PartialFill => ProtoChildState::PartialFill,
        AdapterOrderStatus::Filled => ProtoChildState::Filled,
        AdapterOrderStatus::Cancelled => ProtoChildState::Cancelled,
        AdapterOrderStatus::Rejected => ProtoChildState::Rejected,
        AdapterOrderStatus::ReconcilePending => ProtoChildState::ReconcilePending,
    }
}

fn proto_adapter_side(side: AdapterOrderSide) -> ProtoSide {
    match side {
        AdapterOrderSide::Buy => ProtoSide::Buy,
        AdapterOrderSide::Sell => ProtoSide::Sell,
    }
}

fn restored_order_state(state: i32) -> Result<(OrderState, bool), &'static str> {
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

fn restored_child_state(state: i32) -> Result<AdapterOrderStatus, &'static str> {
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

fn restore_entry(
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

fn order_proto(staged: &StagedOrder, now: DateTime<Utc>) -> ProtoOrder {
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
            self.evaluate_raw(raw.plan.as_ref(), raw.event_context.as_ref(), now, false)
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
            .evaluate_raw(raw.plan.as_ref(), raw.event_context.as_ref(), now, false)
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
            .evaluate_raw(Some(&stored_plan), raw.event_context.as_ref(), now, true)
            .await?;
        let external_request =
            {
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
                    return Ok(Response::new(order_proto(staged, now)));
                }
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
                    return Ok(Response::new(order_proto(staged, now)));
                }
                let priced = priced_broker_order(&staged.raw_plan, now);
                staged.record.begin_submit(now).map_err(|error| {
                    Status::failed_precondition(format!("submit blocked: {error:?}"))
                })?;
                let (order_side, order_type, submitted_price, adapter_legs) = match priced {
                    Ok(value) => value,
                    Err(_) => {
                        staged
                            .record
                            .submission_rejected(now)
                            .map_err(|_| Status::internal("pricing rejection transition failed"))?;
                        return Ok(Response::new(order_proto(staged, now)));
                    }
                };
                if self.execution_backend.is_simulated() {
                    let adapter = match ProtoBrokerId::try_from(staged.raw_plan.broker_id) {
                        Ok(ProtoBrokerId::Longbridge) => longbridge_paper,
                        Ok(ProtoBrokerId::Ibkr) => ibkr_paper,
                        _ => return Err(Status::internal("staged broker is invalid")),
                    };
                    if let Err(error) = submit_to_broker(
                        &mut staged.record,
                        adapter,
                        order_side,
                        order_type,
                        submitted_price,
                        adapter_legs,
                        now,
                    ) {
                        if matches!(
                            error,
                            execution::ExecutionError::Broker(BrokerError::Disconnected)
                                | execution::ExecutionError::Broker(BrokerError::NotReconciled)
                        ) {
                            staged.record.broker_disconnected(now).map_err(|_| {
                                Status::internal("broker disconnect transition failed")
                            })?;
                        } else {
                            staged.record.submission_rejected(now).map_err(|_| {
                                Status::internal("broker rejection transition failed")
                            })?;
                        }
                    }
                    return Ok(Response::new(order_proto(staged, now)));
                }
                if !self.execution_backend.is_external() {
                    staged
                        .record
                        .submission_rejected(now)
                        .map_err(|_| Status::internal("disabled route rejection failed"))?;
                    return Ok(Response::new(order_proto(staged, now)));
                }
                let broker_id = match ProtoBrokerId::try_from(staged.raw_plan.broker_id) {
                    Ok(ProtoBrokerId::Longbridge) => {
                        optiontrader_proto::broker_v1::BrokerId::Longbridge as i32
                    }
                    Ok(ProtoBrokerId::Ibkr) => optiontrader_proto::broker_v1::BrokerId::Ibkr as i32,
                    _ => return Err(Status::internal("staged broker is invalid")),
                };
                expected_request(
                    broker_id,
                    broker::BrokerOrderRequest {
                        idempotency_key: staged.raw_plan.idempotency_key.clone(),
                        plan_hash: staged.raw_plan.plan_hash.clone(),
                        side: order_side,
                        order_type,
                        total_quantity: staged.record.total_quantity,
                        submitted_price,
                        legs: adapter_legs,
                    },
                )
            };

        let submission = self.broker_mutations.submit_order(external_request).await;
        let route_requires_reconciliation = matches!(
            &submission,
            Err(BrokerMutationError::Disabled
                | BrokerMutationError::NotReady
                | BrokerMutationError::OutcomeUnknown)
        );
        let (response, must_reconcile) = {
            let mut workflow = workflow_lock(self)
                .map_err(|()| Status::internal("execution workflow lock poisoned"))?;
            let staged = workflow
                .orders
                .get_mut(&raw.order_id)
                .ok_or_else(|| Status::not_found("order disappeared after submission"))?;
            if staged.record.state != OrderState::Submitting {
                return Err(Status::failed_precondition(
                    "order changed while broker submission was in flight",
                ));
            }
            match submission {
                Ok(order) => {
                    if staged.record.apply_broker_order(&order, now).is_err() {
                        staged.record.broker_disconnected(now).map_err(|_| {
                            Status::internal("conflicting submit result could not reconcile")
                        })?;
                    }
                }
                Err(
                    BrokerMutationError::Disabled
                    | BrokerMutationError::NotReady
                    | BrokerMutationError::Rejected,
                ) => staged
                    .record
                    .submission_rejected(now)
                    .map_err(|_| Status::internal("broker rejection transition failed"))?,
                Err(BrokerMutationError::OutcomeUnknown) => staged
                    .record
                    .broker_disconnected(now)
                    .map_err(|_| Status::internal("unknown submit could not reconcile"))?,
            }
            (
                order_proto(staged, now),
                staged.record.state == OrderState::ReconcilePending
                    || staged.record.residual_exposure
                    || route_requires_reconciliation,
            )
        };
        if must_reconcile {
            let mut broker = self
                .broker
                .write()
                .map_err(|_| Status::internal("broker authority lock poisoned"))?;
            broker.health = BrokerHealth::Reconciling;
            broker.reconciled = false;
        }
        Ok(Response::new(response))
    }

    async fn cancel_order(
        &self,
        request: Request<CancelOrderRequest>,
    ) -> Result<Response<ProtoOrder>, Status> {
        let now = (self.clock)();
        let order_id = request.into_inner().order_id;
        let external_cancel = {
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
                OrderState::AwaitingConfirmation => {
                    staged
                        .record
                        .cancel_unsubmitted(now)
                        .map_err(|_| Status::internal("pre-submit cancel transition failed"))?;
                    return Ok(Response::new(order_proto(staged, now)));
                }
                OrderState::Working | OrderState::PartialFill => {
                    let mode = ProtoMode::try_from(staged.raw_plan.execution_mode)
                        .map_err(|_| Status::internal("staged execution mode is invalid"))?;
                    let route = ProtoBrokerId::try_from(staged.raw_plan.broker_id)
                        .map_err(|_| Status::internal("staged broker is invalid"))?;
                    if !self.execution_backend.allows(mode, route) {
                        return Err(Status::failed_precondition(
                            "broker execution route is not enabled",
                        ));
                    }
                    let Some(broker_order_id) = staged.record.broker_order_id.clone() else {
                        staged.record.broker_disconnected(now).map_err(|_| {
                            Status::internal("invalid working order could not enter reconciliation")
                        })?;
                        return Ok(Response::new(order_proto(staged, now)));
                    };
                    staged
                        .record
                        .request_cancel(now)
                        .map_err(|_| Status::failed_precondition("order cannot be cancelled"))?;
                    if self.execution_backend.is_simulated() {
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
                                }
                            }
                            Err(_) => staged.record.broker_disconnected(now).map_err(|_| {
                                Status::internal("cancel reconciliation transition failed")
                            })?,
                        }
                        return Ok(Response::new(order_proto(staged, now)));
                    }
                    debug_assert!(self.execution_backend.is_external());
                    let broker_id = match ProtoBrokerId::try_from(staged.raw_plan.broker_id) {
                        Ok(ProtoBrokerId::Longbridge) => {
                            optiontrader_proto::broker_v1::BrokerId::Longbridge as i32
                        }
                        Ok(ProtoBrokerId::Ibkr) => {
                            optiontrader_proto::broker_v1::BrokerId::Ibkr as i32
                        }
                        _ => return Err(Status::internal("staged broker is invalid")),
                    };
                    (broker_id, broker_order_id)
                }
                _ if staged.record.state.is_terminal() => {
                    return Ok(Response::new(order_proto(staged, now)));
                }
                _ => return Err(Status::failed_precondition("order cannot be cancelled")),
            }
        };

        let cancel_result = self
            .broker_mutations
            .cancel_order(external_cancel.0, external_cancel.1)
            .await;
        let response = {
            let mut workflow = workflow_lock(self)
                .map_err(|()| Status::internal("execution workflow lock poisoned"))?;
            let staged = workflow
                .orders
                .get_mut(&order_id)
                .ok_or_else(|| Status::not_found("order disappeared after cancel"))?;
            if staged.record.state != OrderState::CancelPending {
                return Err(Status::failed_precondition(
                    "order changed while broker cancel was in flight",
                ));
            }
            if let Ok(order) = cancel_result {
                if staged.record.apply_broker_order(&order, now).is_err() {
                    staged.record.broker_disconnected(now).map_err(|_| {
                        Status::internal("conflicting cancel result could not reconcile")
                    })?;
                }
            } else {
                staged
                    .record
                    .broker_disconnected(now)
                    .map_err(|_| Status::internal("unknown cancel could not reconcile"))?;
            }
            order_proto(staged, now)
        };
        let mut broker = self
            .broker
            .write()
            .map_err(|_| Status::internal("broker authority lock poisoned"))?;
        broker.health = BrokerHealth::Reconciling;
        broker.reconciled = false;
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

    async fn reconcile_execution_order(
        &self,
        request: Request<ReconcileExecutionOrderRequest>,
    ) -> Result<Response<ProtoOrder>, Status> {
        let now = (self.clock)();
        let order_id = request.into_inner().order_id;
        let (raw_plan, broker_order_id, total_quantity) =
            {
                let workflow = workflow_lock(self)
                    .map_err(|()| Status::internal("execution workflow lock poisoned"))?;
                let staged = workflow
                    .orders
                    .get(&order_id)
                    .ok_or_else(|| Status::not_found("order not found"))?;
                if staged.record.state != OrderState::ReconcilePending {
                    return Err(Status::failed_precondition(
                        "order does not require broker reconciliation",
                    ));
                }
                (
                    staged.raw_plan.clone(),
                    staged.record.broker_order_id.clone().ok_or_else(|| {
                        Status::failed_precondition("broker order id is unavailable")
                    })?,
                    staged.record.total_quantity,
                )
            };
        let pricing_time = recovery_pricing_time(&raw_plan)
            .map_err(|_| Status::failed_precondition("durable pricing proof is invalid"))?;
        let (side, order_type, submitted_price, legs) =
            priced_broker_order(&raw_plan, pricing_time)
                .map_err(|_| Status::failed_precondition("durable order proof is invalid"))?;
        let expected = expected_request(
            match ProtoBrokerId::try_from(raw_plan.broker_id).ok() {
                Some(ProtoBrokerId::Longbridge) => {
                    optiontrader_proto::broker_v1::BrokerId::Longbridge as i32
                }
                Some(ProtoBrokerId::Ibkr) => optiontrader_proto::broker_v1::BrokerId::Ibkr as i32,
                _ => {
                    return Err(Status::failed_precondition(
                        "durable broker route is invalid",
                    ))
                }
            },
            broker::BrokerOrderRequest {
                idempotency_key: raw_plan.idempotency_key.clone(),
                plan_hash: raw_plan.plan_hash.clone(),
                side,
                order_type,
                total_quantity,
                submitted_price,
                legs,
            },
        );
        let recovered = match self
            .broker_snapshots
            .recover(expected.clone(), broker_order_id.clone(), now)
            .await
        {
            Ok(value) => value,
            Err(error) => {
                let mut broker = self
                    .broker
                    .write()
                    .map_err(|_| Status::internal("broker authority lock poisoned"))?;
                broker.health = BrokerHealth::Reconciling;
                broker.reconciled = false;
                return Err(match error {
                    BrokerRecoveryError::Unavailable => {
                        Status::unavailable("broker recovery authority is unavailable")
                    }
                    BrokerRecoveryError::UnsupportedBroker => {
                        Status::failed_precondition("broker recovery route is not certified")
                    }
                    BrokerRecoveryError::InvalidSnapshot
                    | BrokerRecoveryError::NotReconciled
                    | BrokerRecoveryError::OrderConflict => {
                        Status::failed_precondition("broker recovery proof did not reconcile")
                    }
                });
            }
        };
        if self.execution_backend == BrokerExecutionBackend::LongbridgePaper {
            if let Err(error) = self
                .broker_mutations
                .bind_recovered_order_for_mutation(expected, broker_order_id)
                .await
            {
                let mut broker = self
                    .broker
                    .write()
                    .map_err(|_| Status::internal("broker authority lock poisoned"))?;
                broker.health = BrokerHealth::Reconciling;
                broker.reconciled = false;
                return Err(match error {
                    BrokerRecoveryError::Unavailable => {
                        Status::unavailable("Longbridge mutation identity rebinding is unavailable")
                    }
                    BrokerRecoveryError::UnsupportedBroker
                    | BrokerRecoveryError::InvalidSnapshot
                    | BrokerRecoveryError::NotReconciled
                    | BrokerRecoveryError::OrderConflict => Status::failed_precondition(
                        "Longbridge mutation identity did not reconcile",
                    ),
                });
            }
        }
        let (response, reconciliation_remains) = {
            let mut workflow = workflow_lock(self)
                .map_err(|()| Status::internal("execution workflow lock poisoned"))?;
            let staged = workflow
                .orders
                .get_mut(&order_id)
                .ok_or_else(|| Status::not_found("order disappeared during reconciliation"))?;
            if staged.record.state != OrderState::ReconcilePending {
                return Err(Status::failed_precondition(
                    "order changed during broker reconciliation",
                ));
            }
            staged
                .record
                .apply_broker_order(&recovered.order, now)
                .map_err(|_| Status::failed_precondition("broker order conflicts with workflow"))?;
            let response = order_proto(staged, now);
            let remains = workflow.orders.values().any(|entry| {
                entry.record.state == OrderState::ReconcilePending || entry.record.residual_exposure
            });
            (response, remains)
        };
        let account_reconciliation_pending = self
            .broker_reconciliations
            .lock()
            .await
            .values()
            .any(|entry| !entry.committed);
        let mut broker = self
            .broker
            .write()
            .map_err(|_| Status::internal("broker authority lock poisoned"))?;
        broker.buying_power = recovered.buying_power;
        broker.health = if reconciliation_remains || account_reconciliation_pending {
            BrokerHealth::Reconciling
        } else {
            BrokerHealth::Healthy
        };
        broker.reconciled = !reconciliation_remains && !account_reconciliation_pending;
        Ok(Response::new(response))
    }

    async fn begin_broker_reconciliation(
        &self,
        request: Request<BeginBrokerReconciliationRequest>,
    ) -> Result<Response<BrokerReconciliationBatch>, Status> {
        let now = (self.clock)();
        let broker_id = request.into_inner().broker_id;
        if !matches!(
            ProtoBrokerId::try_from(broker_id).ok(),
            Some(ProtoBrokerId::Ibkr | ProtoBrokerId::Longbridge)
        ) {
            return Err(Status::invalid_argument("broker route is invalid"));
        }
        if self
            .execution_backend
            .broker_route()
            .is_some_and(|route| route as i32 != broker_id)
        {
            return Err(Status::failed_precondition(
                "broker reconciliation route differs from execution backend",
            ));
        }
        let mut reconciliations = self.broker_reconciliations.lock().await;
        reconciliations.insert(
            broker_id,
            PendingBrokerReconciliation {
                snapshot_sequence: 0,
                snapshot_hash: String::new(),
                expires_at: now + chrono::Duration::seconds(15),
                buying_power: Decimal::ZERO,
                positions: BTreeMap::new(),
                committed: false,
            },
        );
        {
            let mut broker = self
                .broker
                .write()
                .map_err(|_| Status::internal("broker authority lock poisoned"))?;
            broker.health = BrokerHealth::Reconciling;
            broker.reconciled = false;
        }
        let ValidatedBrokerSnapshot {
            snapshot,
            snapshot_hash,
            buying_power,
        } = self
            .broker_snapshots
            .fetch_snapshot(broker_id, now)
            .await
            .map_err(|error| match error {
                BrokerRecoveryError::Unavailable => {
                    Status::unavailable("broker snapshot authority is unavailable")
                }
                BrokerRecoveryError::UnsupportedBroker => {
                    Status::failed_precondition("broker snapshot route is not certified")
                }
                BrokerRecoveryError::InvalidSnapshot
                | BrokerRecoveryError::NotReconciled
                | BrokerRecoveryError::OrderConflict => {
                    Status::failed_precondition("broker account snapshot did not reconcile")
                }
            })?;
        let positions = snapshot
            .positions
            .iter()
            .filter(|position| position.quantity != 0)
            .map(|position| (position.contract_id.clone(), position.quantity))
            .collect();
        let expires_at = now + chrono::Duration::seconds(15);
        reconciliations.insert(
            broker_id,
            PendingBrokerReconciliation {
                snapshot_sequence: snapshot.snapshot_sequence,
                snapshot_hash: snapshot_hash.clone(),
                expires_at,
                buying_power,
                positions,
                committed: false,
            },
        );
        Ok(Response::new(BrokerReconciliationBatch {
            schema_version: "1.0".into(),
            broker_id,
            snapshot_sequence: snapshot.snapshot_sequence,
            snapshot_hash,
            snapshot_protobuf: snapshot.encode_to_vec(),
            expires_at_utc: expires_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        }))
    }

    async fn commit_broker_reconciliation(
        &self,
        request: Request<CommitBrokerReconciliationRequest>,
    ) -> Result<Response<CommitBrokerReconciliationResponse>, Status> {
        let now = (self.clock)();
        let raw = request.into_inner();
        if raw.snapshot_hash.len() != 64
            || !raw
                .snapshot_hash
                .bytes()
                .all(|value| value.is_ascii_hexdigit())
            || raw.mismatch_codes.len() > 100
        {
            return Err(Status::invalid_argument(
                "broker reconciliation receipt is invalid",
            ));
        }
        let mut pending = self.broker_reconciliations.lock().await;
        let entry = pending
            .get_mut(&raw.broker_id)
            .ok_or_else(|| Status::failed_precondition("no broker reconciliation is pending"))?;
        if entry.snapshot_sequence != raw.snapshot_sequence
            || entry.snapshot_hash != raw.snapshot_hash
        {
            return Err(Status::failed_precondition(
                "broker reconciliation receipt does not match pending snapshot",
            ));
        }
        if entry.expires_at < now {
            return Err(Status::failed_precondition(
                "broker reconciliation receipt expired",
            ));
        }
        if !raw.persistence_succeeded || !raw.mismatch_codes.is_empty() {
            return Ok(Response::new(CommitBrokerReconciliationResponse {
                accepted: true,
                broker_reconciled: false,
                reason_codes: if raw.persistence_succeeded {
                    raw.mismatch_codes
                } else {
                    vec!["PERSISTENCE_FAILED".into()]
                },
            }));
        }
        let buying_power = entry.buying_power;
        let positions = entry.positions.clone();
        let already_committed = entry.committed;
        let workflow_pending = workflow_lock(self)
            .map_err(|()| Status::internal("execution workflow lock poisoned"))?
            .orders
            .values()
            .any(|entry| {
                entry.record.state == OrderState::ReconcilePending || entry.record.residual_exposure
            });
        if workflow_pending {
            return Ok(Response::new(CommitBrokerReconciliationResponse {
                accepted: true,
                broker_reconciled: false,
                reason_codes: vec!["WORKFLOW_RECONCILIATION_PENDING".into()],
            }));
        }
        let mut broker = self
            .broker
            .write()
            .map_err(|_| Status::internal("broker authority lock poisoned"))?;
        if !already_committed {
            entry.committed = true;
            broker.buying_power = buying_power;
            broker.positions = positions;
            broker.health = BrokerHealth::Healthy;
            broker.reconciled = true;
        } else if broker.health != BrokerHealth::Healthy || !broker.reconciled {
            return Ok(Response::new(CommitBrokerReconciliationResponse {
                accepted: true,
                broker_reconciled: false,
                reason_codes: vec!["BROKER_AUTHORITY_CHANGED".into()],
            }));
        }
        Ok(Response::new(CommitBrokerReconciliationResponse {
            accepted: true,
            broker_reconciled: true,
            reason_codes: Vec::new(),
        }))
    }

    async fn restore_workflow(
        &self,
        request: Request<RestoreWorkflowRequest>,
    ) -> Result<Response<RestoreWorkflowResponse>, Status> {
        let now = (self.clock)();
        let entries = request.into_inner().entries;
        if entries.len() > 10_000 {
            return Err(Status::invalid_argument("restore batch is too large"));
        }
        let mut restored = Vec::with_capacity(entries.len());
        let mut batch_order_ids = BTreeSet::new();
        let mut batch_keys = BTreeSet::new();
        for entry in entries {
            let (staged, reconciliation_required) =
                restore_entry(entry, now).map_err(|reason| {
                    Status::invalid_argument(format!("invalid restore entry: {reason}"))
                })?;
            if !batch_order_ids.insert(staged.record.order_id.clone())
                || !batch_keys.insert(staged.record.idempotency_key.clone())
            {
                return Err(Status::invalid_argument("duplicate restore identity"));
            }
            restored.push((staged, reconciliation_required));
        }

        let mut workflow = workflow_lock(self)
            .map_err(|()| Status::internal("execution workflow lock poisoned"))?;
        for (staged, _) in &restored {
            let existing_order = workflow.orders.get(&staged.record.order_id);
            let existing_key = workflow.order_by_key.get(&staged.record.idempotency_key);
            if existing_order.is_some() || existing_key.is_some() {
                let compatible = existing_order.is_some_and(|existing| {
                    existing.record.plan_hash == staged.record.plan_hash
                        && existing.record.idempotency_key == staged.record.idempotency_key
                        && (existing.record.state_version() >= staged.record.state_version()
                            || (existing.record.state == OrderState::ReconcilePending
                                && staged.record.state == OrderState::ReconcilePending
                                && existing.record.state_version().checked_add(1)
                                    == Some(staged.record.state_version())))
                }) && existing_key.is_some_and(|(hash, order_id)| {
                    hash == &staged.record.plan_hash && order_id == &staged.record.order_id
                });
                if !compatible {
                    return Err(Status::already_exists("workflow identity conflicts"));
                }
            }
        }
        let mut orders = Vec::with_capacity(restored.len());
        let mut reconciliation_order_ids = Vec::new();
        for (staged, reconciliation_required) in restored {
            let order_id = staged.record.order_id.clone();
            if let Some(existing) = workflow.orders.get(&order_id) {
                if existing.record.state == OrderState::ReconcilePending {
                    reconciliation_order_ids.push(order_id);
                }
                orders.push(order_proto(existing, now));
                continue;
            }
            workflow.order_by_key.insert(
                staged.record.idempotency_key.clone(),
                (staged.record.plan_hash.clone(), order_id.clone()),
            );
            if reconciliation_required {
                reconciliation_order_ids.push(order_id.clone());
            }
            orders.push(order_proto(&staged, now));
            workflow.orders.insert(order_id, staged);
        }
        let reconciliation_required = !reconciliation_order_ids.is_empty();
        drop(workflow);
        if reconciliation_required {
            let mut broker = self
                .broker
                .write()
                .map_err(|_| Status::internal("broker authority lock poisoned"))?;
            broker.health = BrokerHealth::Reconciling;
            broker.reconciled = false;
        }
        Ok(Response::new(RestoreWorkflowResponse {
            orders,
            reconciliation_order_ids,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use optiontrader_proto::execution_v1::{
        CandidateLeg as ProtoLeg, EventSourceProof as ProtoSource, OptionQuoteProof as ProtoQuote,
        OptionRight as ProtoRight, OrderSide as ProtoSide,
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
            allowed_strategies: BTreeSet::from([
                StrategyKind::LongGamma,
                StrategyKind::ShortPremium,
                StrategyKind::EventVolCrush,
            ]),
            positions: BTreeMap::new(),
            limits: RiskLimits {
                max_plan_loss: Decimal::new(1_000, 0),
                max_daily_loss: Decimal::new(1_000, 0),
                max_open_risk: Decimal::new(1_000, 0),
                max_daily_trades: 3,
                max_contracts: 2,
                max_quote_age_ms: 120_000,
                max_option_spread_bps: 3_000,
                entry_start_minute_et: 9 * 60 + 35,
                entry_cutoff_minute_et: 15 * 60 + 30,
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

    #[test]
    fn real_broker_execution_requires_every_paper_safety_gate() {
        assert_eq!(
            BrokerExecutionBackend::from_config(
                "simulated-paper",
                "local",
                false,
                false,
                true,
                false,
                false,
            ),
            Ok(BrokerExecutionBackend::SimulatedPaper)
        );
        assert!(BrokerExecutionBackend::from_config(
            "ibkr-paper",
            "paper",
            false,
            false,
            true,
            true,
            false,
        )
        .is_err());
        assert_eq!(
            BrokerExecutionBackend::from_config(
                "ibkr-paper",
                "paper",
                false,
                true,
                true,
                true,
                false,
            ),
            Ok(BrokerExecutionBackend::IbkrPaper)
        );
        assert_eq!(
            BrokerExecutionBackend::from_config(
                "longbridge-paper",
                "paper",
                false,
                true,
                true,
                false,
                true,
            ),
            Ok(BrokerExecutionBackend::LongbridgePaper)
        );
        assert!(BrokerExecutionBackend::from_config(
            "simulated-paper",
            "paper",
            true,
            true,
            true,
            true,
            true,
        )
        .is_err());
        assert!(BrokerExecutionBackend::IbkrPaper.allows(ProtoMode::Paper, ProtoBrokerId::Ibkr));
        assert!(
            !BrokerExecutionBackend::IbkrPaper.allows(ProtoMode::Paper, ProtoBrokerId::Longbridge)
        );
        assert!(!BrokerExecutionBackend::LongbridgePaper
            .allows(ProtoMode::ControlledAuto, ProtoBrokerId::Longbridge));
        assert_eq!(
            BrokerExecutionBackend::IbkrPaper.require_reconciliation_route(true, "ibkr"),
            Ok(BrokerExecutionBackend::IbkrPaper)
        );
        assert!(BrokerExecutionBackend::IbkrPaper
            .require_reconciliation_route(false, "ibkr")
            .is_err());
        assert!(BrokerExecutionBackend::LongbridgePaper
            .require_reconciliation_route(true, "ibkr")
            .is_err());
        assert_eq!(
            BrokerExecutionBackend::SimulatedPaper
                .require_reconciliation_route(false, "ibkr,longbridge"),
            Ok(BrokerExecutionBackend::SimulatedPaper)
        );
    }

    #[tokio::test]
    async fn external_execution_rejects_other_broker_reconciliation() {
        let mut service = service();
        service.execution_backend = BrokerExecutionBackend::IbkrPaper;
        let error = service
            .begin_broker_reconciliation(Request::new(BeginBrokerReconciliationRequest {
                broker_id: ProtoBrokerId::Longbridge as i32,
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn longbridge_credit_spread_gets_independent_thetadata_leg_prices() {
        let mut raw = candidate(ProtoMode::Paper);
        raw.broker_id = ProtoBrokerId::Longbridge as i32;
        raw.strategy = ProtoStrategy::ShortPremium as i32;
        raw.order_side = ProtoSide::Sell as i32;
        raw.limit_price = "1.00".into();
        let mut hedge = raw.legs[0].clone();
        hedge.contract_id = "QQQ-20260720-C-505".into();
        hedge.broker_contract_id = "QQQ260720C00505000.US".into();
        hedge.strike = "505".into();
        hedge.quote.as_mut().unwrap().bid = "0.45".into();
        hedge.quote.as_mut().unwrap().ask = "0.50".into();
        let mut short = raw.legs[0].clone();
        short.side = ProtoSide::Sell as i32;
        short.contract_id = "QQQ-20260720-C-500".into();
        short.broker_contract_id = "QQQ260720C00500000.US".into();
        short.quote.as_mut().unwrap().bid = "1.50".into();
        short.quote.as_mut().unwrap().ask = "1.55".into();
        raw.legs = vec![short, hedge];

        let (side, order_type, parent_price, legs) = priced_broker_order(&raw, now()).unwrap();
        assert_eq!(side, AdapterOrderSide::Sell);
        assert_eq!(order_type, AdapterOrderType::Limit);
        assert_eq!(parent_price, Some(Decimal::new(100, 2)));
        assert_eq!(legs[0].submitted_price, Some(Decimal::new(150, 2)));
        assert_eq!(legs[1].submitted_price, Some(Decimal::new(50, 2)));
    }

    fn candidate(mode: ProtoMode) -> ProtoPlan {
        let mut value = ProtoPlan {
            schema_version: "1.3".into(),
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
                quote: Some(ProtoQuote {
                    bid: "2.40".into(),
                    ask: "2.50".into(),
                    bid_size: 20,
                    ask_size: 25,
                    occurred_at_utc: "2026-07-20T13:45:00Z".into(),
                    delta: "0.52".into(),
                    gamma: "0.08".into(),
                    theta: "-0.12".into(),
                    vega: "0.05".into(),
                    chain_snapshot_id: "opt-1".into(),
                    provider: "THETADATA".into(),
                }),
                broker_contract_id: "123456".into(),
                symbol: "QQQ".into(),
                exchange: "SMART".into(),
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
            data_snapshot_ids: vec![MarketSnapshot::fixture().snapshot_id, "opt-1".into()],
            manual_confirmation_required: true,
            order_side: ProtoSide::Buy as i32,
            order_type: ProtoOrderType::Limit as i32,
            adaptive_limit: None,
            market_data_provider: "THETADATA".into(),
            position_effect: ProtoPositionEffect::Open as i32,
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

    fn closing_candidate(order_type: ProtoOrderType) -> ProtoPlan {
        let mut value = candidate(ProtoMode::Paper);
        value.position_effect = ProtoPositionEffect::Close as i32;
        value.legs[0].side = ProtoSide::Sell as i32;
        value.order_side = ProtoSide::Sell as i32;
        value.order_type = order_type as i32;
        value.max_loss = "0".into();
        value.take_profit.clear();
        value.stop_loss.clear();
        value.time_stop_minutes = 0;
        value.invalidation_rules = vec!["position_or_market_context_changes".into()];
        value.plan_id.clear();
        value.plan_hash.clear();
        value.idempotency_key.clear();
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

    fn longbridge_candidate() -> ProtoPlan {
        let mut value = candidate(ProtoMode::Paper);
        value.broker_id = ProtoBrokerId::Longbridge as i32;
        value.legs[0].broker_contract_id = "QQQ260720C00500000.US".into();
        value.plan_id.clear();
        value.plan_hash.clear();
        value.idempotency_key.clear();
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

    #[test]
    fn proto_parser_rejects_non_thetadata_leg_before_domain_risk() {
        let mut raw = candidate(ProtoMode::Paper);
        raw.legs[0].quote.as_mut().unwrap().provider = "BROKER".into();
        assert!(matches!(super::plan(&raw), Err("option quote provider")));
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
    async fn proven_market_close_stages_and_confirms_without_event_context() {
        let service = service();
        {
            let mut authority = service.broker.write().unwrap();
            authority.positions.insert("123456".into(), 2);
            authority.risk_limits_confirmed = false;
            authority.kill_switch_active = true;
            authority.buying_power = Decimal::ZERO;
            authority.allowed_strategies.clear();
        }
        let plan = closing_candidate(ProtoOrderType::Market);
        let staged = service
            .stage_candidate(Request::new(EvaluateCandidateRequest {
                plan: Some(plan),
                event_context: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            staged.initial_risk_decision.as_ref().unwrap().decision,
            RiskDecisionKind::Approved as i32
        );
        let awaiting = staged.order.unwrap();
        let confirmed = service
            .confirm_candidate(Request::new(ConfirmCandidateRequest {
                order_id: awaiting.order_id,
                confirmed_plan_hash: awaiting.plan_hash,
                confirmation_token: staged.confirmation_token,
                event_context: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            ProtoOrderState::try_from(confirmed.state).unwrap(),
            ProtoOrderState::Working
        );
    }

    #[tokio::test]
    async fn close_without_matching_broker_position_is_rejected() {
        let service = service();
        let decision = service
            .evaluate_candidate(Request::new(EvaluateCandidateRequest {
                plan: Some(closing_candidate(ProtoOrderType::Limit)),
                event_context: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(decision.decision, RiskDecisionKind::Rejected as i32);
        assert_eq!(
            decision.reason_codes,
            vec![ProtoReason::PositionNotReducible as i32]
        );
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
    async fn external_submit_unknown_outcome_is_never_treated_as_rejection() {
        let mut service = service();
        service.execution_backend = BrokerExecutionBackend::IbkrPaper;
        service.broker_mutations = BrokerSnapshotAuthority::MutationFixed {
            bind: Ok(()),
            submit: Err(BrokerMutationError::OutcomeUnknown),
            cancel: Err(BrokerMutationError::OutcomeUnknown),
        };
        let staged = stage(&service, ProtoMode::Paper).await;
        let order = staged.order.unwrap();
        let uncertain = service
            .confirm_candidate(Request::new(ConfirmCandidateRequest {
                order_id: order.order_id,
                confirmed_plan_hash: order.plan_hash,
                confirmation_token: staged.confirmation_token,
                event_context: Some(context()),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            ProtoOrderState::try_from(uncertain.state).unwrap(),
            ProtoOrderState::ReconcilePending
        );
        assert!(uncertain.residual_exposure);
        let broker = service.broker.read().unwrap();
        assert_eq!(broker.health, BrokerHealth::Reconciling);
        assert!(!broker.reconciled);
    }

    #[tokio::test]
    async fn external_submit_not_ready_rejects_order_but_closes_authority() {
        let mut service = service();
        service.execution_backend = BrokerExecutionBackend::IbkrPaper;
        service.broker_mutations = BrokerSnapshotAuthority::MutationFixed {
            bind: Ok(()),
            submit: Err(BrokerMutationError::NotReady),
            cancel: Err(BrokerMutationError::OutcomeUnknown),
        };
        let staged = stage(&service, ProtoMode::Paper).await;
        let order = staged.order.unwrap();
        let rejected = service
            .confirm_candidate(Request::new(ConfirmCandidateRequest {
                order_id: order.order_id,
                confirmed_plan_hash: order.plan_hash,
                confirmation_token: staged.confirmation_token,
                event_context: Some(context()),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            ProtoOrderState::try_from(rejected.state).unwrap(),
            ProtoOrderState::Rejected
        );
        assert!(!rejected.residual_exposure);
        let broker = service.broker.read().unwrap();
        assert_eq!(broker.health, BrokerHealth::Reconciling);
        assert!(!broker.reconciled);
    }

    #[tokio::test]
    async fn external_cancel_unknown_outcome_closes_authority() {
        let mut service = service();
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
        service.execution_backend = BrokerExecutionBackend::IbkrPaper;
        service.broker_mutations = BrokerSnapshotAuthority::MutationFixed {
            bind: Ok(()),
            submit: Err(BrokerMutationError::OutcomeUnknown),
            cancel: Err(BrokerMutationError::OutcomeUnknown),
        };
        let uncertain = service
            .cancel_order(Request::new(CancelOrderRequest {
                order_id: order.order_id,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            ProtoOrderState::try_from(uncertain.state).unwrap(),
            ProtoOrderState::ReconcilePending
        );
        let broker = service.broker.read().unwrap();
        assert_eq!(broker.health, BrokerHealth::Reconciling);
        assert!(!broker.reconciled);
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

        let response = service
            .cancel_order(Request::new(CancelOrderRequest {
                order_id: order.order_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            ProtoOrderState::try_from(response.state).unwrap(),
            ProtoOrderState::ReconcilePending
        );
        assert!(response.residual_exposure);

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

    #[tokio::test]
    async fn restart_restores_unclaimed_confirmation_capability_without_restage() {
        let original = service();
        let staged = stage(&original, ProtoMode::Shadow).await;
        let durable = staged.order.unwrap();
        let restarted = service();
        let response = restarted
            .restore_workflow(Request::new(RestoreWorkflowRequest {
                entries: vec![RestorableExecutionOrder {
                    plan: Some(candidate(ProtoMode::Shadow)),
                    order: Some(durable.clone()),
                    confirmation_token: staged.confirmation_token.clone(),
                }],
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.reconciliation_order_ids.is_empty());
        assert_eq!(response.orders[0].state_version, durable.state_version);

        let confirmed = restarted
            .confirm_candidate(Request::new(ConfirmCandidateRequest {
                order_id: durable.order_id,
                confirmed_plan_hash: durable.plan_hash,
                confirmation_token: staged.confirmation_token,
                event_context: Some(context()),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            ProtoOrderState::try_from(confirmed.state).unwrap(),
            ProtoOrderState::Shadowed
        );
    }

    #[tokio::test]
    async fn restart_forces_submitted_or_claimed_order_into_reconciliation() {
        let original = service();
        let staged = stage(&original, ProtoMode::Paper).await;
        let awaiting = staged.order.unwrap();
        let working = original
            .confirm_candidate(Request::new(ConfirmCandidateRequest {
                order_id: awaiting.order_id,
                confirmed_plan_hash: awaiting.plan_hash,
                confirmation_token: staged.confirmation_token,
                event_context: Some(context()),
            }))
            .await
            .unwrap()
            .into_inner();
        let restarted = service();
        let response = restarted
            .restore_workflow(Request::new(RestoreWorkflowRequest {
                entries: vec![RestorableExecutionOrder {
                    plan: Some(candidate(ProtoMode::Paper)),
                    order: Some(working.clone()),
                    confirmation_token: String::new(),
                }],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.reconciliation_order_ids, vec![working.order_id]);
        assert_eq!(
            ProtoOrderState::try_from(response.orders[0].state).unwrap(),
            ProtoOrderState::ReconcilePending
        );
        assert!(response.orders[0].residual_exposure);
        assert_eq!(response.orders[0].state_version, working.state_version + 1);
        {
            let authority = restarted.broker.read().unwrap();
            assert_eq!(authority.health, BrokerHealth::Reconciling);
            assert!(!authority.reconciled);
        }

        let repeated = restarted
            .restore_workflow(Request::new(RestoreWorkflowRequest {
                entries: vec![RestorableExecutionOrder {
                    plan: Some(candidate(ProtoMode::Paper)),
                    order: Some(response.orders[0].clone()),
                    confirmation_token: String::new(),
                }],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            repeated.orders[0].state_version,
            response.orders[0].state_version
        );
    }

    #[tokio::test]
    async fn broker_proof_resolves_restart_reconciliation_and_reopens_authority() {
        let original = service();
        let staged = stage(&original, ProtoMode::Paper).await;
        let awaiting = staged.order.unwrap();
        let working = original
            .confirm_candidate(Request::new(ConfirmCandidateRequest {
                order_id: awaiting.order_id,
                confirmed_plan_hash: awaiting.plan_hash,
                confirmation_token: staged.confirmation_token,
                event_context: Some(context()),
            }))
            .await
            .unwrap()
            .into_inner();
        let raw_plan = candidate(ProtoMode::Paper);
        let (side, order_type, submitted_price, legs) =
            priced_broker_order(&raw_plan, now()).unwrap();
        let mut restarted = service();
        restarted.broker_snapshots =
            BrokerSnapshotAuthority::Fixed(Ok(crate::broker_registry::RecoveredBrokerOrder {
                order: broker::BrokerOrder {
                    broker_order_id: working.broker_order_id.clone(),
                    idempotency_key: raw_plan.idempotency_key.clone(),
                    plan_hash: raw_plan.plan_hash.clone(),
                    status: AdapterOrderStatus::Working,
                    side,
                    order_type,
                    total_quantity: working.total_quantity,
                    filled_quantity: 0,
                    submitted_price,
                    legs,
                    child_orders: Vec::new(),
                    residual_exposure: false,
                },
                buying_power: Decimal::new(75_000, 0),
            }));
        restarted
            .restore_workflow(Request::new(RestoreWorkflowRequest {
                entries: vec![RestorableExecutionOrder {
                    plan: Some(raw_plan),
                    order: Some(working.clone()),
                    confirmation_token: String::new(),
                }],
            }))
            .await
            .unwrap();
        restarted.broker_reconciliations.lock().await.insert(
            ProtoBrokerId::Ibkr as i32,
            PendingBrokerReconciliation {
                snapshot_sequence: 91,
                snapshot_hash: "c".repeat(64),
                expires_at: now() + chrono::Duration::seconds(15),
                buying_power: Decimal::new(80_000, 0),
                positions: BTreeMap::new(),
                committed: false,
            },
        );
        let reconciled = restarted
            .reconcile_execution_order(Request::new(ReconcileExecutionOrderRequest {
                order_id: working.order_id,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            ProtoOrderState::try_from(reconciled.state).unwrap(),
            ProtoOrderState::Working
        );
        assert!(!reconciled.residual_exposure);
        {
            let authority = restarted.broker.read().unwrap();
            assert_eq!(authority.health, BrokerHealth::Reconciling);
            assert!(!authority.reconciled);
        }
        let account_commit = restarted
            .commit_broker_reconciliation(Request::new(CommitBrokerReconciliationRequest {
                broker_id: ProtoBrokerId::Ibkr as i32,
                snapshot_sequence: 91,
                snapshot_hash: "c".repeat(64),
                persistence_succeeded: true,
                mismatch_codes: Vec::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(account_commit.broker_reconciled);
        let authority = restarted.broker.read().unwrap();
        assert_eq!(authority.health, BrokerHealth::Healthy);
        assert!(authority.reconciled);
        assert_eq!(authority.buying_power, Decimal::new(80_000, 0));
    }

    #[tokio::test]
    async fn longbridge_restart_rebinds_mutation_identity_before_cancel() {
        let raw_plan = longbridge_candidate();
        let original = service();
        let staged = original
            .stage_candidate(Request::new(EvaluateCandidateRequest {
                plan: Some(raw_plan.clone()),
                event_context: Some(context()),
            }))
            .await
            .unwrap()
            .into_inner();
        let awaiting = staged.order.unwrap();
        let working = original
            .confirm_candidate(Request::new(ConfirmCandidateRequest {
                order_id: awaiting.order_id,
                confirmed_plan_hash: awaiting.plan_hash,
                confirmation_token: staged.confirmation_token,
                event_context: Some(context()),
            }))
            .await
            .unwrap()
            .into_inner();
        let (side, order_type, submitted_price, legs) =
            priced_broker_order(&raw_plan, now()).unwrap();
        let recovered_order = broker::BrokerOrder {
            broker_order_id: working.broker_order_id.clone(),
            idempotency_key: raw_plan.idempotency_key.clone(),
            plan_hash: raw_plan.plan_hash.clone(),
            status: AdapterOrderStatus::Working,
            side,
            order_type,
            total_quantity: working.total_quantity,
            filled_quantity: 0,
            submitted_price,
            legs,
            child_orders: Vec::new(),
            residual_exposure: false,
        };
        let mut restarted = service();
        restarted.execution_backend = BrokerExecutionBackend::LongbridgePaper;
        restarted.broker_snapshots =
            BrokerSnapshotAuthority::Fixed(Ok(crate::broker_registry::RecoveredBrokerOrder {
                order: recovered_order.clone(),
                buying_power: Decimal::new(75_000, 0),
            }));
        restarted.broker_mutations = BrokerSnapshotAuthority::MutationFixed {
            bind: Err(BrokerRecoveryError::OrderConflict),
            submit: Err(BrokerMutationError::OutcomeUnknown),
            cancel: Err(BrokerMutationError::OutcomeUnknown),
        };
        restarted
            .restore_workflow(Request::new(RestoreWorkflowRequest {
                entries: vec![RestorableExecutionOrder {
                    plan: Some(raw_plan),
                    order: Some(working.clone()),
                    confirmation_token: String::new(),
                }],
            }))
            .await
            .unwrap();

        let blocked = restarted
            .reconcile_execution_order(Request::new(ReconcileExecutionOrderRequest {
                order_id: working.order_id.clone(),
            }))
            .await
            .unwrap_err();
        assert_eq!(blocked.code(), tonic::Code::FailedPrecondition);
        let still_pending = restarted
            .get_order(Request::new(GetOrderRequest {
                order_id: working.order_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            ProtoOrderState::try_from(still_pending.state).unwrap(),
            ProtoOrderState::ReconcilePending
        );

        let mut cancelled_order = recovered_order;
        cancelled_order.status = AdapterOrderStatus::Cancelled;
        restarted.broker_mutations = BrokerSnapshotAuthority::MutationFixed {
            bind: Ok(()),
            submit: Err(BrokerMutationError::OutcomeUnknown),
            cancel: Ok(cancelled_order),
        };
        let reconciled = restarted
            .reconcile_execution_order(Request::new(ReconcileExecutionOrderRequest {
                order_id: working.order_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            ProtoOrderState::try_from(reconciled.state).unwrap(),
            ProtoOrderState::Working
        );
        let cancelled = restarted
            .cancel_order(Request::new(CancelOrderRequest {
                order_id: working.order_id,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            ProtoOrderState::try_from(cancelled.state).unwrap(),
            ProtoOrderState::Cancelled
        );
    }

    #[tokio::test]
    async fn broker_fact_commit_requires_same_hash_and_durable_success() {
        let mut service = service();
        let snapshot = optiontrader_proto::broker_v1::BrokerSnapshot {
            schema_version: "1.0".into(),
            snapshot_sequence: 44,
            account: Some(optiontrader_proto::broker_v1::AccountSnapshot {
                broker_id: optiontrader_proto::broker_v1::BrokerId::Ibkr as i32,
                occurred_at_utc: now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                health: optiontrader_proto::broker_v1::BrokerHealth::Healthy as i32,
                reconciled: true,
                buying_power: "12345".into(),
                net_liquidation: "25000".into(),
                currency: "USD".into(),
            }),
            positions: vec![optiontrader_proto::broker_v1::PositionSnapshot {
                contract_id: "123456".into(),
                quantity: 3,
                average_price: "2.25".into(),
            }],
            orders: Vec::new(),
            fills: Vec::new(),
        };
        let hash = format!("{:x}", Sha256::digest(snapshot.encode_to_vec()));
        service.broker_snapshots =
            BrokerSnapshotAuthority::FullFixed(Ok(ValidatedBrokerSnapshot {
                snapshot: snapshot.clone(),
                snapshot_hash: hash.clone(),
                buying_power: Decimal::new(12_345, 0),
            }));

        let batch = service
            .begin_broker_reconciliation(Request::new(BeginBrokerReconciliationRequest {
                broker_id: ProtoBrokerId::Ibkr as i32,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(batch.snapshot_protobuf, snapshot.encode_to_vec());
        assert!(!service.broker_handle().read().unwrap().reconciled);

        let wrong = service
            .commit_broker_reconciliation(Request::new(CommitBrokerReconciliationRequest {
                broker_id: ProtoBrokerId::Ibkr as i32,
                snapshot_sequence: batch.snapshot_sequence,
                snapshot_hash: "f".repeat(64),
                persistence_succeeded: true,
                mismatch_codes: Vec::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(wrong.code(), tonic::Code::FailedPrecondition);
        assert!(!service.broker_handle().read().unwrap().reconciled);

        let committed = service
            .commit_broker_reconciliation(Request::new(CommitBrokerReconciliationRequest {
                broker_id: ProtoBrokerId::Ibkr as i32,
                snapshot_sequence: batch.snapshot_sequence,
                snapshot_hash: hash,
                persistence_succeeded: true,
                mismatch_codes: Vec::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(committed.broker_reconciled);
        let broker_handle = service.broker_handle();
        let authority = broker_handle.read().unwrap().clone();
        assert!(authority.reconciled);
        assert_eq!(authority.buying_power, Decimal::new(12_345, 0));
        assert_eq!(authority.positions.get("123456"), Some(&3));

        let second = service
            .begin_broker_reconciliation(Request::new(BeginBrokerReconciliationRequest {
                broker_id: ProtoBrokerId::Ibkr as i32,
            }))
            .await
            .unwrap()
            .into_inner();
        let failed = service
            .commit_broker_reconciliation(Request::new(CommitBrokerReconciliationRequest {
                broker_id: ProtoBrokerId::Ibkr as i32,
                snapshot_sequence: second.snapshot_sequence,
                snapshot_hash: second.snapshot_hash,
                persistence_succeeded: false,
                mismatch_codes: Vec::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!failed.broker_reconciled);
        assert!(!service.broker_handle().read().unwrap().reconciled);
        assert!(service
            .broker_reconciliations
            .lock()
            .await
            .values()
            .any(|entry| !entry.committed));
    }

    #[tokio::test]
    async fn residual_leg_exposure_keeps_account_authority_closed() {
        let service = service();
        let staged = stage(&service, ProtoMode::Paper).await;
        let awaiting = staged.order.unwrap();
        let working = service
            .confirm_candidate(Request::new(ConfirmCandidateRequest {
                order_id: awaiting.order_id,
                confirmed_plan_hash: awaiting.plan_hash,
                confirmation_token: staged.confirmation_token,
                event_context: Some(context()),
            }))
            .await
            .unwrap()
            .into_inner();
        workflow_lock(&service)
            .unwrap()
            .orders
            .get_mut(&working.order_id)
            .unwrap()
            .record
            .residual_exposure = true;
        service.broker_reconciliations.lock().await.insert(
            ProtoBrokerId::Ibkr as i32,
            PendingBrokerReconciliation {
                snapshot_sequence: 92,
                snapshot_hash: "d".repeat(64),
                expires_at: now() + chrono::Duration::seconds(15),
                buying_power: Decimal::new(80_000, 0),
                positions: BTreeMap::new(),
                committed: false,
            },
        );
        {
            let mut broker = service.broker.write().unwrap();
            broker.health = BrokerHealth::Reconciling;
            broker.reconciled = false;
        }

        let receipt = service
            .commit_broker_reconciliation(Request::new(CommitBrokerReconciliationRequest {
                broker_id: ProtoBrokerId::Ibkr as i32,
                snapshot_sequence: 92,
                snapshot_hash: "d".repeat(64),
                persistence_succeeded: true,
                mismatch_codes: Vec::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!receipt.broker_reconciled);
        assert_eq!(
            receipt.reason_codes,
            vec!["WORKFLOW_RECONCILIATION_PENDING"]
        );
        assert!(!service.broker.read().unwrap().reconciled);
    }

    #[tokio::test]
    async fn unavailable_full_snapshot_leaves_sticky_account_reconciliation() {
        let service = service();
        let error = service
            .begin_broker_reconciliation(Request::new(BeginBrokerReconciliationRequest {
                broker_id: ProtoBrokerId::Ibkr as i32,
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(!service.broker.read().unwrap().reconciled);
        assert!(service
            .broker_reconciliations
            .lock()
            .await
            .values()
            .any(|entry| !entry.committed));
    }

    #[tokio::test]
    async fn paper_fault_drill_preserves_partial_fill_across_disconnect_and_restart() {
        let original = service();
        let staged = stage(&original, ProtoMode::Paper).await;
        let awaiting = staged.order.unwrap();
        original
            .confirm_candidate(Request::new(ConfirmCandidateRequest {
                order_id: awaiting.order_id.clone(),
                confirmed_plan_hash: awaiting.plan_hash,
                confirmation_token: staged.confirmation_token,
                event_context: Some(context()),
            }))
            .await
            .unwrap();
        let disconnected = {
            let mut workflow = workflow_lock(&original).unwrap();
            let Workflow {
                orders, ibkr_paper, ..
            } = &mut *workflow;
            let record = &mut orders.get_mut(&awaiting.order_id).unwrap().record;
            let broker_order_id = record.broker_order_id.clone().unwrap();
            let partial = ibkr_paper
                .apply_fill(&broker_order_id, 1, Decimal::new(245, 2))
                .unwrap();
            record.apply_broker_order(&partial, now()).unwrap();
            record.broker_disconnected(now()).unwrap();
            order_proto(orders.get(&awaiting.order_id).unwrap(), now())
        };
        assert_eq!(disconnected.filled_quantity, 1);
        assert!(disconnected.residual_exposure);
        assert_eq!(
            ProtoOrderState::try_from(disconnected.state).unwrap(),
            ProtoOrderState::ReconcilePending
        );

        let restarted = service();
        let restored = restarted
            .restore_workflow(Request::new(RestoreWorkflowRequest {
                entries: vec![RestorableExecutionOrder {
                    plan: Some(candidate(ProtoMode::Paper)),
                    order: Some(disconnected.clone()),
                    confirmation_token: String::new(),
                }],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(restored.orders[0].filled_quantity, 1);
        assert!(restored.orders[0].residual_exposure);
        assert_eq!(
            restored.orders[0].state_version,
            disconnected.state_version + 1
        );
    }

    #[tokio::test]
    async fn paper_broker_rejection_is_terminal_and_restart_does_not_resubmit() {
        let original = service();
        let staged = stage(&original, ProtoMode::Paper).await;
        let awaiting = staged.order.unwrap();
        original
            .confirm_candidate(Request::new(ConfirmCandidateRequest {
                order_id: awaiting.order_id.clone(),
                confirmed_plan_hash: awaiting.plan_hash,
                confirmation_token: staged.confirmation_token,
                event_context: Some(context()),
            }))
            .await
            .unwrap();
        let rejected = {
            let mut workflow = workflow_lock(&original).unwrap();
            let Workflow {
                orders, ibkr_paper, ..
            } = &mut *workflow;
            let record = &mut orders.get_mut(&awaiting.order_id).unwrap().record;
            let broker_order_id = record.broker_order_id.clone().unwrap();
            let broker_rejected = ibkr_paper.reject(&broker_order_id).unwrap();
            record.apply_broker_order(&broker_rejected, now()).unwrap();
            order_proto(orders.get(&awaiting.order_id).unwrap(), now())
        };
        let restarted = service();
        let response = restarted
            .restore_workflow(Request::new(RestoreWorkflowRequest {
                entries: vec![RestorableExecutionOrder {
                    plan: Some(candidate(ProtoMode::Paper)),
                    order: Some(rejected.clone()),
                    confirmation_token: String::new(),
                }],
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.reconciliation_order_ids.is_empty());
        assert_eq!(response.orders[0].state, rejected.state);
        assert_eq!(response.orders[0].state_version, rejected.state_version);
    }
}
