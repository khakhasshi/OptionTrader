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

mod backend;
mod candidate;
mod confirmation;
mod mapping;
mod orders;
mod reconciliation;

use backend::*;
use mapping::*;

#[tonic::async_trait]
impl RiskExecutionService for RiskExecutionServiceImpl {
    async fn evaluate_candidate(
        &self,
        request: Request<EvaluateCandidateRequest>,
    ) -> Result<Response<ProtoDecision>, Status> {
        self.evaluate_candidate_rpc(request).await
    }

    async fn stage_candidate(
        &self,
        request: Request<EvaluateCandidateRequest>,
    ) -> Result<Response<StageCandidateResponse>, Status> {
        self.stage_candidate_rpc(request).await
    }

    async fn confirm_candidate(
        &self,
        request: Request<ConfirmCandidateRequest>,
    ) -> Result<Response<ProtoOrder>, Status> {
        self.confirm_candidate_rpc(request).await
    }

    async fn cancel_order(
        &self,
        request: Request<CancelOrderRequest>,
    ) -> Result<Response<ProtoOrder>, Status> {
        self.cancel_order_rpc(request).await
    }

    async fn get_order(
        &self,
        request: Request<GetOrderRequest>,
    ) -> Result<Response<ProtoOrder>, Status> {
        self.get_order_rpc(request).await
    }

    async fn reconcile_execution_order(
        &self,
        request: Request<ReconcileExecutionOrderRequest>,
    ) -> Result<Response<ProtoOrder>, Status> {
        self.reconcile_execution_order_rpc(request).await
    }

    async fn begin_broker_reconciliation(
        &self,
        request: Request<BeginBrokerReconciliationRequest>,
    ) -> Result<Response<BrokerReconciliationBatch>, Status> {
        self.begin_broker_reconciliation_rpc(request).await
    }

    async fn commit_broker_reconciliation(
        &self,
        request: Request<CommitBrokerReconciliationRequest>,
    ) -> Result<Response<CommitBrokerReconciliationResponse>, Status> {
        self.commit_broker_reconciliation_rpc(request).await
    }

    async fn restore_workflow(
        &self,
        request: Request<RestoreWorkflowRequest>,
    ) -> Result<Response<RestoreWorkflowResponse>, Status> {
        self.restore_workflow_rpc(request).await
    }
}

#[cfg(test)]
mod tests;
