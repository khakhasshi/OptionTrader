//! Behavioral regression tests for the risk execution boundary.

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
        BrokerExecutionBackend::from_config("ibkr-paper", "paper", false, true, true, true, false,),
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
    assert!(!BrokerExecutionBackend::IbkrPaper.allows(ProtoMode::Paper, ProtoBrokerId::Longbridge));
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
    let (side, order_type, submitted_price, legs) = priced_broker_order(&raw_plan, now()).unwrap();
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
    let (side, order_type, submitted_price, legs) = priced_broker_order(&raw_plan, now()).unwrap();
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
    service.broker_snapshots = BrokerSnapshotAuthority::FullFixed(Ok(ValidatedBrokerSnapshot {
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
