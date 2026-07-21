//! Direct, read-only broker recovery authority used after process restart.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broker::{
    longbridge::LongbridgeBroker, AccountSnapshot as DomainAccount, BrokerAdapter,
    BrokerChildOrder, BrokerError, BrokerHealth as DomainHealth, BrokerOrder, BrokerOrderLeg,
    BrokerOrderRequest, BrokerOrderStatus, BrokerOrderType, Fill as DomainFill, OrderSide,
    PositionSnapshot as DomainPosition,
};
use chrono::{DateTime, Utc};
use optiontrader_proto::broker_v1::{
    broker_adapter_service_client::BrokerAdapterServiceClient, AccountSnapshot, AdaptivePriority,
    BrokerChildOrderSnapshot, BrokerHealth, BrokerId, BrokerOrderSnapshot,
    BrokerOrderStatus as ProtoStatus, BrokerOrderType as ProtoOrderType, BrokerSnapshot,
    CancelBrokerOrderRequest, FillSnapshot, GetBrokerSnapshotRequest, OrderSide as ProtoSide,
    PositionSnapshot, RecoverBrokerOrderRequest, SubmitBrokerOrderRequest,
};
use prost::Message;
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerRecoveryError {
    UnsupportedBroker,
    Unavailable,
    InvalidSnapshot,
    NotReconciled,
    OrderConflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerMutationError {
    Disabled,
    NotReady,
    Rejected,
    OutcomeUnknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredBrokerOrder {
    pub order: BrokerOrder,
    pub buying_power: Decimal,
}

#[derive(Debug, Clone)]
pub struct ValidatedBrokerSnapshot {
    pub snapshot: BrokerSnapshot,
    pub snapshot_hash: String,
    pub buying_power: Decimal,
}

#[derive(Clone)]
pub enum BrokerSnapshotAuthority {
    Remote {
        ibkr_endpoint: String,
        longbridge: Arc<Mutex<LongbridgeSnapshotState>>,
    },
    #[cfg(test)]
    Fixed(Result<RecoveredBrokerOrder, BrokerRecoveryError>),
    #[cfg(test)]
    FullFixed(Result<ValidatedBrokerSnapshot, BrokerRecoveryError>),
    #[cfg(test)]
    MutationFixed {
        bind: Result<(), BrokerRecoveryError>,
        submit: Result<BrokerOrder, BrokerMutationError>,
        cancel: Result<BrokerOrder, BrokerMutationError>,
    },
}

pub(crate) struct LongbridgeSnapshotState {
    adapter: Option<LongbridgeBroker>,
    sequence: u64,
    submission_enabled: bool,
}

impl BrokerSnapshotAuthority {
    pub fn from_env(longbridge_submission_enabled: bool) -> Self {
        Self::Remote {
            ibkr_endpoint: std::env::var("OPTIONTRADER_IBKR_SIDECAR_GRPC")
                .unwrap_or_else(|_| "http://127.0.0.1:50053".into()),
            longbridge: Arc::new(Mutex::new(LongbridgeSnapshotState {
                adapter: None,
                sequence: 0,
                submission_enabled: longbridge_submission_enabled,
            })),
        }
    }

    pub async fn recover(
        &self,
        expected_order: SubmitBrokerOrderRequest,
        expected_broker_order_id: String,
        now: DateTime<Utc>,
    ) -> Result<RecoveredBrokerOrder, BrokerRecoveryError> {
        let (ibkr_endpoint, longbridge) = match self {
            Self::Remote {
                ibkr_endpoint,
                longbridge,
            } => (ibkr_endpoint, longbridge),
            #[cfg(test)]
            Self::Fixed(result) => return result.clone(),
            #[cfg(test)]
            Self::FullFixed(_) => return Err(BrokerRecoveryError::Unavailable),
            #[cfg(test)]
            Self::MutationFixed { .. } => return Err(BrokerRecoveryError::Unavailable),
        };
        if expected_broker_order_id.is_empty() {
            return Err(BrokerRecoveryError::OrderConflict);
        }
        if BrokerId::try_from(expected_order.broker_id).ok() == Some(BrokerId::Longbridge) {
            let state = Arc::clone(longbridge);
            return tokio::task::spawn_blocking(move || {
                let mut state = state.lock().map_err(|_| BrokerRecoveryError::Unavailable)?;
                if state.adapter.is_none() {
                    state.adapter = Some(
                        LongbridgeBroker::from_env(state.submission_enabled)
                            .map_err(map_longbridge_recovery_error)?,
                    );
                }
                let request = domain_request(&expected_order)?;
                let next_sequence = state.sequence.saturating_add(1).max(1);
                let adapter = state
                    .adapter
                    .as_mut()
                    .ok_or(BrokerRecoveryError::Unavailable)?;
                adapter
                    .restore_expected_order(request, &expected_broker_order_id)
                    .map_err(map_longbridge_recovery_error)?;
                adapter.reconcile().map_err(map_longbridge_recovery_error)?;
                let recovered = adapter
                    .orders()
                    .into_iter()
                    .find(|order| order.broker_order_id == expected_broker_order_id)
                    .ok_or(BrokerRecoveryError::OrderConflict)?;
                let snapshot = longbridge_snapshot(adapter, next_sequence, now);
                let recovered_proto = proto_order(recovered);
                let result = validate_snapshot(
                    snapshot,
                    recovered_proto,
                    &expected_order,
                    &expected_broker_order_id,
                    now,
                );
                state.sequence = next_sequence;
                result
            })
            .await
            .map_err(|_| BrokerRecoveryError::Unavailable)?;
        }
        if BrokerId::try_from(expected_order.broker_id).ok() != Some(BrokerId::Ibkr) {
            return Err(BrokerRecoveryError::UnsupportedBroker);
        }
        let mut client = tokio::time::timeout(
            Duration::from_secs(2),
            BrokerAdapterServiceClient::connect(ibkr_endpoint.clone()),
        )
        .await
        .map_err(|_| BrokerRecoveryError::Unavailable)?
        .map_err(|_| BrokerRecoveryError::Unavailable)?;
        let recovered = tokio::time::timeout(
            Duration::from_secs(3),
            client.recover_broker_order(RecoverBrokerOrderRequest {
                expected_order: Some(expected_order.clone()),
                expected_broker_order_id: expected_broker_order_id.clone(),
            }),
        )
        .await
        .map_err(|_| BrokerRecoveryError::Unavailable)?
        .map_err(|_| BrokerRecoveryError::Unavailable)?
        .into_inner();
        let snapshot = tokio::time::timeout(
            Duration::from_secs(3),
            client.get_broker_snapshot(GetBrokerSnapshotRequest {
                broker_id: expected_order.broker_id,
            }),
        )
        .await
        .map_err(|_| BrokerRecoveryError::Unavailable)?
        .map_err(|_| BrokerRecoveryError::Unavailable)?
        .into_inner();
        validate_snapshot(
            snapshot,
            recovered,
            &expected_order,
            &expected_broker_order_id,
            now,
        )
    }

    pub async fn fetch_snapshot(
        &self,
        broker_id: i32,
        now: DateTime<Utc>,
    ) -> Result<ValidatedBrokerSnapshot, BrokerRecoveryError> {
        let (ibkr_endpoint, longbridge) = match self {
            Self::Remote {
                ibkr_endpoint,
                longbridge,
            } => (ibkr_endpoint, longbridge),
            #[cfg(test)]
            Self::Fixed(_) => return Err(BrokerRecoveryError::Unavailable),
            #[cfg(test)]
            Self::FullFixed(result) => return result.clone(),
            #[cfg(test)]
            Self::MutationFixed { .. } => return Err(BrokerRecoveryError::Unavailable),
        };
        if BrokerId::try_from(broker_id).ok() == Some(BrokerId::Longbridge) {
            let state = Arc::clone(longbridge);
            return tokio::task::spawn_blocking(move || {
                let mut state = state.lock().map_err(|_| BrokerRecoveryError::Unavailable)?;
                if state.adapter.is_none() {
                    state.adapter = Some(
                        LongbridgeBroker::from_env(state.submission_enabled)
                            .map_err(map_longbridge_recovery_error)?,
                    );
                }
                let next_sequence = state.sequence.saturating_add(1).max(1);
                let adapter = state
                    .adapter
                    .as_mut()
                    .ok_or(BrokerRecoveryError::Unavailable)?;
                adapter.reconcile().map_err(map_longbridge_recovery_error)?;
                let snapshot = longbridge_snapshot(adapter, next_sequence, now);
                let result = validate_full_snapshot(snapshot, broker_id, now);
                state.sequence = next_sequence;
                result
            })
            .await
            .map_err(|_| BrokerRecoveryError::Unavailable)?;
        }
        if BrokerId::try_from(broker_id).ok() != Some(BrokerId::Ibkr) {
            return Err(BrokerRecoveryError::UnsupportedBroker);
        }
        let mut client = tokio::time::timeout(
            Duration::from_secs(2),
            BrokerAdapterServiceClient::connect(ibkr_endpoint.clone()),
        )
        .await
        .map_err(|_| BrokerRecoveryError::Unavailable)?
        .map_err(|_| BrokerRecoveryError::Unavailable)?;
        let snapshot = tokio::time::timeout(
            Duration::from_secs(3),
            client.get_broker_snapshot(GetBrokerSnapshotRequest { broker_id }),
        )
        .await
        .map_err(|_| BrokerRecoveryError::Unavailable)?
        .map_err(|_| BrokerRecoveryError::Unavailable)?
        .into_inner();
        validate_full_snapshot(snapshot, broker_id, now)
    }

    /// Rebuild the mutation adapter's in-memory identity ledger after a
    /// separately authenticated read-only recovery. This performs broker reads
    /// only; it cannot submit, replace, or cancel an order.
    pub async fn bind_recovered_order_for_mutation(
        &self,
        expected_order: SubmitBrokerOrderRequest,
        expected_broker_order_id: String,
    ) -> Result<(), BrokerRecoveryError> {
        if expected_broker_order_id.is_empty()
            || BrokerId::try_from(expected_order.broker_id).ok() != Some(BrokerId::Longbridge)
        {
            return Err(BrokerRecoveryError::OrderConflict);
        }
        let longbridge = match self {
            Self::Remote { longbridge, .. } => longbridge,
            #[cfg(test)]
            Self::Fixed(_) | Self::FullFixed(_) => return Err(BrokerRecoveryError::Unavailable),
            #[cfg(test)]
            Self::MutationFixed { bind, .. } => return bind.clone(),
        };
        let state = Arc::clone(longbridge);
        tokio::task::spawn_blocking(move || {
            let mut state = state.lock().map_err(|_| BrokerRecoveryError::Unavailable)?;
            if !state.submission_enabled {
                return Err(BrokerRecoveryError::NotReconciled);
            }
            if state.adapter.is_none() {
                state.adapter =
                    Some(LongbridgeBroker::from_env(true).map_err(map_longbridge_recovery_error)?);
            }
            let request = domain_request(&expected_order)?;
            state
                .adapter
                .as_mut()
                .ok_or(BrokerRecoveryError::Unavailable)?
                .restore_expected_order(request, &expected_broker_order_id)
                .map_err(map_longbridge_recovery_error)?;
            Ok(())
        })
        .await
        .map_err(|_| BrokerRecoveryError::Unavailable)?
    }

    pub async fn submit_order(
        &self,
        request: SubmitBrokerOrderRequest,
    ) -> Result<BrokerOrder, BrokerMutationError> {
        let (ibkr_endpoint, longbridge) = match self {
            Self::Remote {
                ibkr_endpoint,
                longbridge,
            } => (ibkr_endpoint, longbridge),
            #[cfg(test)]
            Self::Fixed(_) | Self::FullFixed(_) => return Err(BrokerMutationError::Disabled),
            #[cfg(test)]
            Self::MutationFixed { submit, .. } => return submit.clone(),
        };
        match BrokerId::try_from(request.broker_id).ok() {
            Some(BrokerId::Longbridge) => {
                let state = Arc::clone(longbridge);
                tokio::task::spawn_blocking(move || {
                    let mut state = state
                        .lock()
                        .map_err(|_| BrokerMutationError::OutcomeUnknown)?;
                    if !state.submission_enabled {
                        return Err(BrokerMutationError::Disabled);
                    }
                    if state.adapter.is_none() {
                        state.adapter = Some(
                            LongbridgeBroker::from_env(true)
                                .map_err(|_| BrokerMutationError::NotReady)?,
                        );
                    }
                    let adapter = state
                        .adapter
                        .as_mut()
                        .ok_or(BrokerMutationError::NotReady)?;
                    let domain =
                        domain_request(&request).map_err(|_| BrokerMutationError::Rejected)?;
                    submit_after_reconciliation(adapter, domain)
                })
                .await
                .map_err(|_| BrokerMutationError::OutcomeUnknown)?
            }
            Some(BrokerId::Ibkr) => {
                let mut client = tokio::time::timeout(
                    Duration::from_secs(2),
                    BrokerAdapterServiceClient::connect(ibkr_endpoint.clone()),
                )
                .await
                .map_err(|_| BrokerMutationError::OutcomeUnknown)?
                .map_err(|_| BrokerMutationError::NotReady)?;
                let response = tokio::time::timeout(
                    Duration::from_secs(5),
                    client.submit_broker_order(request.clone()),
                )
                .await
                .map_err(|_| BrokerMutationError::OutcomeUnknown)?
                .map_err(|status| {
                    if status.code() == tonic::Code::InvalidArgument {
                        BrokerMutationError::Rejected
                    } else {
                        BrokerMutationError::OutcomeUnknown
                    }
                })?
                .into_inner();
                validate_mutation_order(response, &request)
            }
            Some(BrokerId::Unspecified) | None => Err(BrokerMutationError::Rejected),
        }
    }

    pub async fn cancel_order(
        &self,
        broker_id: i32,
        broker_order_id: String,
    ) -> Result<BrokerOrder, BrokerMutationError> {
        if broker_order_id.is_empty() {
            return Err(BrokerMutationError::Rejected);
        }
        let (ibkr_endpoint, longbridge) = match self {
            Self::Remote {
                ibkr_endpoint,
                longbridge,
            } => (ibkr_endpoint, longbridge),
            #[cfg(test)]
            Self::Fixed(_) | Self::FullFixed(_) => return Err(BrokerMutationError::Disabled),
            #[cfg(test)]
            Self::MutationFixed { cancel, .. } => return cancel.clone(),
        };
        match BrokerId::try_from(broker_id).ok() {
            Some(BrokerId::Longbridge) => {
                let state = Arc::clone(longbridge);
                tokio::task::spawn_blocking(move || {
                    let mut state = state
                        .lock()
                        .map_err(|_| BrokerMutationError::OutcomeUnknown)?;
                    if !state.submission_enabled {
                        return Err(BrokerMutationError::Disabled);
                    }
                    if state.adapter.is_none() {
                        state.adapter = Some(
                            LongbridgeBroker::from_env(true)
                                .map_err(|_| BrokerMutationError::NotReady)?,
                        );
                    }
                    let adapter = state
                        .adapter
                        .as_mut()
                        .ok_or(BrokerMutationError::NotReady)?;
                    let mut order = adapter
                        .cancel(&broker_order_id)
                        .map_err(map_longbridge_mutation_error)?;
                    if order.status == BrokerOrderStatus::Working {
                        order.status = BrokerOrderStatus::ReconcilePending;
                        order.residual_exposure = true;
                    }
                    Ok(order)
                })
                .await
                .map_err(|_| BrokerMutationError::OutcomeUnknown)?
            }
            Some(BrokerId::Ibkr) => {
                let mut client = tokio::time::timeout(
                    Duration::from_secs(2),
                    BrokerAdapterServiceClient::connect(ibkr_endpoint.clone()),
                )
                .await
                .map_err(|_| BrokerMutationError::OutcomeUnknown)?
                .map_err(|_| BrokerMutationError::NotReady)?;
                let response = tokio::time::timeout(
                    Duration::from_secs(5),
                    client.cancel_broker_order(CancelBrokerOrderRequest {
                        broker_id,
                        broker_order_id,
                    }),
                )
                .await
                .map_err(|_| BrokerMutationError::OutcomeUnknown)?
                .map_err(|_| BrokerMutationError::OutcomeUnknown)?
                .into_inner();
                let mut order =
                    domain_order(response).map_err(|_| BrokerMutationError::OutcomeUnknown)?;
                if order.status == BrokerOrderStatus::Working {
                    order.status = BrokerOrderStatus::ReconcilePending;
                    order.residual_exposure = true;
                }
                Ok(order)
            }
            Some(BrokerId::Unspecified) | None => Err(BrokerMutationError::Rejected),
        }
    }
}

fn validate_mutation_order(
    raw: BrokerOrderSnapshot,
    expected: &SubmitBrokerOrderRequest,
) -> Result<BrokerOrder, BrokerMutationError> {
    let broker_order_id = raw.broker_order_id.clone();
    if broker_order_id.is_empty() || !order_matches_expected(&raw, expected, &broker_order_id) {
        return Err(BrokerMutationError::OutcomeUnknown);
    }
    domain_order(raw).map_err(|_| BrokerMutationError::OutcomeUnknown)
}

fn map_longbridge_recovery_error(error: BrokerError) -> BrokerRecoveryError {
    match error {
        BrokerError::Disconnected | BrokerError::InvalidConfiguration => {
            BrokerRecoveryError::Unavailable
        }
        BrokerError::OrderNotFound
        | BrokerError::DuplicateConflict
        | BrokerError::InvalidOrderType
        | BrokerError::InvalidPrice
        | BrokerError::InvalidQuantity
        | BrokerError::UnsupportedOrderShape => BrokerRecoveryError::OrderConflict,
        BrokerError::NotReconciled
        | BrokerError::QuoteUnavailable
        | BrokerError::QuoteStale
        | BrokerError::QuoteCrossed
        | BrokerError::SpreadTooWide
        | BrokerError::TerminalOrder
        | BrokerError::LiveSubmissionDisabled => BrokerRecoveryError::NotReconciled,
    }
}

fn map_longbridge_mutation_error(error: BrokerError) -> BrokerMutationError {
    match error {
        BrokerError::InvalidQuantity
        | BrokerError::InvalidOrderType
        | BrokerError::InvalidPrice
        | BrokerError::QuoteUnavailable
        | BrokerError::QuoteStale
        | BrokerError::QuoteCrossed
        | BrokerError::SpreadTooWide
        | BrokerError::UnsupportedOrderShape
        | BrokerError::DuplicateConflict
        | BrokerError::LiveSubmissionDisabled => BrokerMutationError::Rejected,
        BrokerError::NotReconciled => BrokerMutationError::NotReady,
        BrokerError::Disconnected
        | BrokerError::OrderNotFound
        | BrokerError::TerminalOrder
        | BrokerError::InvalidConfiguration => BrokerMutationError::OutcomeUnknown,
    }
}

fn submit_after_reconciliation(
    adapter: &mut dyn BrokerAdapter,
    request: BrokerOrderRequest,
) -> Result<BrokerOrder, BrokerMutationError> {
    adapter.reconcile().map_err(map_longbridge_mutation_error)?;
    adapter
        .submit(request)
        .map_err(map_longbridge_mutation_error)
}

fn domain_request(
    raw: &SubmitBrokerOrderRequest,
) -> Result<BrokerOrderRequest, BrokerRecoveryError> {
    if BrokerId::try_from(raw.broker_id).ok() != Some(BrokerId::Longbridge)
        || raw.idempotency_key.is_empty()
        || raw.plan_hash.len() != 64
        || raw.total_quantity == 0
    {
        return Err(BrokerRecoveryError::OrderConflict);
    }
    let domain_type = order_type(raw.order_type)?;
    if (domain_type == BrokerOrderType::AdaptiveLimit
        && AdaptivePriority::try_from(raw.adaptive_priority).ok() != Some(AdaptivePriority::Normal))
        || (domain_type != BrokerOrderType::AdaptiveLimit
            && AdaptivePriority::try_from(raw.adaptive_priority).ok()
                != Some(AdaptivePriority::Unspecified))
    {
        return Err(BrokerRecoveryError::OrderConflict);
    }
    let submitted_price = optional_price(&raw.submitted_price, domain_type)?;
    let legs = raw
        .legs
        .iter()
        .map(|leg| {
            Ok(BrokerOrderLeg {
                contract_id: required(leg.contract_id.clone())?,
                side: side(leg.side)?,
                quantity: positive(leg.quantity)?,
                broker_contract_id: nonempty(leg.broker_contract_id.clone()),
                symbol: nonempty(leg.symbol.clone()),
                exchange: nonempty(leg.exchange.clone()),
                submitted_price: optional_price(&leg.submitted_price, domain_type)?,
            })
        })
        .collect::<Result<Vec<_>, BrokerRecoveryError>>()?;
    Ok(BrokerOrderRequest {
        idempotency_key: raw.idempotency_key.clone(),
        plan_hash: raw.plan_hash.clone(),
        side: side(raw.side)?,
        order_type: domain_type,
        total_quantity: raw.total_quantity,
        submitted_price,
        legs,
    })
}

fn longbridge_snapshot(
    adapter: &LongbridgeBroker,
    sequence: u64,
    now: DateTime<Utc>,
) -> BrokerSnapshot {
    BrokerSnapshot {
        schema_version: "1.0".into(),
        snapshot_sequence: sequence,
        account: Some(proto_account(adapter.account(), now)),
        positions: adapter
            .positions()
            .into_iter()
            .map(proto_position)
            .collect(),
        orders: adapter.orders().into_iter().map(proto_order).collect(),
        fills: adapter.fills().into_iter().map(proto_fill).collect(),
    }
}

fn proto_account(account: DomainAccount, now: DateTime<Utc>) -> AccountSnapshot {
    AccountSnapshot {
        broker_id: BrokerId::Longbridge as i32,
        occurred_at_utc: now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        health: match account.health {
            DomainHealth::Healthy => BrokerHealth::Healthy,
            DomainHealth::Degraded => BrokerHealth::Degraded,
            DomainHealth::Disconnected => BrokerHealth::Disconnected,
            DomainHealth::Reconciling => BrokerHealth::Reconciling,
        } as i32,
        reconciled: account.reconciled,
        buying_power: account.buying_power.to_string(),
        net_liquidation: account.net_liquidation.to_string(),
        currency: account.currency,
    }
}

fn proto_position(position: DomainPosition) -> PositionSnapshot {
    PositionSnapshot {
        contract_id: position.contract_id,
        quantity: position.quantity,
        average_price: position.average_price.to_string(),
    }
}

fn proto_fill(fill: DomainFill) -> FillSnapshot {
    FillSnapshot {
        fill_id: fill.fill_id,
        broker_order_id: fill.broker_order_id,
        contract_id: fill.contract_id,
        side: proto_side(fill.side) as i32,
        quantity: fill.quantity,
        price: fill.price.to_string(),
        occurred_at_utc: fill
            .occurred_at_utc
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
    }
}

fn proto_order(order: BrokerOrder) -> BrokerOrderSnapshot {
    let order_type = proto_order_type(order.order_type);
    BrokerOrderSnapshot {
        broker_order_id: order.broker_order_id,
        idempotency_key: order.idempotency_key,
        plan_hash: order.plan_hash,
        status: proto_status(order.status) as i32,
        total_quantity: order.total_quantity,
        filled_quantity: order.filled_quantity,
        submitted_price: order
            .submitted_price
            .map_or_else(String::new, |v| v.to_string()),
        legs: order
            .legs
            .into_iter()
            .map(|leg| optiontrader_proto::broker_v1::BrokerOrderLeg {
                contract_id: leg.contract_id,
                side: proto_side(leg.side) as i32,
                quantity: leg.quantity,
                broker_contract_id: leg.broker_contract_id.unwrap_or_default(),
                symbol: leg.symbol.unwrap_or_default(),
                exchange: leg.exchange.unwrap_or_default(),
                submitted_price: leg
                    .submitted_price
                    .map_or_else(String::new, |v| v.to_string()),
            })
            .collect(),
        side: proto_side(order.side) as i32,
        order_type: order_type as i32,
        adaptive_priority: if order_type == ProtoOrderType::AdaptiveLimit {
            AdaptivePriority::Normal as i32
        } else {
            AdaptivePriority::Unspecified as i32
        },
        child_orders: order
            .child_orders
            .into_iter()
            .map(|child| BrokerChildOrderSnapshot {
                broker_order_id: child.broker_order_id,
                leg_index: u32::try_from(child.leg_index).unwrap_or(u32::MAX),
                contract_id: child.contract_id,
                side: proto_side(child.side) as i32,
                quantity: child.quantity,
                filled_quantity: child.filled_quantity,
                status: proto_status(child.status) as i32,
                submitted_price: child
                    .submitted_price
                    .map_or_else(String::new, |v| v.to_string()),
            })
            .collect(),
        residual_exposure: order.residual_exposure,
    }
}

fn proto_side(side: OrderSide) -> ProtoSide {
    match side {
        OrderSide::Buy => ProtoSide::Buy,
        OrderSide::Sell => ProtoSide::Sell,
    }
}

fn proto_order_type(order_type: BrokerOrderType) -> ProtoOrderType {
    match order_type {
        BrokerOrderType::Market => ProtoOrderType::Market,
        BrokerOrderType::Limit => ProtoOrderType::Limit,
        BrokerOrderType::AdaptiveLimit => ProtoOrderType::AdaptiveLimit,
    }
}

fn proto_status(status: BrokerOrderStatus) -> ProtoStatus {
    match status {
        BrokerOrderStatus::Working => ProtoStatus::Working,
        BrokerOrderStatus::PartialFill => ProtoStatus::PartialFill,
        BrokerOrderStatus::Filled => ProtoStatus::Filled,
        BrokerOrderStatus::Cancelled => ProtoStatus::Cancelled,
        BrokerOrderStatus::Rejected => ProtoStatus::Rejected,
        BrokerOrderStatus::ReconcilePending => ProtoStatus::ReconcilePending,
    }
}

fn validate_full_snapshot(
    snapshot: BrokerSnapshot,
    broker_id: i32,
    now: DateTime<Utc>,
) -> Result<ValidatedBrokerSnapshot, BrokerRecoveryError> {
    if snapshot.schema_version != "1.0" || snapshot.snapshot_sequence == 0 {
        return Err(BrokerRecoveryError::InvalidSnapshot);
    }
    let account = snapshot
        .account
        .as_ref()
        .ok_or(BrokerRecoveryError::InvalidSnapshot)?;
    let account_at = parse_utc(&account.occurred_at_utc)?;
    let account_age = now.timestamp_millis() - account_at.timestamp_millis();
    if account.broker_id != broker_id
        || BrokerHealth::try_from(account.health).ok() != Some(BrokerHealth::Healthy)
        || !account.reconciled
        || !(-5_000..=30_000).contains(&account_age)
        || account.currency.trim().is_empty()
    {
        return Err(BrokerRecoveryError::NotReconciled);
    }
    let buying_power = positive_or_zero_decimal(&account.buying_power)?;
    positive_or_zero_decimal(&account.net_liquidation)?;

    let mut position_ids = BTreeSet::new();
    for position in &snapshot.positions {
        if position.contract_id.is_empty()
            || !position_ids.insert(position.contract_id.as_str())
            || positive_or_zero_decimal(&position.average_price).is_err()
        {
            return Err(BrokerRecoveryError::InvalidSnapshot);
        }
    }
    let mut order_ids = BTreeSet::new();
    for order in &snapshot.orders {
        if order.broker_order_id.is_empty()
            || !order_ids.insert(order.broker_order_id.as_str())
            || order.idempotency_key.is_empty()
            || order.plan_hash.len() != 64
            || !order
                .plan_hash
                .bytes()
                .all(|value| value.is_ascii_hexdigit())
            || order.legs.is_empty()
            || order.total_quantity == 0
            || order.filled_quantity > order.total_quantity
            || domain_order(order.clone()).is_err()
        {
            return Err(BrokerRecoveryError::InvalidSnapshot);
        }
        for child in &order.child_orders {
            if !order_ids.insert(child.broker_order_id.as_str()) {
                return Err(BrokerRecoveryError::InvalidSnapshot);
            }
        }
    }
    let mut fill_ids = BTreeSet::new();
    for fill in &snapshot.fills {
        let occurred = parse_utc(&fill.occurred_at_utc)?;
        if fill.fill_id.is_empty()
            || fill.broker_order_id.is_empty()
            || fill.contract_id.is_empty()
            || !fill_ids.insert(fill.fill_id.as_str())
            || side(fill.side).is_err()
            || fill.quantity == 0
            || positive_or_zero_decimal(&fill.price)? == Decimal::ZERO
            || occurred.timestamp_millis() - now.timestamp_millis() > 5_000
        {
            return Err(BrokerRecoveryError::InvalidSnapshot);
        }
    }
    let snapshot_hash = format!("{:x}", Sha256::digest(snapshot.encode_to_vec()));
    Ok(ValidatedBrokerSnapshot {
        snapshot,
        snapshot_hash,
        buying_power,
    })
}

fn validate_snapshot(
    snapshot: BrokerSnapshot,
    recovered: BrokerOrderSnapshot,
    expected: &SubmitBrokerOrderRequest,
    expected_broker_order_id: &str,
    now: DateTime<Utc>,
) -> Result<RecoveredBrokerOrder, BrokerRecoveryError> {
    if snapshot.schema_version != "1.0" || snapshot.snapshot_sequence == 0 {
        return Err(BrokerRecoveryError::InvalidSnapshot);
    }
    let account = snapshot
        .account
        .ok_or(BrokerRecoveryError::InvalidSnapshot)?;
    let account_at = parse_utc(&account.occurred_at_utc)?;
    let account_age = now.timestamp_millis() - account_at.timestamp_millis();
    if account.broker_id != expected.broker_id
        || BrokerHealth::try_from(account.health).ok() != Some(BrokerHealth::Healthy)
        || !account.reconciled
        || !(-5_000..=30_000).contains(&account_age)
    {
        return Err(BrokerRecoveryError::NotReconciled);
    }
    let buying_power = positive_or_zero_decimal(&account.buying_power)?;
    let mut ids = BTreeSet::new();
    if snapshot
        .orders
        .iter()
        .any(|order| !ids.insert(order.broker_order_id.as_str()))
    {
        return Err(BrokerRecoveryError::InvalidSnapshot);
    }
    let authoritative = snapshot
        .orders
        .into_iter()
        .find(|order| order.broker_order_id == expected_broker_order_id)
        .ok_or(BrokerRecoveryError::OrderConflict)?;
    if recovered.encode_to_vec() != authoritative.encode_to_vec()
        || !order_matches_expected(&authoritative, expected, expected_broker_order_id)
    {
        return Err(BrokerRecoveryError::OrderConflict);
    }
    Ok(RecoveredBrokerOrder {
        order: domain_order(authoritative)?,
        buying_power,
    })
}

fn order_matches_expected(
    order: &BrokerOrderSnapshot,
    expected: &SubmitBrokerOrderRequest,
    expected_broker_order_id: &str,
) -> bool {
    order.broker_order_id == expected_broker_order_id
        && order.idempotency_key == expected.idempotency_key
        && order.plan_hash == expected.plan_hash
        && order.total_quantity == expected.total_quantity
        && order.submitted_price == expected.submitted_price
        && order.legs == expected.legs
        && order.side == expected.side
        && order.order_type == expected.order_type
        && order.adaptive_priority == expected.adaptive_priority
}

fn domain_order(raw: BrokerOrderSnapshot) -> Result<BrokerOrder, BrokerRecoveryError> {
    let domain_side = side(raw.side)?;
    let order_type = order_type(raw.order_type)?;
    let domain_status = status(raw.status)?;
    let submitted_price = optional_price(&raw.submitted_price, order_type)?;
    let legs = raw
        .legs
        .into_iter()
        .map(|leg| {
            Ok(BrokerOrderLeg {
                contract_id: required(leg.contract_id)?,
                side: side(leg.side)?,
                quantity: positive(leg.quantity)?,
                broker_contract_id: nonempty(leg.broker_contract_id),
                symbol: nonempty(leg.symbol),
                exchange: nonempty(leg.exchange),
                submitted_price: optional_price(&leg.submitted_price, order_type)?,
            })
        })
        .collect::<Result<Vec<_>, BrokerRecoveryError>>()?;
    let child_orders = raw
        .child_orders
        .into_iter()
        .map(|child| {
            Ok(BrokerChildOrder {
                broker_order_id: required(child.broker_order_id)?,
                leg_index: usize::try_from(child.leg_index)
                    .map_err(|_| BrokerRecoveryError::InvalidSnapshot)?,
                contract_id: required(child.contract_id)?,
                side: side(child.side)?,
                quantity: positive(child.quantity)?,
                filled_quantity: child.filled_quantity,
                status: status(child.status)?,
                submitted_price: optional_price(&child.submitted_price, order_type)?,
            })
        })
        .collect::<Result<Vec<_>, BrokerRecoveryError>>()?;
    Ok(BrokerOrder {
        broker_order_id: required(raw.broker_order_id)?,
        idempotency_key: required(raw.idempotency_key)?,
        plan_hash: required(raw.plan_hash)?,
        status: domain_status,
        side: domain_side,
        order_type,
        total_quantity: positive(raw.total_quantity)?,
        filled_quantity: raw.filled_quantity,
        submitted_price,
        legs,
        child_orders,
        residual_exposure: raw.residual_exposure,
    })
}

fn parse_utc(value: &str) -> Result<DateTime<Utc>, BrokerRecoveryError> {
    if !value.ends_with('Z') {
        return Err(BrokerRecoveryError::InvalidSnapshot);
    }
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| BrokerRecoveryError::InvalidSnapshot)
}

fn positive_or_zero_decimal(value: &str) -> Result<Decimal, BrokerRecoveryError> {
    let value = Decimal::from_str_exact(value).map_err(|_| BrokerRecoveryError::InvalidSnapshot)?;
    if value.is_sign_negative() {
        return Err(BrokerRecoveryError::InvalidSnapshot);
    }
    Ok(value)
}

fn optional_price(
    value: &str,
    order_type: BrokerOrderType,
) -> Result<Option<Decimal>, BrokerRecoveryError> {
    if order_type == BrokerOrderType::Market {
        return value
            .is_empty()
            .then_some(None)
            .ok_or(BrokerRecoveryError::InvalidSnapshot);
    }
    let price = Decimal::from_str_exact(value).map_err(|_| BrokerRecoveryError::InvalidSnapshot)?;
    (price > Decimal::ZERO)
        .then_some(Some(price))
        .ok_or(BrokerRecoveryError::InvalidSnapshot)
}

fn side(value: i32) -> Result<OrderSide, BrokerRecoveryError> {
    match ProtoSide::try_from(value).ok() {
        Some(ProtoSide::Buy) => Ok(OrderSide::Buy),
        Some(ProtoSide::Sell) => Ok(OrderSide::Sell),
        _ => Err(BrokerRecoveryError::InvalidSnapshot),
    }
}

fn order_type(value: i32) -> Result<BrokerOrderType, BrokerRecoveryError> {
    match ProtoOrderType::try_from(value).ok() {
        Some(ProtoOrderType::Market) => Ok(BrokerOrderType::Market),
        Some(ProtoOrderType::Limit) => Ok(BrokerOrderType::Limit),
        Some(ProtoOrderType::AdaptiveLimit) => Ok(BrokerOrderType::AdaptiveLimit),
        _ => Err(BrokerRecoveryError::InvalidSnapshot),
    }
}

fn status(value: i32) -> Result<BrokerOrderStatus, BrokerRecoveryError> {
    match ProtoStatus::try_from(value).ok() {
        Some(ProtoStatus::Working) => Ok(BrokerOrderStatus::Working),
        Some(ProtoStatus::PartialFill) => Ok(BrokerOrderStatus::PartialFill),
        Some(ProtoStatus::Filled) => Ok(BrokerOrderStatus::Filled),
        Some(ProtoStatus::Cancelled) => Ok(BrokerOrderStatus::Cancelled),
        Some(ProtoStatus::Rejected) => Ok(BrokerOrderStatus::Rejected),
        Some(ProtoStatus::ReconcilePending) => Ok(BrokerOrderStatus::ReconcilePending),
        _ => Err(BrokerRecoveryError::InvalidSnapshot),
    }
}

fn positive(value: u32) -> Result<u32, BrokerRecoveryError> {
    (value > 0)
        .then_some(value)
        .ok_or(BrokerRecoveryError::InvalidSnapshot)
}

fn required(value: String) -> Result<String, BrokerRecoveryError> {
    (!value.is_empty())
        .then_some(value)
        .ok_or(BrokerRecoveryError::InvalidSnapshot)
}

fn nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

pub fn expected_request(broker_id: i32, request: BrokerOrderRequest) -> SubmitBrokerOrderRequest {
    let proto_side = match request.side {
        OrderSide::Buy => ProtoSide::Buy,
        OrderSide::Sell => ProtoSide::Sell,
    };
    let proto_type = match request.order_type {
        BrokerOrderType::Market => ProtoOrderType::Market,
        BrokerOrderType::Limit => ProtoOrderType::Limit,
        BrokerOrderType::AdaptiveLimit => ProtoOrderType::AdaptiveLimit,
    };
    SubmitBrokerOrderRequest {
        broker_id,
        idempotency_key: request.idempotency_key,
        plan_hash: request.plan_hash,
        total_quantity: request.total_quantity,
        submitted_price: request
            .submitted_price
            .map_or_else(String::new, |value| value.to_string()),
        legs: request
            .legs
            .into_iter()
            .map(|leg| optiontrader_proto::broker_v1::BrokerOrderLeg {
                contract_id: leg.contract_id,
                side: match leg.side {
                    OrderSide::Buy => ProtoSide::Buy as i32,
                    OrderSide::Sell => ProtoSide::Sell as i32,
                },
                quantity: leg.quantity,
                broker_contract_id: leg.broker_contract_id.unwrap_or_default(),
                symbol: leg.symbol.unwrap_or_default(),
                exchange: leg.exchange.unwrap_or_default(),
                submitted_price: leg
                    .submitted_price
                    .map_or_else(String::new, |value| value.to_string()),
            })
            .collect(),
        side: proto_side as i32,
        order_type: proto_type as i32,
        adaptive_priority: if request.order_type == BrokerOrderType::AdaptiveLimit {
            AdaptivePriority::Normal as i32
        } else {
            AdaptivePriority::Unspecified as i32
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker::PaperBroker;
    use optiontrader_proto::broker_v1::{
        AccountSnapshot, BrokerOrderLeg as ProtoLeg, BrokerOrderStatus as ProtoStatus,
    };

    fn now() -> DateTime<Utc> {
        "2026-07-21T14:30:01Z".parse().unwrap()
    }

    fn expected() -> SubmitBrokerOrderRequest {
        SubmitBrokerOrderRequest {
            broker_id: BrokerId::Ibkr as i32,
            idempotency_key: "submit-key".into(),
            plan_hash: "a".repeat(64),
            total_quantity: 2,
            submitted_price: "1.25".into(),
            legs: vec![ProtoLeg {
                contract_id: "QQQ-C-500".into(),
                side: ProtoSide::Buy as i32,
                quantity: 2,
                broker_contract_id: "123".into(),
                symbol: "QQQ".into(),
                exchange: "SMART".into(),
                submitted_price: "2.50".into(),
            }],
            side: ProtoSide::Buy as i32,
            order_type: ProtoOrderType::Limit as i32,
            adaptive_priority: AdaptivePriority::Unspecified as i32,
        }
    }

    fn order() -> BrokerOrderSnapshot {
        let expected = expected();
        BrokerOrderSnapshot {
            broker_order_id: "900".into(),
            idempotency_key: expected.idempotency_key,
            plan_hash: expected.plan_hash,
            status: ProtoStatus::Working as i32,
            total_quantity: expected.total_quantity,
            filled_quantity: 0,
            submitted_price: expected.submitted_price,
            legs: expected.legs,
            side: expected.side,
            order_type: expected.order_type,
            adaptive_priority: expected.adaptive_priority,
            child_orders: Vec::new(),
            residual_exposure: false,
        }
    }

    fn snapshot(order: BrokerOrderSnapshot) -> BrokerSnapshot {
        BrokerSnapshot {
            schema_version: "1.0".into(),
            snapshot_sequence: 7,
            account: Some(AccountSnapshot {
                broker_id: BrokerId::Ibkr as i32,
                occurred_at_utc: "2026-07-21T14:30:00Z".into(),
                health: BrokerHealth::Healthy as i32,
                reconciled: true,
                buying_power: "10000".into(),
                net_liquidation: "25000".into(),
                currency: "USD".into(),
            }),
            positions: Vec::new(),
            orders: vec![order],
            fills: Vec::new(),
        }
    }

    #[test]
    fn fresh_reconciled_snapshot_and_exact_order_are_required() {
        let expected = expected();
        let recovered = order();
        let result = validate_snapshot(
            snapshot(recovered.clone()),
            recovered,
            &expected,
            "900",
            now(),
        )
        .unwrap();
        assert_eq!(result.buying_power, Decimal::new(10_000, 0));
        assert_eq!(result.order.status, BrokerOrderStatus::Working);
    }

    #[test]
    fn mutation_response_is_bound_to_exact_request_shape() {
        let expected = expected();
        let accepted = validate_mutation_order(order(), &expected).unwrap();
        assert_eq!(accepted.broker_order_id, "900");

        let mut drifted = order();
        drifted.submitted_price = "1.26".into();
        assert_eq!(
            validate_mutation_order(drifted, &expected),
            Err(BrokerMutationError::OutcomeUnknown)
        );
    }

    #[tokio::test]
    async fn read_only_longbridge_authority_cannot_mutate() {
        let authority = BrokerSnapshotAuthority::from_env(false);
        let mut request = expected();
        request.broker_id = BrokerId::Longbridge as i32;
        assert_eq!(
            authority
                .bind_recovered_order_for_mutation(request.clone(), "native-order".into())
                .await,
            Err(BrokerRecoveryError::NotReconciled)
        );
        assert_eq!(
            authority.submit_order(request).await,
            Err(BrokerMutationError::Disabled)
        );
        assert_eq!(
            authority
                .cancel_order(BrokerId::Longbridge as i32, "native-order".into())
                .await,
            Err(BrokerMutationError::Disabled)
        );
    }

    #[test]
    fn mutation_adapter_must_reconcile_before_first_submit() {
        let mut raw = expected();
        raw.broker_id = BrokerId::Longbridge as i32;
        raw.legs[0].broker_contract_id = "QQQ260721C00500000.US".into();
        let request = domain_request(&raw).unwrap();
        let mut adapter = PaperBroker::new(broker::BrokerId::Longbridge);
        adapter.set_connection(DomainHealth::Reconciling, false);

        let submitted = submit_after_reconciliation(&mut adapter, request).unwrap();
        assert_eq!(submitted.status, BrokerOrderStatus::Working);
        assert_eq!(adapter.account().health, DomainHealth::Healthy);
        assert!(adapter.account().reconciled);
    }

    #[test]
    fn failed_mutation_reconciliation_never_reaches_submit() {
        let mut raw = expected();
        raw.broker_id = BrokerId::Longbridge as i32;
        raw.legs[0].broker_contract_id = "QQQ260721C00500000.US".into();
        let request = domain_request(&raw).unwrap();
        let mut adapter = PaperBroker::new(broker::BrokerId::Longbridge);
        adapter.set_connection(DomainHealth::Disconnected, false);

        assert_eq!(
            submit_after_reconciliation(&mut adapter, request),
            Err(BrokerMutationError::OutcomeUnknown)
        );
        assert!(adapter.orders().is_empty());
    }

    #[test]
    fn full_snapshot_validation_binds_hash_and_rejects_duplicate_facts() {
        let valid = snapshot(order());
        let expected_hash = format!("{:x}", Sha256::digest(valid.encode_to_vec()));
        let result = validate_full_snapshot(valid.clone(), BrokerId::Ibkr as i32, now()).unwrap();
        assert_eq!(result.snapshot_hash, expected_hash);
        assert_eq!(result.snapshot, valid);

        let mut duplicate = valid;
        duplicate.orders.push(duplicate.orders[0].clone());
        assert!(matches!(
            validate_full_snapshot(duplicate, BrokerId::Ibkr as i32, now()),
            Err(BrokerRecoveryError::InvalidSnapshot)
        ));
    }

    #[test]
    fn stale_account_duplicate_order_and_identity_drift_fail_closed() {
        let expected = expected();
        let recovered = order();
        let mut stale = snapshot(recovered.clone());
        stale.account.as_mut().unwrap().occurred_at_utc = "2026-07-21T14:29:00Z".into();
        assert_eq!(
            validate_snapshot(stale, recovered.clone(), &expected, "900", now()),
            Err(BrokerRecoveryError::NotReconciled)
        );

        let mut duplicate = snapshot(recovered.clone());
        duplicate.orders.push(recovered.clone());
        assert_eq!(
            validate_snapshot(duplicate, recovered.clone(), &expected, "900", now()),
            Err(BrokerRecoveryError::InvalidSnapshot)
        );

        let mut drifted = recovered.clone();
        drifted.plan_hash = "b".repeat(64);
        assert_eq!(
            validate_snapshot(snapshot(drifted), recovered, &expected, "900", now()),
            Err(BrokerRecoveryError::OrderConflict)
        );
    }

    #[test]
    fn longbridge_request_and_snapshot_projection_preserve_order_identity() {
        let mut raw = expected();
        raw.broker_id = BrokerId::Longbridge as i32;
        raw.legs[0].broker_contract_id = "QQQ260721C00500000.US".into();
        let request = domain_request(&raw).unwrap();
        assert_eq!(request.order_type, BrokerOrderType::Limit);
        assert_eq!(
            request.legs[0].broker_contract_id.as_deref(),
            Some("QQQ260721C00500000.US")
        );

        let domain = BrokerOrder {
            broker_order_id: "lb-900".into(),
            idempotency_key: request.idempotency_key.clone(),
            plan_hash: request.plan_hash.clone(),
            status: BrokerOrderStatus::Working,
            side: request.side,
            order_type: request.order_type,
            total_quantity: request.total_quantity,
            filled_quantity: 0,
            submitted_price: request.submitted_price,
            legs: request.legs,
            child_orders: Vec::new(),
            residual_exposure: false,
        };
        let projected = proto_order(domain.clone());
        assert_eq!(domain_order(projected).unwrap(), domain);

        raw.order_type = ProtoOrderType::AdaptiveLimit as i32;
        raw.adaptive_priority = AdaptivePriority::Unspecified as i32;
        assert_eq!(
            domain_request(&raw),
            Err(BrokerRecoveryError::OrderConflict)
        );
    }

    #[tokio::test]
    #[ignore = "requires explicitly supplied Longbridge demo credentials and network access"]
    async fn demo_account_longbridge_authority_snapshot_smoke() {
        assert_eq!(
            std::env::var("OPTIONTRADER_RUN_LONGBRIDGE_DEMO_SMOKE").as_deref(),
            Ok("true"),
            "explicit demo smoke opt-in is required"
        );
        let authority = BrokerSnapshotAuthority::from_env(false);
        let validated = authority
            .fetch_snapshot(BrokerId::Longbridge as i32, Utc::now())
            .await
            .expect("Longbridge authority must produce a validated read-only snapshot");
        assert_eq!(validated.snapshot.schema_version, "1.0");
        assert!(validated.snapshot.snapshot_sequence > 0);
        assert_eq!(validated.snapshot_hash.len(), 64);
        assert_eq!(
            validated.snapshot.account.unwrap().broker_id,
            BrokerId::Longbridge as i32
        );
    }
}
