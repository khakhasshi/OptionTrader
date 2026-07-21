//! Direct, read-only broker recovery authority used after process restart.

use std::collections::BTreeSet;
use std::time::Duration;

use broker::{
    BrokerChildOrder, BrokerOrder, BrokerOrderLeg, BrokerOrderRequest, BrokerOrderStatus,
    BrokerOrderType, OrderSide,
};
use chrono::{DateTime, Utc};
use optiontrader_proto::broker_v1::{
    broker_adapter_service_client::BrokerAdapterServiceClient, AdaptivePriority, BrokerHealth,
    BrokerId, BrokerOrderSnapshot, BrokerOrderStatus as ProtoStatus,
    BrokerOrderType as ProtoOrderType, BrokerSnapshot, GetBrokerSnapshotRequest,
    OrderSide as ProtoSide, RecoverBrokerOrderRequest, SubmitBrokerOrderRequest,
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
    },
    #[cfg(test)]
    Fixed(Result<RecoveredBrokerOrder, BrokerRecoveryError>),
    #[cfg(test)]
    FullFixed(Result<ValidatedBrokerSnapshot, BrokerRecoveryError>),
}

impl BrokerSnapshotAuthority {
    pub fn from_env() -> Self {
        Self::Remote {
            ibkr_endpoint: std::env::var("OPTIONTRADER_IBKR_SIDECAR_GRPC")
                .unwrap_or_else(|_| "http://127.0.0.1:50053".into()),
        }
    }

    pub async fn recover(
        &self,
        expected_order: SubmitBrokerOrderRequest,
        expected_broker_order_id: String,
        now: DateTime<Utc>,
    ) -> Result<RecoveredBrokerOrder, BrokerRecoveryError> {
        let ibkr_endpoint = match self {
            Self::Remote { ibkr_endpoint } => ibkr_endpoint,
            #[cfg(test)]
            Self::Fixed(result) => return result.clone(),
            #[cfg(test)]
            Self::FullFixed(_) => return Err(BrokerRecoveryError::Unavailable),
        };
        if BrokerId::try_from(expected_order.broker_id).ok() != Some(BrokerId::Ibkr) {
            return Err(BrokerRecoveryError::UnsupportedBroker);
        }
        if expected_broker_order_id.is_empty() {
            return Err(BrokerRecoveryError::OrderConflict);
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
        let ibkr_endpoint = match self {
            Self::Remote { ibkr_endpoint } => ibkr_endpoint,
            #[cfg(test)]
            Self::Fixed(_) => return Err(BrokerRecoveryError::Unavailable),
            #[cfg(test)]
            Self::FullFixed(result) => return result.clone(),
        };
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
}
