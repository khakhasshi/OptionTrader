//! Fail-closed order lifecycle, confirmation, idempotency and reconciliation.

use std::collections::{BTreeMap, BTreeSet};

use broker::{
    BrokerAdapter, BrokerChildOrder, BrokerError, BrokerOrder, BrokerOrderLeg, BrokerOrderRequest,
    BrokerOrderStatus, BrokerOrderType, OrderSide,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderState {
    Proposed,
    RiskRejected,
    AwaitingConfirmation,
    Approved,
    Submitting,
    Working,
    PartialFill,
    Filled,
    CancelPending,
    Cancelled,
    Rejected,
    Expired,
    ReconcilePending,
    Shadowed,
}

impl OrderState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::RiskRejected
                | Self::Filled
                | Self::Cancelled
                | Self::Rejected
                | Self::Expired
                | Self::Shadowed
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionError {
    InvalidTransition,
    ConfirmationExpired,
    ConfirmationHashMismatch,
    DuplicateConflict,
    Broker(BrokerError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderEvent {
    pub sequence: u64,
    pub from: OrderState,
    pub to: OrderState,
    pub kind: &'static str,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct OrderRecord {
    pub order_id: String,
    pub plan_id: String,
    pub plan_hash: String,
    pub idempotency_key: String,
    pub state: OrderState,
    pub expires_at: DateTime<Utc>,
    pub total_quantity: u32,
    pub filled_quantity: u32,
    pub broker_order_id: Option<String>,
    pub broker_child_order_ids: Vec<String>,
    pub broker_child_orders: Vec<BrokerChildOrder>,
    pub residual_exposure: bool,
    pub confirmation_id: Option<String>,
    pub events: Vec<OrderEvent>,
    base_state_version: u64,
    restored_updated_at: Option<DateTime<Utc>>,
}

impl OrderRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn restored(
        order_id: String,
        plan_id: String,
        plan_hash: String,
        idempotency_key: String,
        state: OrderState,
        expires_at: DateTime<Utc>,
        total_quantity: u32,
        filled_quantity: u32,
        broker_order_id: Option<String>,
        broker_child_orders: Vec<BrokerChildOrder>,
        residual_exposure: bool,
        state_version: u64,
        updated_at: DateTime<Utc>,
    ) -> Result<Self, ExecutionError> {
        if order_id.is_empty()
            || plan_id.is_empty()
            || plan_hash.len() != 64
            || idempotency_key.is_empty()
            || total_quantity == 0
            || filled_quantity > total_quantity
            || state_version == 0
        {
            return Err(ExecutionError::DuplicateConflict);
        }
        let broker_child_order_ids = broker_child_orders
            .iter()
            .map(|child| child.broker_order_id.clone())
            .collect();
        Ok(Self {
            order_id,
            plan_id,
            plan_hash,
            idempotency_key,
            state,
            expires_at,
            total_quantity,
            filled_quantity,
            broker_order_id,
            broker_child_order_ids,
            broker_child_orders,
            residual_exposure,
            confirmation_id: None,
            events: Vec::new(),
            base_state_version: state_version,
            restored_updated_at: Some(updated_at),
        })
    }

    pub fn proposed(
        order_id: String,
        plan_id: String,
        plan_hash: String,
        idempotency_key: String,
        expires_at: DateTime<Utc>,
        total_quantity: u32,
    ) -> Result<Self, ExecutionError> {
        if total_quantity == 0 {
            return Err(ExecutionError::InvalidTransition);
        }
        Ok(Self {
            order_id,
            plan_id,
            plan_hash,
            idempotency_key,
            state: OrderState::Proposed,
            expires_at,
            total_quantity,
            filled_quantity: 0,
            broker_order_id: None,
            broker_child_order_ids: Vec::new(),
            broker_child_orders: Vec::new(),
            residual_exposure: false,
            confirmation_id: None,
            events: Vec::new(),
            base_state_version: 0,
            restored_updated_at: None,
        })
    }

    pub fn state_version(&self) -> u64 {
        self.base_state_version + self.events.len() as u64
    }

    pub fn updated_at(&self, fallback: DateTime<Utc>) -> DateTime<Utc> {
        self.events.last().map_or_else(
            || self.restored_updated_at.unwrap_or(fallback),
            |event| event.occurred_at,
        )
    }

    fn transition(
        &mut self,
        to: OrderState,
        kind: &'static str,
        at: DateTime<Utc>,
    ) -> Result<(), ExecutionError> {
        if !allowed(self.state, to) {
            return Err(ExecutionError::InvalidTransition);
        }
        let from = self.state;
        self.state = to;
        self.events.push(OrderEvent {
            sequence: self.state_version() + 1,
            from,
            to,
            kind,
            occurred_at: at,
        });
        Ok(())
    }

    pub fn initial_risk(
        &mut self,
        approved: bool,
        at: DateTime<Utc>,
    ) -> Result<(), ExecutionError> {
        self.transition(
            if approved {
                OrderState::AwaitingConfirmation
            } else {
                OrderState::RiskRejected
            },
            if approved {
                "INITIAL_RISK_APPROVED"
            } else {
                "INITIAL_RISK_REJECTED"
            },
            at,
        )
    }

    pub fn confirm(
        &mut self,
        confirmation_id: String,
        confirmed_plan_hash: &str,
        at: DateTime<Utc>,
    ) -> Result<(), ExecutionError> {
        if at >= self.expires_at {
            self.transition(OrderState::Expired, "CONFIRMATION_EXPIRED", at)?;
            return Err(ExecutionError::ConfirmationExpired);
        }
        if confirmed_plan_hash != self.plan_hash {
            return Err(ExecutionError::ConfirmationHashMismatch);
        }
        if confirmation_id.is_empty() {
            return Err(ExecutionError::InvalidTransition);
        }
        self.confirmation_id = Some(confirmation_id);
        self.transition(OrderState::Approved, "USER_CONFIRMED", at)
    }

    pub fn begin_submit(&mut self, at: DateTime<Utc>) -> Result<(), ExecutionError> {
        if at >= self.expires_at {
            self.transition(OrderState::Expired, "PLAN_EXPIRED_BEFORE_SUBMIT", at)?;
            return Err(ExecutionError::ConfirmationExpired);
        }
        self.transition(OrderState::Submitting, "FINAL_RISK_APPROVED", at)
    }

    pub fn final_risk_rejected(&mut self, at: DateTime<Utc>) -> Result<(), ExecutionError> {
        self.transition(OrderState::RiskRejected, "FINAL_RISK_REJECTED", at)
    }

    pub fn submission_rejected(&mut self, at: DateTime<Utc>) -> Result<(), ExecutionError> {
        self.transition(OrderState::Rejected, "SUBMISSION_REJECTED", at)
    }

    pub fn cancel_unsubmitted(&mut self, at: DateTime<Utc>) -> Result<(), ExecutionError> {
        self.transition(OrderState::Cancelled, "CANCELLED_BEFORE_SUBMIT", at)
    }

    pub fn complete_shadow(&mut self, at: DateTime<Utc>) -> Result<(), ExecutionError> {
        self.transition(OrderState::Shadowed, "SHADOW_RECORDED", at)
    }

    pub fn apply_broker_order(
        &mut self,
        order: &BrokerOrder,
        at: DateTime<Utc>,
    ) -> Result<(), ExecutionError> {
        if let Some(existing) = self.broker_order_id.as_deref() {
            if existing != order.broker_order_id {
                return Err(ExecutionError::DuplicateConflict);
            }
        }
        if order.plan_hash != self.plan_hash
            || order.idempotency_key != self.idempotency_key
            || order.total_quantity != self.total_quantity
            || order.filled_quantity > order.total_quantity
            || order.filled_quantity < self.filled_quantity
        {
            return Err(ExecutionError::DuplicateConflict);
        }
        if !child_projection_valid(order) {
            return Err(ExecutionError::DuplicateConflict);
        }
        let previous_filled_quantity = self.filled_quantity;
        let previous_child_orders = self.broker_child_orders.clone();
        let previous_residual_exposure = self.residual_exposure;
        let (target, kind) = match order.status {
            BrokerOrderStatus::Working => (OrderState::Working, "BROKER_WORKING"),
            BrokerOrderStatus::PartialFill => (OrderState::PartialFill, "BROKER_PARTIAL_FILL"),
            BrokerOrderStatus::Filled => (OrderState::Filled, "BROKER_FILLED"),
            BrokerOrderStatus::Cancelled => (OrderState::Cancelled, "BROKER_CANCELLED"),
            BrokerOrderStatus::Rejected => (OrderState::Rejected, "BROKER_REJECTED"),
            BrokerOrderStatus::ReconcilePending => {
                (OrderState::ReconcilePending, "BROKER_RECONCILE_PENDING")
            }
        };
        if self.state == target
            && order.filled_quantity != previous_filled_quantity
            && target != OrderState::PartialFill
        {
            return Err(ExecutionError::DuplicateConflict);
        }
        self.broker_order_id = Some(order.broker_order_id.clone());
        self.broker_child_order_ids = order
            .child_orders
            .iter()
            .map(|child| child.broker_order_id.clone())
            .collect();
        self.broker_child_orders = order.child_orders.clone();
        self.residual_exposure = order.residual_exposure;
        self.filled_quantity = order.filled_quantity;
        if self.state == target {
            if order.filled_quantity == previous_filled_quantity
                && order.child_orders == previous_child_orders
                && order.residual_exposure == previous_residual_exposure
            {
                return Ok(());
            }
            self.events.push(OrderEvent {
                sequence: self.state_version() + 1,
                from: target,
                to: target,
                kind: if order.filled_quantity != previous_filled_quantity {
                    "BROKER_PARTIAL_FILL_PROGRESS"
                } else {
                    "BROKER_CHILD_PROJECTION_PROGRESS"
                },
                occurred_at: at,
            });
            return Ok(());
        }
        self.transition(target, kind, at)
    }

    pub fn request_cancel(&mut self, at: DateTime<Utc>) -> Result<(), ExecutionError> {
        self.transition(OrderState::CancelPending, "CANCEL_REQUESTED", at)
    }

    pub fn broker_disconnected(&mut self, at: DateTime<Utc>) -> Result<(), ExecutionError> {
        if self.state.is_terminal() {
            return Ok(());
        }
        if self.broker_order_id.is_none()
            && matches!(
                self.state,
                OrderState::Submitting
                    | OrderState::Working
                    | OrderState::PartialFill
                    | OrderState::CancelPending
            )
        {
            // The request may have crossed the broker boundary before the
            // response was lost. Treat a fill as possible until account-level
            // reconciliation proves otherwise.
            self.residual_exposure = true;
        }
        self.transition(OrderState::ReconcilePending, "BROKER_DISCONNECTED", at)
    }
}

fn child_projection_valid(order: &BrokerOrder) -> bool {
    if order.status == BrokerOrderStatus::PartialFill && !order.residual_exposure {
        return false;
    }
    if order.status == BrokerOrderStatus::Filled && order.residual_exposure {
        return false;
    }
    if order.child_orders.is_empty() {
        // A split submit can become ambiguous before a broker child id is
        // returned. In that case residual=true is deliberately allowed.
        return true;
    }
    let mut ids = BTreeSet::new();
    let mut leg_indices = BTreeSet::new();
    for child in &order.child_orders {
        let Some(leg) = order.legs.get(child.leg_index) else {
            return false;
        };
        if !ids.insert(child.broker_order_id.as_str())
            || !leg_indices.insert(child.leg_index)
            || child.contract_id != leg.contract_id
            || child.side != leg.side
            || child.quantity != leg.quantity
            || child.filled_quantity > child.quantity
            || (child.status == BrokerOrderStatus::Filled
                && child.filled_quantity != child.quantity)
            || (child.status == BrokerOrderStatus::PartialFill
                && !(0 < child.filled_quantity && child.filled_quantity < child.quantity))
            || (child.status == BrokerOrderStatus::Rejected && child.filled_quantity != 0)
        {
            return false;
        }
    }
    let all_filled = order.child_orders.len() == order.legs.len()
        && order.child_orders.iter().all(|child| {
            child.status == BrokerOrderStatus::Filled && child.filled_quantity == child.quantity
        });
    let possible_fill = order.child_orders.iter().any(|child| {
        matches!(
            child.status,
            BrokerOrderStatus::Working | BrokerOrderStatus::ReconcilePending
        )
    });
    let incomplete_fill = order
        .child_orders
        .iter()
        .any(|child| child.filled_quantity > 0)
        && !all_filled;
    order.residual_exposure == (possible_fill || incomplete_fill)
}

fn allowed(from: OrderState, to: OrderState) -> bool {
    use OrderState::*;
    matches!(
        (from, to),
        (Proposed, RiskRejected | AwaitingConfirmation)
            | (
                AwaitingConfirmation,
                Approved | RiskRejected | Expired | Cancelled
            )
            | (Approved, Submitting | Shadowed | Expired)
            | (
                Submitting,
                Working | PartialFill | Filled | Rejected | ReconcilePending
            )
            | (
                Working,
                PartialFill | Filled | CancelPending | Cancelled | Rejected | ReconcilePending
            )
            | (
                PartialFill,
                PartialFill | Filled | CancelPending | Cancelled | ReconcilePending
            )
            | (
                CancelPending,
                PartialFill | Filled | Cancelled | ReconcilePending
            )
            | (
                ReconcilePending,
                Working | PartialFill | Filled | Cancelled | Rejected
            )
    )
}

/// Process-local mirror of the persisted unique idempotency constraint. The
/// database and broker must enforce the same key for restart safety.
#[derive(Debug, Default)]
pub struct IdempotencyRegistry {
    entries: BTreeMap<String, (String, String)>,
}

impl IdempotencyRegistry {
    pub fn reserve(
        &mut self,
        key: &str,
        plan_hash: &str,
        order_id: &str,
    ) -> Result<Option<String>, ExecutionError> {
        if let Some((existing_hash, existing_order)) = self.entries.get(key) {
            if existing_hash != plan_hash {
                return Err(ExecutionError::DuplicateConflict);
            }
            return Ok(Some(existing_order.clone()));
        }
        self.entries
            .insert(key.to_owned(), (plan_hash.to_owned(), order_id.to_owned()));
        Ok(None)
    }
}

pub fn submit_to_broker<A: BrokerAdapter>(
    record: &mut OrderRecord,
    adapter: &mut A,
    side: OrderSide,
    order_type: BrokerOrderType,
    submitted_price: Option<Decimal>,
    legs: Vec<BrokerOrderLeg>,
    at: DateTime<Utc>,
) -> Result<(), ExecutionError> {
    let order = adapter
        .submit(BrokerOrderRequest {
            idempotency_key: record.idempotency_key.clone(),
            plan_hash: record.plan_hash.clone(),
            side,
            order_type,
            total_quantity: record.total_quantity,
            submitted_price,
            legs,
        })
        .map_err(ExecutionError::Broker)?;
    record.apply_broker_order(&order, at)
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker::{BrokerHealth, BrokerId, OrderSide, PaperBroker};
    use chrono::Duration;

    fn now() -> DateTime<Utc> {
        "2026-07-21T14:30:00Z".parse().unwrap()
    }

    fn record() -> OrderRecord {
        OrderRecord::proposed(
            "order-1".into(),
            "plan-1".into(),
            "a".repeat(64),
            "key-1".into(),
            now() + Duration::minutes(1),
            2,
        )
        .unwrap()
    }

    fn legs() -> Vec<BrokerOrderLeg> {
        vec![BrokerOrderLeg {
            contract_id: "QQQ-20260721-C-500".into(),
            side: OrderSide::Buy,
            quantity: 2,
            broker_contract_id: None,
            symbol: Some("QQQ".into()),
            exchange: None,
            submitted_price: Some(Decimal::new(250, 2)),
        }]
    }

    #[test]
    fn confirmation_and_submit_are_strict_and_idempotent() {
        let mut record = record();
        record.initial_risk(true, now()).unwrap();
        record
            .confirm("confirm-1".into(), &"a".repeat(64), now())
            .unwrap();
        record.begin_submit(now()).unwrap();
        let mut broker = PaperBroker::new(BrokerId::Ibkr);
        submit_to_broker(
            &mut record,
            &mut broker,
            OrderSide::Buy,
            BrokerOrderType::Limit,
            Some(Decimal::new(250, 2)),
            legs(),
            now(),
        )
        .unwrap();
        assert_eq!(record.state, OrderState::Working);
        assert_eq!(broker.orders().len(), 1);
        // Broker idempotency returns the same order; applying it is a no-op.
        submit_to_broker(
            &mut record,
            &mut broker,
            OrderSide::Buy,
            BrokerOrderType::Limit,
            Some(Decimal::new(250, 2)),
            legs(),
            now(),
        )
        .unwrap();
        assert_eq!(broker.orders().len(), 1);
    }

    #[test]
    fn wrong_hash_and_expired_confirmation_never_approve() {
        let mut mismatch = record();
        mismatch.initial_risk(true, now()).unwrap();
        assert_eq!(
            mismatch.confirm("confirm-1".into(), "b", now()),
            Err(ExecutionError::ConfirmationHashMismatch)
        );
        assert_eq!(mismatch.state, OrderState::AwaitingConfirmation);

        let mut expired = record();
        expired.initial_risk(true, now()).unwrap();
        let result = expired.confirm(
            "confirm-1".into(),
            &"a".repeat(64),
            now() + Duration::minutes(1),
        );
        assert_eq!(result, Err(ExecutionError::ConfirmationExpired));
        assert_eq!(expired.state, OrderState::Expired);
    }

    #[test]
    fn partial_fill_cancel_and_disconnect_reconcile_follow_broker_truth() {
        let mut record = record();
        record.initial_risk(true, now()).unwrap();
        record
            .confirm("confirm-1".into(), &"a".repeat(64), now())
            .unwrap();
        record.begin_submit(now()).unwrap();
        let mut broker = PaperBroker::new(BrokerId::Longbridge);
        submit_to_broker(
            &mut record,
            &mut broker,
            OrderSide::Buy,
            BrokerOrderType::Limit,
            Some(Decimal::ONE),
            legs(),
            now(),
        )
        .unwrap();
        let broker_id = record.broker_order_id.clone().unwrap();
        let partial = broker.apply_fill(&broker_id, 1, Decimal::ONE).unwrap();
        record.apply_broker_order(&partial, now()).unwrap();
        assert_eq!(record.state, OrderState::PartialFill);

        record.broker_disconnected(now()).unwrap();
        assert_eq!(record.state, OrderState::ReconcilePending);
        broker.set_connection(BrokerHealth::Healthy, false);
        broker.reconcile().unwrap();
        let reconciled = broker.orders().into_iter().next().unwrap();
        record.apply_broker_order(&reconciled, now()).unwrap();
        assert_eq!(record.state, OrderState::PartialFill);

        record.request_cancel(now()).unwrap();
        let cancelled = broker.cancel(&broker_id).unwrap();
        record.apply_broker_order(&cancelled, now()).unwrap();
        assert_eq!(record.state, OrderState::Cancelled);
        assert_eq!(record.filled_quantity, 1);
    }

    #[test]
    fn repeated_partial_fill_progress_increments_version_and_never_regresses() {
        let mut record = OrderRecord::proposed(
            "order-progress".into(),
            "plan-progress".into(),
            "a".repeat(64),
            "key-progress".into(),
            now() + Duration::minutes(1),
            3,
        )
        .unwrap();
        record.initial_risk(true, now()).unwrap();
        record
            .confirm("confirm-progress".into(), &"a".repeat(64), now())
            .unwrap();
        record.begin_submit(now()).unwrap();
        let partial = BrokerOrder {
            broker_order_id: "broker-progress".into(),
            idempotency_key: "key-progress".into(),
            plan_hash: "a".repeat(64),
            status: BrokerOrderStatus::PartialFill,
            side: OrderSide::Buy,
            order_type: BrokerOrderType::Limit,
            total_quantity: 3,
            filled_quantity: 1,
            submitted_price: Some(Decimal::ONE),
            legs: vec![BrokerOrderLeg {
                contract_id: "QQQ-20260721-C-500".into(),
                side: OrderSide::Buy,
                quantity: 3,
                broker_contract_id: None,
                symbol: Some("QQQ".into()),
                exchange: None,
                submitted_price: Some(Decimal::ONE),
            }],
            child_orders: Vec::new(),
            residual_exposure: true,
        };
        record.apply_broker_order(&partial, now()).unwrap();
        let first_version = record.events.len();

        let progressed = BrokerOrder {
            filled_quantity: 2,
            ..partial.clone()
        };
        record
            .apply_broker_order(&progressed, now() + Duration::seconds(1))
            .unwrap();
        assert_eq!(record.events.len(), first_version + 1);
        assert_eq!(record.filled_quantity, 2);
        assert_eq!(
            record.events.last().unwrap().kind,
            "BROKER_PARTIAL_FILL_PROGRESS"
        );

        assert_eq!(
            record.apply_broker_order(&partial, now() + Duration::seconds(2)),
            Err(ExecutionError::DuplicateConflict)
        );
        assert_eq!(record.filled_quantity, 2);
    }

    #[test]
    fn child_projection_cannot_hide_possible_residual_exposure() {
        let mut record = OrderRecord::proposed(
            "order-child".into(),
            "plan-child".into(),
            "a".repeat(64),
            "key-child".into(),
            now() + Duration::minutes(1),
            1,
        )
        .unwrap();
        record.initial_risk(true, now()).unwrap();
        record
            .confirm("confirm-child".into(), &"a".repeat(64), now())
            .unwrap();
        record.begin_submit(now()).unwrap();
        let leg = BrokerOrderLeg {
            contract_id: "QQQ-20260721-C-500".into(),
            side: OrderSide::Buy,
            quantity: 1,
            broker_contract_id: None,
            symbol: Some("QQQ".into()),
            exchange: None,
            submitted_price: Some(Decimal::ONE),
        };
        let hidden = BrokerOrder {
            broker_order_id: "broker-child".into(),
            idempotency_key: "key-child".into(),
            plan_hash: "a".repeat(64),
            status: BrokerOrderStatus::Working,
            side: OrderSide::Buy,
            order_type: BrokerOrderType::Limit,
            total_quantity: 1,
            filled_quantity: 0,
            submitted_price: Some(Decimal::ONE),
            legs: vec![leg.clone()],
            child_orders: vec![BrokerChildOrder {
                broker_order_id: "child-1".into(),
                leg_index: 0,
                contract_id: leg.contract_id,
                side: leg.side,
                quantity: 1,
                filled_quantity: 0,
                status: BrokerOrderStatus::Working,
                submitted_price: leg.submitted_price,
            }],
            residual_exposure: false,
        };
        assert_eq!(
            record.apply_broker_order(&hidden, now()),
            Err(ExecutionError::DuplicateConflict)
        );
        assert!(record.broker_child_orders.is_empty());
    }

    #[test]
    fn idempotency_key_conflict_fails_closed() {
        let mut registry = IdempotencyRegistry::default();
        assert_eq!(registry.reserve("key", "a", "order-1"), Ok(None));
        assert_eq!(
            registry.reserve("key", "a", "order-2"),
            Ok(Some("order-1".into()))
        );
        assert_eq!(
            registry.reserve("key", "b", "order-3"),
            Err(ExecutionError::DuplicateConflict)
        );
    }

    #[test]
    fn unknown_submit_outcome_keeps_possible_exposure_visible() {
        let mut record = record();
        record.initial_risk(true, now()).unwrap();
        record
            .confirm("confirm-unknown".into(), &"a".repeat(64), now())
            .unwrap();
        record.begin_submit(now()).unwrap();
        record.broker_disconnected(now()).unwrap();
        assert_eq!(record.state, OrderState::ReconcilePending);
        assert!(record.residual_exposure);
        assert!(record.broker_order_id.is_none());
    }

    #[test]
    fn invalid_transition_is_rejected() {
        let mut record = record();
        assert_eq!(
            record.request_cancel(now()),
            Err(ExecutionError::InvalidTransition)
        );
        assert_eq!(record.state, OrderState::Proposed);
    }
}
