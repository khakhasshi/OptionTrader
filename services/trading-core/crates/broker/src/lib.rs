//! Broker boundary and deterministic paper adapter.
//!
//! Broker reports are the sole execution fact source. Live Longbridge/IBKR
//! adapters deliberately remain disconnected until account-specific paper
//! certification is complete; neither can accidentally submit a live order.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BrokerId {
    Longbridge,
    Ibkr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BrokerHealth {
    Healthy,
    Degraded,
    Disconnected,
    Reconciling,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerOrderStatus {
    Working,
    PartialFill,
    Filled,
    Cancelled,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountSnapshot {
    pub broker_id: BrokerId,
    pub health: BrokerHealth,
    pub reconciled: bool,
    pub buying_power: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionSnapshot {
    pub contract_id: String,
    pub quantity: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fill {
    pub fill_id: String,
    pub broker_order_id: String,
    pub quantity: u32,
    pub price: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerOrderLeg {
    pub contract_id: String,
    pub side: OrderSide,
    pub quantity: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerOrderRequest {
    pub idempotency_key: String,
    pub plan_hash: String,
    pub total_quantity: u32,
    pub limit_price: Decimal,
    pub legs: Vec<BrokerOrderLeg>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerOrder {
    pub broker_order_id: String,
    pub idempotency_key: String,
    pub plan_hash: String,
    pub status: BrokerOrderStatus,
    pub total_quantity: u32,
    pub filled_quantity: u32,
    pub limit_price: Decimal,
    pub legs: Vec<BrokerOrderLeg>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerError {
    Disconnected,
    NotReconciled,
    DuplicateConflict,
    InvalidQuantity,
    OrderNotFound,
    TerminalOrder,
    LiveSubmissionDisabled,
}

/// Contract implemented by paper and, after certification, live adapters.
pub trait BrokerAdapter {
    fn broker_id(&self) -> BrokerId;
    fn account(&self) -> AccountSnapshot;
    fn positions(&self) -> Vec<PositionSnapshot>;
    fn orders(&self) -> Vec<BrokerOrder>;
    fn fills(&self) -> Vec<Fill>;
    fn submit(&mut self, request: BrokerOrderRequest) -> Result<BrokerOrder, BrokerError>;
    fn cancel(&mut self, broker_order_id: &str) -> Result<BrokerOrder, BrokerError>;
    fn reconcile(&mut self) -> Result<(), BrokerError>;
}

/// In-memory deterministic paper broker used by Phase 3 tests and shadow/paper
/// workflows. Idempotency behavior mirrors the required live adapter contract.
#[derive(Debug)]
pub struct PaperBroker {
    broker_id: BrokerId,
    account: AccountSnapshot,
    orders: BTreeMap<String, BrokerOrder>,
    order_by_key: BTreeMap<String, String>,
    fills: Vec<Fill>,
    next_order: u64,
    next_fill: u64,
}

impl PaperBroker {
    pub fn new(broker_id: BrokerId) -> Self {
        Self {
            broker_id,
            account: AccountSnapshot {
                broker_id,
                health: BrokerHealth::Healthy,
                reconciled: true,
                buying_power: Decimal::new(100_000, 0),
            },
            orders: BTreeMap::new(),
            order_by_key: BTreeMap::new(),
            fills: Vec::new(),
            next_order: 1,
            next_fill: 1,
        }
    }

    pub fn set_connection(&mut self, health: BrokerHealth, reconciled: bool) {
        self.account.health = health;
        self.account.reconciled = reconciled;
    }

    pub fn apply_fill(
        &mut self,
        broker_order_id: &str,
        quantity: u32,
        price: Decimal,
    ) -> Result<BrokerOrder, BrokerError> {
        let order = self
            .orders
            .get_mut(broker_order_id)
            .ok_or(BrokerError::OrderNotFound)?;
        if matches!(
            order.status,
            BrokerOrderStatus::Filled | BrokerOrderStatus::Cancelled | BrokerOrderStatus::Rejected
        ) {
            return Err(BrokerError::TerminalOrder);
        }
        if quantity == 0 || order.filled_quantity.saturating_add(quantity) > order.total_quantity {
            return Err(BrokerError::InvalidQuantity);
        }
        order.filled_quantity += quantity;
        order.status = if order.filled_quantity == order.total_quantity {
            BrokerOrderStatus::Filled
        } else {
            BrokerOrderStatus::PartialFill
        };
        let fill = Fill {
            fill_id: format!("paper-fill-{}", self.next_fill),
            broker_order_id: broker_order_id.to_owned(),
            quantity,
            price,
        };
        self.next_fill += 1;
        self.fills.push(fill);
        Ok(order.clone())
    }

    pub fn reject(&mut self, broker_order_id: &str) -> Result<BrokerOrder, BrokerError> {
        let order = self
            .orders
            .get_mut(broker_order_id)
            .ok_or(BrokerError::OrderNotFound)?;
        if order.filled_quantity > 0 || order.status != BrokerOrderStatus::Working {
            return Err(BrokerError::TerminalOrder);
        }
        order.status = BrokerOrderStatus::Rejected;
        Ok(order.clone())
    }
}

impl BrokerAdapter for PaperBroker {
    fn broker_id(&self) -> BrokerId {
        self.broker_id
    }

    fn account(&self) -> AccountSnapshot {
        self.account.clone()
    }

    fn positions(&self) -> Vec<PositionSnapshot> {
        Vec::new()
    }

    fn orders(&self) -> Vec<BrokerOrder> {
        self.orders.values().cloned().collect()
    }

    fn fills(&self) -> Vec<Fill> {
        self.fills.clone()
    }

    fn submit(&mut self, request: BrokerOrderRequest) -> Result<BrokerOrder, BrokerError> {
        if self.account.health != BrokerHealth::Healthy {
            return Err(BrokerError::Disconnected);
        }
        if !self.account.reconciled {
            return Err(BrokerError::NotReconciled);
        }
        let contracts: BTreeSet<&str> = request
            .legs
            .iter()
            .map(|leg| leg.contract_id.as_str())
            .collect();
        if request.total_quantity == 0
            || request.limit_price <= Decimal::ZERO
            || request.legs.is_empty()
            || request.legs.len() > 4
            || contracts.len() != request.legs.len()
            || request.legs.iter().any(|leg| {
                leg.contract_id.is_empty()
                    || leg.quantity == 0
                    || leg.quantity != request.total_quantity
            })
        {
            return Err(BrokerError::InvalidQuantity);
        }
        if let Some(order_id) = self.order_by_key.get(&request.idempotency_key) {
            let existing = self
                .orders
                .get(order_id)
                .expect("idempotency index references an order");
            if existing.plan_hash != request.plan_hash
                || existing.total_quantity != request.total_quantity
                || existing.limit_price != request.limit_price
                || existing.legs != request.legs
            {
                return Err(BrokerError::DuplicateConflict);
            }
            return Ok(existing.clone());
        }
        let broker_order_id = format!("paper-order-{}", self.next_order);
        self.next_order += 1;
        let order = BrokerOrder {
            broker_order_id: broker_order_id.clone(),
            idempotency_key: request.idempotency_key.clone(),
            plan_hash: request.plan_hash,
            status: BrokerOrderStatus::Working,
            total_quantity: request.total_quantity,
            filled_quantity: 0,
            limit_price: request.limit_price,
            legs: request.legs,
        };
        self.order_by_key
            .insert(request.idempotency_key, broker_order_id.clone());
        self.orders.insert(broker_order_id, order.clone());
        Ok(order)
    }

    fn cancel(&mut self, broker_order_id: &str) -> Result<BrokerOrder, BrokerError> {
        if self.account.health != BrokerHealth::Healthy {
            return Err(BrokerError::Disconnected);
        }
        let order = self
            .orders
            .get_mut(broker_order_id)
            .ok_or(BrokerError::OrderNotFound)?;
        if matches!(
            order.status,
            BrokerOrderStatus::Filled | BrokerOrderStatus::Cancelled | BrokerOrderStatus::Rejected
        ) {
            return Err(BrokerError::TerminalOrder);
        }
        order.status = BrokerOrderStatus::Cancelled;
        Ok(order.clone())
    }

    fn reconcile(&mut self) -> Result<(), BrokerError> {
        if self.account.health == BrokerHealth::Disconnected {
            return Err(BrokerError::Disconnected);
        }
        self.account.reconciled = true;
        self.account.health = BrokerHealth::Healthy;
        Ok(())
    }
}

/// Explicitly disabled live adapter boundary. It exposes health/account reads
/// but can never submit or cancel while Phase 3 live trading remains off.
#[derive(Debug)]
pub struct DisabledLiveBroker {
    broker_id: BrokerId,
}

impl DisabledLiveBroker {
    pub fn longbridge() -> Self {
        Self {
            broker_id: BrokerId::Longbridge,
        }
    }

    pub fn ibkr() -> Self {
        Self {
            broker_id: BrokerId::Ibkr,
        }
    }
}

impl BrokerAdapter for DisabledLiveBroker {
    fn broker_id(&self) -> BrokerId {
        self.broker_id
    }

    fn account(&self) -> AccountSnapshot {
        AccountSnapshot {
            broker_id: self.broker_id,
            health: BrokerHealth::Disconnected,
            reconciled: false,
            buying_power: Decimal::ZERO,
        }
    }

    fn positions(&self) -> Vec<PositionSnapshot> {
        Vec::new()
    }

    fn orders(&self) -> Vec<BrokerOrder> {
        Vec::new()
    }

    fn fills(&self) -> Vec<Fill> {
        Vec::new()
    }

    fn submit(&mut self, _request: BrokerOrderRequest) -> Result<BrokerOrder, BrokerError> {
        Err(BrokerError::LiveSubmissionDisabled)
    }

    fn cancel(&mut self, _broker_order_id: &str) -> Result<BrokerOrder, BrokerError> {
        Err(BrokerError::LiveSubmissionDisabled)
    }

    fn reconcile(&mut self) -> Result<(), BrokerError> {
        Err(BrokerError::LiveSubmissionDisabled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(key: &str, hash: &str) -> BrokerOrderRequest {
        BrokerOrderRequest {
            idempotency_key: key.into(),
            plan_hash: hash.into(),
            total_quantity: 2,
            limit_price: Decimal::new(125, 2),
            legs: vec![
                BrokerOrderLeg {
                    contract_id: "QQQ-20260721-C-500".into(),
                    side: OrderSide::Sell,
                    quantity: 2,
                },
                BrokerOrderLeg {
                    contract_id: "QQQ-20260721-C-501".into(),
                    side: OrderSide::Buy,
                    quantity: 2,
                },
            ],
        }
    }

    #[test]
    fn repeated_submit_returns_one_broker_order_and_conflict_is_rejected() {
        let mut broker = PaperBroker::new(BrokerId::Ibkr);
        let first = broker.submit(request("key-1", "a")).unwrap();
        let again = broker.submit(request("key-1", "a")).unwrap();
        assert_eq!(first.broker_order_id, again.broker_order_id);
        assert_eq!(broker.orders().len(), 1);
        assert_eq!(
            broker.submit(request("key-1", "b")),
            Err(BrokerError::DuplicateConflict)
        );
    }

    #[test]
    fn partial_fill_then_cancel_preserves_filled_quantity() {
        let mut broker = PaperBroker::new(BrokerId::Longbridge);
        let order = broker.submit(request("key-1", "a")).unwrap();
        let partial = broker
            .apply_fill(&order.broker_order_id, 1, Decimal::new(130, 2))
            .unwrap();
        assert_eq!(partial.status, BrokerOrderStatus::PartialFill);
        let cancelled = broker.cancel(&order.broker_order_id).unwrap();
        assert_eq!(cancelled.status, BrokerOrderStatus::Cancelled);
        assert_eq!(cancelled.filled_quantity, 1);
        assert_eq!(broker.fills().len(), 1);
    }

    #[test]
    fn disconnected_and_disabled_live_adapters_fail_closed() {
        let mut paper = PaperBroker::new(BrokerId::Ibkr);
        paper.set_connection(BrokerHealth::Disconnected, false);
        assert_eq!(
            paper.submit(request("key-1", "a")),
            Err(BrokerError::Disconnected)
        );
        let mut live = DisabledLiveBroker::longbridge();
        assert_eq!(
            live.submit(request("key-1", "a")),
            Err(BrokerError::LiveSubmissionDisabled)
        );
    }

    #[test]
    fn paper_adapter_rejects_missing_duplicate_or_mismatched_legs() {
        let mut broker = PaperBroker::new(BrokerId::Ibkr);
        let mut missing = request("missing", "a");
        missing.legs.clear();
        assert_eq!(broker.submit(missing), Err(BrokerError::InvalidQuantity));

        let mut duplicate = request("duplicate", "b");
        duplicate.legs[1].contract_id = duplicate.legs[0].contract_id.clone();
        assert_eq!(broker.submit(duplicate), Err(BrokerError::InvalidQuantity));

        let mut mismatch = request("mismatch", "c");
        mismatch.legs[1].quantity = 1;
        assert_eq!(broker.submit(mismatch), Err(BrokerError::InvalidQuantity));

        let first = broker.submit(request("semantic-key", "same-hash")).unwrap();
        let mut changed = request("semantic-key", "same-hash");
        changed.limit_price = Decimal::new(126, 2);
        assert_eq!(broker.submit(changed), Err(BrokerError::DuplicateConflict));
        assert_eq!(
            broker.orders().first().unwrap().broker_order_id,
            first.broker_order_id
        );
    }
}
