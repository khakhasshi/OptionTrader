//! Native Longbridge Rust SDK adapter.
//!
//! Longbridge currently exposes only single-leg option orders. Multi-leg
//! requests are rejected here instead of being decomposed into legging risk.

use std::{collections::BTreeMap, sync::Arc};

use longbridge::{
    blocking::TradeContextSync,
    trade::{
        GetTodayOrdersOptions, Order as LbOrder, OrderSide as LbSide, OrderStatus as LbStatus,
        OrderType as LbOrderType, SubmitOrderOptions, TimeInForceType,
    },
    Config,
};
use rust_decimal::{prelude::ToPrimitive, Decimal};

use crate::{
    AccountSnapshot, BrokerAdapter, BrokerError, BrokerHealth, BrokerId, BrokerOrder,
    BrokerOrderRequest, BrokerOrderStatus, BrokerOrderType, Fill, OrderSide, PositionSnapshot,
};

pub struct LongbridgeBroker {
    context: TradeContextSync,
    submission_enabled: bool,
    account: AccountSnapshot,
    positions: Vec<PositionSnapshot>,
    orders: BTreeMap<String, BrokerOrder>,
    order_by_key: BTreeMap<String, String>,
}

impl LongbridgeBroker {
    /// Credentials are read by the official SDK from LONGBRIDGE_* (or the
    /// legacy LONGPORT_*) environment variables and are never copied into a
    /// request, log record, or protobuf payload.
    pub fn from_env(submission_enabled: bool) -> Result<Self, BrokerError> {
        let config = Config::from_apikey_env().map_err(|_| BrokerError::Disconnected)?;
        Ok(Self {
            context: TradeContextSync::new(Arc::new(config), |_| {}),
            submission_enabled,
            account: disconnected_account(),
            positions: Vec::new(),
            orders: BTreeMap::new(),
            order_by_key: BTreeMap::new(),
        })
    }

    fn mark_disconnected(&mut self) {
        self.account.health = BrokerHealth::Disconnected;
        self.account.reconciled = false;
    }

    fn remote_order(&self, order_id: &str) -> Result<LbOrder, BrokerError> {
        let options = GetTodayOrdersOptions::new().order_id(order_id.to_owned());
        self.context
            .today_orders(options)
            .map_err(|_| BrokerError::Disconnected)?
            .into_iter()
            .find(|order| order.order_id == order_id)
            .ok_or(BrokerError::OrderNotFound)
    }

    fn update_known_order(&mut self, order_id: &str) -> Result<BrokerOrder, BrokerError> {
        let remote = self.remote_order(order_id)?;
        let known = self
            .orders
            .get_mut(order_id)
            .ok_or(BrokerError::OrderNotFound)?;
        known.status = map_status(remote.status)?;
        known.filled_quantity = decimal_quantity(remote.executed_quantity)?;
        Ok(known.clone())
    }
}

impl BrokerAdapter for LongbridgeBroker {
    fn broker_id(&self) -> BrokerId {
        BrokerId::Longbridge
    }

    fn account(&self) -> AccountSnapshot {
        self.account.clone()
    }

    fn positions(&self) -> Vec<PositionSnapshot> {
        self.positions.clone()
    }

    fn orders(&self) -> Vec<BrokerOrder> {
        self.orders.values().cloned().collect()
    }

    fn fills(&self) -> Vec<Fill> {
        Vec::new()
    }

    fn submit(&mut self, request: BrokerOrderRequest) -> Result<BrokerOrder, BrokerError> {
        if !self.submission_enabled {
            return Err(BrokerError::LiveSubmissionDisabled);
        }
        if self.account.health != BrokerHealth::Healthy {
            return Err(BrokerError::Disconnected);
        }
        if !self.account.reconciled {
            return Err(BrokerError::NotReconciled);
        }
        validate_single_leg(&request)?;
        if let Some(order_id) = self.order_by_key.get(&request.idempotency_key) {
            let existing = self
                .orders
                .get(order_id)
                .ok_or(BrokerError::NotReconciled)?;
            if !same_request(existing, &request) {
                return Err(BrokerError::DuplicateConflict);
            }
            return Ok(existing.clone());
        }

        let leg = &request.legs[0];
        let native_symbol = leg
            .broker_contract_id
            .as_ref()
            .ok_or(BrokerError::UnsupportedOrderShape)?;
        let lb_type = match request.order_type {
            BrokerOrderType::Market => LbOrderType::MO,
            BrokerOrderType::Limit | BrokerOrderType::AdaptiveLimit => LbOrderType::LO,
        };
        let mut options = SubmitOrderOptions::new(
            native_symbol.clone(),
            lb_type,
            map_side(request.side),
            Decimal::from(request.total_quantity),
            TimeInForceType::Day,
        )
        .remark(format!("optiontrader:{}", &request.plan_hash[..16]));
        if let Some(price) = request.submitted_price {
            options = options.submitted_price(price);
        }
        let response = match self.context.submit_order(options) {
            Ok(response) => response,
            Err(_) => {
                self.mark_disconnected();
                return Err(BrokerError::Disconnected);
            }
        };
        let order = BrokerOrder {
            broker_order_id: response.order_id.clone(),
            idempotency_key: request.idempotency_key.clone(),
            plan_hash: request.plan_hash,
            status: BrokerOrderStatus::Working,
            side: request.side,
            order_type: request.order_type,
            total_quantity: request.total_quantity,
            filled_quantity: 0,
            submitted_price: request.submitted_price,
            legs: request.legs,
        };
        self.order_by_key
            .insert(request.idempotency_key, response.order_id.clone());
        self.orders.insert(response.order_id, order.clone());
        Ok(order)
    }

    fn cancel(&mut self, broker_order_id: &str) -> Result<BrokerOrder, BrokerError> {
        if !self.submission_enabled {
            return Err(BrokerError::LiveSubmissionDisabled);
        }
        if !self.orders.contains_key(broker_order_id) {
            return Err(BrokerError::OrderNotFound);
        }
        if self
            .context
            .cancel_order(broker_order_id.to_owned())
            .is_err()
        {
            self.mark_disconnected();
            return Err(BrokerError::Disconnected);
        }
        self.update_known_order(broker_order_id)
    }

    fn reconcile(&mut self) -> Result<(), BrokerError> {
        self.account.health = BrokerHealth::Reconciling;
        self.account.reconciled = false;
        let balances = match self.context.account_balance(Some("USD")) {
            Ok(value) => value,
            Err(_) => {
                self.mark_disconnected();
                return Err(BrokerError::Disconnected);
            }
        };
        if balances.len() != 1 {
            self.mark_disconnected();
            return Err(BrokerError::NotReconciled);
        }
        let remote_positions = self
            .context
            .stock_positions(None)
            .map_err(|_| BrokerError::Disconnected)?;
        let mut positions = Vec::new();
        for channel in remote_positions.channels {
            for position in channel.positions {
                let quantity = position
                    .quantity
                    .to_i32()
                    .ok_or(BrokerError::NotReconciled)?;
                positions.push(PositionSnapshot {
                    contract_id: position.symbol,
                    quantity,
                });
            }
        }
        let balance = &balances[0];
        self.account.buying_power = balance.buy_power;
        self.positions = positions;
        self.account.health = BrokerHealth::Healthy;
        self.account.reconciled = true;
        Ok(())
    }
}

fn disconnected_account() -> AccountSnapshot {
    AccountSnapshot {
        broker_id: BrokerId::Longbridge,
        health: BrokerHealth::Disconnected,
        reconciled: false,
        buying_power: Decimal::ZERO,
    }
}

fn validate_single_leg(request: &BrokerOrderRequest) -> Result<(), BrokerError> {
    if request.idempotency_key.is_empty() || request.plan_hash.len() != 64 {
        return Err(BrokerError::DuplicateConflict);
    }
    if request.legs.len() != 1 {
        return Err(BrokerError::UnsupportedOrderShape);
    }
    let leg = &request.legs[0];
    if leg.side != request.side
        || leg.quantity != request.total_quantity
        || request.total_quantity == 0
        || leg.contract_id.is_empty()
        || leg.broker_contract_id.as_deref().is_none_or(str::is_empty)
    {
        return Err(BrokerError::InvalidQuantity);
    }
    match (request.order_type, request.submitted_price) {
        (BrokerOrderType::Market, None) => Ok(()),
        (BrokerOrderType::Limit | BrokerOrderType::AdaptiveLimit, Some(price))
            if price > Decimal::ZERO =>
        {
            Ok(())
        }
        _ => Err(BrokerError::InvalidPrice),
    }
}

fn same_request(order: &BrokerOrder, request: &BrokerOrderRequest) -> bool {
    order.plan_hash == request.plan_hash
        && order.side == request.side
        && order.order_type == request.order_type
        && order.total_quantity == request.total_quantity
        && order.submitted_price == request.submitted_price
        && order.legs == request.legs
}

fn map_side(side: OrderSide) -> LbSide {
    match side {
        OrderSide::Buy => LbSide::Buy,
        OrderSide::Sell => LbSide::Sell,
    }
}

fn map_status(status: LbStatus) -> Result<BrokerOrderStatus, BrokerError> {
    match status {
        LbStatus::Filled => Ok(BrokerOrderStatus::Filled),
        LbStatus::PartialFilled | LbStatus::PartialWithdrawal => Ok(BrokerOrderStatus::PartialFill),
        LbStatus::Rejected | LbStatus::Expired => Ok(BrokerOrderStatus::Rejected),
        LbStatus::Canceled => Ok(BrokerOrderStatus::Cancelled),
        LbStatus::NotReported
        | LbStatus::ReplacedNotReported
        | LbStatus::ProtectedNotReported
        | LbStatus::VarietiesNotReported
        | LbStatus::WaitToNew
        | LbStatus::New
        | LbStatus::WaitToReplace
        | LbStatus::PendingReplace
        | LbStatus::Replaced
        | LbStatus::WaitToCancel
        | LbStatus::PendingCancel => Ok(BrokerOrderStatus::Working),
        LbStatus::Unknown => Err(BrokerError::NotReconciled),
    }
}

fn decimal_quantity(value: Decimal) -> Result<u32, BrokerError> {
    if value.fract() != Decimal::ZERO {
        return Err(BrokerError::NotReconciled);
    }
    value.to_u32().ok_or(BrokerError::NotReconciled)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BrokerOrderLeg;

    fn request(legs: usize, order_type: BrokerOrderType) -> BrokerOrderRequest {
        BrokerOrderRequest {
            idempotency_key: "key".into(),
            plan_hash: "a".repeat(64),
            side: OrderSide::Buy,
            order_type,
            total_quantity: 1,
            submitted_price: (order_type != BrokerOrderType::Market)
                .then_some(Decimal::new(125, 2)),
            legs: (0..legs)
                .map(|index| BrokerOrderLeg {
                    contract_id: format!("QQQ260721C00{}00000.US", 500 + index),
                    side: OrderSide::Buy,
                    quantity: 1,
                    broker_contract_id: Some(format!("QQQ260721C00{}00000.US", 500 + index)),
                    symbol: Some("QQQ".into()),
                    exchange: None,
                })
                .collect(),
        }
    }

    #[test]
    fn longbridge_rejects_multi_leg_instead_of_legging() {
        assert_eq!(
            validate_single_leg(&request(2, BrokerOrderType::Limit)),
            Err(BrokerError::UnsupportedOrderShape)
        );
    }

    #[test]
    fn market_has_no_price_and_limit_variants_require_one() {
        assert_eq!(
            validate_single_leg(&request(1, BrokerOrderType::Market)),
            Ok(())
        );
        let mut invalid = request(1, BrokerOrderType::AdaptiveLimit);
        invalid.submitted_price = None;
        assert_eq!(
            validate_single_leg(&invalid),
            Err(BrokerError::InvalidPrice)
        );
    }
}
