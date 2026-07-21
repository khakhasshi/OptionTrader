//! Native Longbridge Rust SDK adapter.
//!
//! Longbridge exposes single-leg option orders. A defined-risk package is
//! executed as a guarded sequence: all BUY protection legs must be confirmed
//! filled before any SELL leg can be submitted.

use std::{
    collections::BTreeMap,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

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
    execute_buy_first, AccountSnapshot, BrokerAdapter, BrokerChildOrder, BrokerError, BrokerHealth,
    BrokerId, BrokerOrder, BrokerOrderLeg, BrokerOrderRequest, BrokerOrderStatus, BrokerOrderType,
    Fill, OrderSide, PositionSnapshot, SequentialExecutionConfig, SequentialLegGateway,
};

pub struct LongbridgeBroker {
    context: TradeContextSync,
    submission_enabled: bool,
    account: AccountSnapshot,
    positions: Vec<PositionSnapshot>,
    orders: BTreeMap<String, BrokerOrder>,
    order_by_key: BTreeMap<String, String>,
    sequential_config: SequentialExecutionConfig,
}

impl LongbridgeBroker {
    /// Credentials are read by the official SDK from LONGBRIDGE_* (or legacy
    /// LONGPORT_*) environment variables and never enter requests or logs.
    pub fn from_env(submission_enabled: bool) -> Result<Self, BrokerError> {
        let config = Config::from_apikey_env().map_err(|_| BrokerError::Disconnected)?;
        let sequential_config = SequentialExecutionConfig {
            fill_timeout_ms: env_u64("OPTIONTRADER_LONGBRIDGE_LEG_FILL_TIMEOUT_MS", 8_000)?,
            poll_interval_ms: env_u64("OPTIONTRADER_LONGBRIDGE_LEG_POLL_INTERVAL_MS", 250)?,
        }
        .validate()?;
        Ok(Self {
            context: TradeContextSync::new(Arc::new(config), |_| {}),
            submission_enabled,
            account: disconnected_account(),
            positions: Vec::new(),
            orders: BTreeMap::new(),
            order_by_key: BTreeMap::new(),
            sequential_config,
        })
    }

    fn mark_disconnected(&mut self) {
        self.account.health = BrokerHealth::Disconnected;
        self.account.reconciled = false;
    }

    fn remote_order(&self, order_id: &str) -> Result<LbOrder, BrokerError> {
        self.context
            .today_orders(GetTodayOrdersOptions::new().order_id(order_id.to_owned()))
            .map_err(|_| BrokerError::Disconnected)?
            .into_iter()
            .find(|order| order.order_id == order_id)
            .ok_or(BrokerError::OrderNotFound)
    }

    fn update_known_order(&mut self, order_id: &str) -> Result<BrokerOrder, BrokerError> {
        let current = self
            .orders
            .get(order_id)
            .cloned()
            .ok_or(BrokerError::OrderNotFound)?;
        if current.legs.len() == 1 {
            let remote = self.remote_order(order_id)?;
            let known = self
                .orders
                .get_mut(order_id)
                .ok_or(BrokerError::OrderNotFound)?;
            known.status = map_status(remote.status)?;
            known.filled_quantity = decimal_quantity(remote.executed_quantity)?;
            return Ok(known.clone());
        }
        let children = self.recover_split_children(&current)?;
        let known = self
            .orders
            .get_mut(order_id)
            .ok_or(BrokerError::OrderNotFound)?;
        known.child_orders = children;
        refresh_parent_status(known);
        Ok(known.clone())
    }

    fn recover_split_children(
        &self,
        parent: &BrokerOrder,
    ) -> Result<Vec<BrokerChildOrder>, BrokerError> {
        let remote_orders = self
            .context
            .today_orders(GetTodayOrdersOptions::new())
            .map_err(|_| BrokerError::Disconnected)?;
        let mut children = Vec::new();
        for leg_index in buy_first_indices(&parent.legs) {
            let leg = &parent.legs[leg_index];
            let remark = leg_remark(&parent.plan_hash, leg_index);
            let matches: Vec<&LbOrder> = remote_orders
                .iter()
                .filter(|order| {
                    order.remark == remark
                        && order.symbol == leg.broker_contract_id.as_deref().unwrap_or_default()
                        && order.side == map_side(leg.side)
                        && order.quantity == Decimal::from(leg.quantity)
                })
                .collect();
            if matches.len() > 1 {
                return Err(BrokerError::DuplicateConflict);
            }
            let Some(remote) = matches.first() else {
                break;
            };
            children.push(child_order(
                remote.order_id.clone(),
                leg_index,
                leg,
                map_status(remote.status)?,
                decimal_quantity(remote.executed_quantity)?,
            ));
        }
        if children.is_empty() {
            return Err(BrokerError::NotReconciled);
        }
        Ok(children)
    }

    fn submit_native_single(
        &mut self,
        request: BrokerOrderRequest,
    ) -> Result<BrokerOrder, BrokerError> {
        let leg = &request.legs[0];
        let native_symbol = leg
            .broker_contract_id
            .as_ref()
            .ok_or(BrokerError::UnsupportedOrderShape)?;
        let mut options = SubmitOrderOptions::new(
            native_symbol.clone(),
            map_order_type(request.order_type),
            map_side(leg.side),
            Decimal::from(leg.quantity),
            TimeInForceType::Day,
        )
        .remark(format!("optiontrader:{}", &request.plan_hash[..16]));
        if let Some(price) = leg.submitted_price {
            options = options.submitted_price(price);
        }
        let response = self.context.submit_order(options).map_err(|_| {
            self.mark_disconnected();
            BrokerError::Disconnected
        })?;
        Ok(BrokerOrder {
            broker_order_id: response.order_id,
            idempotency_key: request.idempotency_key,
            plan_hash: request.plan_hash,
            status: BrokerOrderStatus::Working,
            side: request.side,
            order_type: request.order_type,
            total_quantity: request.total_quantity,
            filled_quantity: 0,
            submitted_price: request.submitted_price,
            legs: request.legs,
            child_orders: Vec::new(),
            residual_exposure: false,
        })
    }
}

impl SequentialLegGateway for LongbridgeBroker {
    fn execute_leg(
        &mut self,
        request: &BrokerOrderRequest,
        leg_index: usize,
        leg: &BrokerOrderLeg,
        config: SequentialExecutionConfig,
    ) -> Result<BrokerChildOrder, BrokerError> {
        let native_symbol = leg
            .broker_contract_id
            .as_ref()
            .ok_or(BrokerError::UnsupportedOrderShape)?;
        let mut options = SubmitOrderOptions::new(
            native_symbol.clone(),
            map_order_type(request.order_type),
            map_side(leg.side),
            Decimal::from(leg.quantity),
            TimeInForceType::Day,
        )
        .remark(leg_remark(&request.plan_hash, leg_index));
        if let Some(price) = leg.submitted_price {
            options = options.submitted_price(price);
        }
        let response = self.context.submit_order(options).map_err(|_| {
            self.mark_disconnected();
            BrokerError::Disconnected
        })?;
        let started = Instant::now();
        let mut cancel_requested = false;
        let mut last_filled = 0;
        loop {
            match self.remote_order(&response.order_id) {
                Ok(remote) => {
                    let status = map_status(remote.status)?;
                    let filled = decimal_quantity(remote.executed_quantity)?;
                    last_filled = filled;
                    if remote.status == LbStatus::PartialFilled && !cancel_requested {
                        if self
                            .context
                            .cancel_order(response.order_id.clone())
                            .is_err()
                        {
                            self.mark_disconnected();
                            return Ok(child_order(
                                response.order_id,
                                leg_index,
                                leg,
                                BrokerOrderStatus::ReconcilePending,
                                filled,
                            ));
                        }
                        cancel_requested = true;
                    }
                    if matches!(
                        status,
                        BrokerOrderStatus::Filled
                            | BrokerOrderStatus::Cancelled
                            | BrokerOrderStatus::Rejected
                    ) || remote.status == LbStatus::PartialWithdrawal
                    {
                        return Ok(child_order(
                            response.order_id,
                            leg_index,
                            leg,
                            status,
                            filled,
                        ));
                    }
                }
                Err(BrokerError::OrderNotFound)
                    if started.elapsed() < Duration::from_millis(config.fill_timeout_ms) => {}
                Err(error) => {
                    self.mark_disconnected();
                    return Err(error);
                }
            }
            if started.elapsed() >= Duration::from_millis(config.fill_timeout_ms) {
                if !cancel_requested
                    && self
                        .context
                        .cancel_order(response.order_id.clone())
                        .is_err()
                {
                    self.mark_disconnected();
                    return Ok(child_order(
                        response.order_id,
                        leg_index,
                        leg,
                        BrokerOrderStatus::ReconcilePending,
                        last_filled,
                    ));
                }
                let after_cancel = match self.remote_order(&response.order_id) {
                    Ok(order) => order,
                    Err(_) => {
                        return Ok(child_order(
                            response.order_id,
                            leg_index,
                            leg,
                            BrokerOrderStatus::ReconcilePending,
                            last_filled,
                        ));
                    }
                };
                let after_status = map_status(after_cancel.status)?;
                let after_filled = decimal_quantity(after_cancel.executed_quantity)?;
                let terminal = matches!(
                    after_status,
                    BrokerOrderStatus::Filled
                        | BrokerOrderStatus::Cancelled
                        | BrokerOrderStatus::Rejected
                ) || after_cancel.status == LbStatus::PartialWithdrawal;
                return Ok(child_order(
                    response.order_id,
                    leg_index,
                    leg,
                    if terminal {
                        after_status
                    } else {
                        BrokerOrderStatus::ReconcilePending
                    },
                    after_filled,
                ));
            }
            thread::sleep(Duration::from_millis(config.poll_interval_ms));
        }
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
        validate_request(&request)?;
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

        let order = if request.legs.len() == 1 {
            self.submit_native_single(request.clone())?
        } else {
            let parent_id = format!("lb-split:{}", request.idempotency_key);
            let config = self.sequential_config;
            execute_buy_first(self, &request, parent_id, config)?
        };
        self.order_by_key
            .insert(request.idempotency_key, order.broker_order_id.clone());
        self.orders
            .insert(order.broker_order_id.clone(), order.clone());
        Ok(order)
    }

    fn cancel(&mut self, broker_order_id: &str) -> Result<BrokerOrder, BrokerError> {
        if !self.submission_enabled {
            return Err(BrokerError::LiveSubmissionDisabled);
        }
        let known = self
            .orders
            .get(broker_order_id)
            .cloned()
            .ok_or(BrokerError::OrderNotFound)?;
        let targets: Vec<String> = if known.child_orders.is_empty() {
            vec![broker_order_id.to_owned()]
        } else {
            known
                .child_orders
                .iter()
                .filter(|child| {
                    matches!(
                        child.status,
                        BrokerOrderStatus::Working | BrokerOrderStatus::PartialFill
                    )
                })
                .map(|child| child.broker_order_id.clone())
                .collect()
        };
        for target in targets {
            if self.context.cancel_order(target).is_err() {
                self.mark_disconnected();
                return Err(BrokerError::Disconnected);
            }
        }
        self.update_known_order(broker_order_id)
    }

    fn reconcile(&mut self) -> Result<(), BrokerError> {
        self.account.health = BrokerHealth::Reconciling;
        self.account.reconciled = false;
        let balances = self.context.account_balance(Some("USD")).map_err(|_| {
            self.mark_disconnected();
            BrokerError::Disconnected
        })?;
        if balances.len() != 1 {
            self.mark_disconnected();
            return Err(BrokerError::NotReconciled);
        }
        let remote_positions = self.context.stock_positions(None).map_err(|_| {
            self.mark_disconnected();
            BrokerError::Disconnected
        })?;
        let mut positions = Vec::new();
        for channel in remote_positions.channels {
            for position in channel.positions {
                positions.push(PositionSnapshot {
                    contract_id: position.symbol,
                    quantity: position
                        .quantity
                        .to_i32()
                        .ok_or(BrokerError::NotReconciled)?,
                });
            }
        }
        self.account.buying_power = balances[0].buy_power;
        self.positions = positions;
        let order_ids: Vec<String> = self.orders.keys().cloned().collect();
        for order_id in order_ids {
            self.update_known_order(&order_id)?;
        }
        self.account.health = BrokerHealth::Healthy;
        self.account.reconciled = true;
        Ok(())
    }
}

fn validate_request(request: &BrokerOrderRequest) -> Result<(), BrokerError> {
    if request.idempotency_key.is_empty() || request.plan_hash.len() != 64 {
        return Err(BrokerError::DuplicateConflict);
    }
    if request.legs.is_empty() || request.legs.len() > 4 {
        return Err(BrokerError::UnsupportedOrderShape);
    }
    if request.total_quantity == 0
        || request.legs.iter().any(|leg| {
            leg.quantity != request.total_quantity
                || leg.contract_id.is_empty()
                || leg.broker_contract_id.as_deref().is_none_or(str::is_empty)
        })
    {
        return Err(BrokerError::InvalidQuantity);
    }
    if request.legs.len() == 1 && request.legs[0].side != request.side {
        return Err(BrokerError::InvalidOrderType);
    }
    match request.order_type {
        BrokerOrderType::Market
            if request.submitted_price.is_none()
                && request.legs.iter().all(|leg| leg.submitted_price.is_none()) => {}
        BrokerOrderType::Limit | BrokerOrderType::AdaptiveLimit
            if request
                .submitted_price
                .is_some_and(|price| price > Decimal::ZERO)
                && request.legs.iter().all(|leg| {
                    leg.submitted_price
                        .is_some_and(|price| price > Decimal::ZERO)
                }) => {}
        _ => return Err(BrokerError::InvalidPrice),
    }
    if request.legs.len() > 1 && !request.legs.iter().any(|leg| leg.side == OrderSide::Buy) {
        return Err(BrokerError::UnsupportedOrderShape);
    }
    Ok(())
}

fn child_order(
    broker_order_id: String,
    leg_index: usize,
    leg: &BrokerOrderLeg,
    status: BrokerOrderStatus,
    filled_quantity: u32,
) -> BrokerChildOrder {
    BrokerChildOrder {
        broker_order_id,
        leg_index,
        contract_id: leg.contract_id.clone(),
        side: leg.side,
        quantity: leg.quantity,
        filled_quantity,
        status,
        submitted_price: leg.submitted_price,
    }
}

fn leg_remark(plan_hash: &str, leg_index: usize) -> String {
    format!("optiontrader:{}:leg{leg_index}", &plan_hash[..16])
}

fn buy_first_indices(legs: &[BrokerOrderLeg]) -> Vec<usize> {
    let mut indices: Vec<usize> = legs
        .iter()
        .enumerate()
        .filter_map(|(index, leg)| (leg.side == OrderSide::Buy).then_some(index))
        .collect();
    indices.extend(
        legs.iter()
            .enumerate()
            .filter_map(|(index, leg)| (leg.side == OrderSide::Sell).then_some(index)),
    );
    indices
}

fn refresh_parent_status(parent: &mut BrokerOrder) {
    let all_filled = parent.child_orders.len() == parent.legs.len()
        && parent.child_orders.iter().all(|child| {
            child.status == BrokerOrderStatus::Filled && child.filled_quantity == child.quantity
        });
    let uncertain = parent.child_orders.iter().any(|child| {
        matches!(
            child.status,
            BrokerOrderStatus::Working | BrokerOrderStatus::ReconcilePending
        )
    });
    parent.residual_exposure = uncertain
        || (parent
            .child_orders
            .iter()
            .any(|child| child.filled_quantity > 0)
            && !all_filled);
    parent.filled_quantity = if all_filled { parent.total_quantity } else { 0 };
    parent.status = if all_filled {
        BrokerOrderStatus::Filled
    } else if uncertain {
        BrokerOrderStatus::ReconcilePending
    } else if parent.residual_exposure {
        BrokerOrderStatus::PartialFill
    } else {
        BrokerOrderStatus::Rejected
    };
}

fn disconnected_account() -> AccountSnapshot {
    AccountSnapshot {
        broker_id: BrokerId::Longbridge,
        health: BrokerHealth::Disconnected,
        reconciled: false,
        buying_power: Decimal::ZERO,
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

fn map_order_type(order_type: BrokerOrderType) -> LbOrderType {
    match order_type {
        BrokerOrderType::Market => LbOrderType::MO,
        BrokerOrderType::Limit | BrokerOrderType::AdaptiveLimit => LbOrderType::LO,
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

fn env_u64(name: &str, default: u64) -> Result<u64, BrokerError> {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .map_err(|_| BrokerError::InvalidConfiguration),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => Err(BrokerError::InvalidConfiguration),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(legs: Vec<(OrderSide, Decimal)>, order_type: BrokerOrderType) -> BrokerOrderRequest {
        BrokerOrderRequest {
            idempotency_key: "key".into(),
            plan_hash: "a".repeat(64),
            side: if legs.iter().all(|(side, _)| *side == OrderSide::Buy) {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            order_type,
            total_quantity: 1,
            submitted_price: (order_type != BrokerOrderType::Market)
                .then_some(Decimal::new(125, 2)),
            legs: legs
                .into_iter()
                .enumerate()
                .map(|(index, (side, price))| BrokerOrderLeg {
                    contract_id: format!("QQQ260721C00{}00000.US", 500 + index),
                    side,
                    quantity: 1,
                    broker_contract_id: Some(format!("QQQ260721C00{}00000.US", 500 + index)),
                    symbol: Some("QQQ".into()),
                    exchange: None,
                    submitted_price: (order_type != BrokerOrderType::Market).then_some(price),
                })
                .collect(),
        }
    }

    #[test]
    fn longbridge_accepts_hedged_multi_leg_for_buy_first_execution() {
        let request = request(
            vec![
                (OrderSide::Sell, Decimal::new(200, 2)),
                (OrderSide::Buy, Decimal::new(75, 2)),
            ],
            BrokerOrderType::Limit,
        );
        assert_eq!(validate_request(&request), Ok(()));
    }

    #[test]
    fn longbridge_rejects_unhedged_multi_leg_sell() {
        let request = request(
            vec![
                (OrderSide::Sell, Decimal::new(200, 2)),
                (OrderSide::Sell, Decimal::new(75, 2)),
            ],
            BrokerOrderType::Limit,
        );
        assert_eq!(
            validate_request(&request),
            Err(BrokerError::UnsupportedOrderShape)
        );
    }

    #[test]
    fn market_has_no_price_and_limit_variants_require_every_leg_price() {
        assert_eq!(
            validate_request(&request(
                vec![(OrderSide::Buy, Decimal::ONE)],
                BrokerOrderType::Market
            )),
            Ok(())
        );
        let mut invalid = request(
            vec![(OrderSide::Buy, Decimal::ONE)],
            BrokerOrderType::AdaptiveLimit,
        );
        invalid.legs[0].submitted_price = None;
        assert_eq!(validate_request(&invalid), Err(BrokerError::InvalidPrice));
    }
}
