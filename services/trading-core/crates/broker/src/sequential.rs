use crate::{
    BrokerChildOrder, BrokerError, BrokerOrder, BrokerOrderLeg, BrokerOrderRequest,
    BrokerOrderStatus, OrderSide,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SequentialExecutionConfig {
    pub fill_timeout_ms: u64,
    pub poll_interval_ms: u64,
}

impl SequentialExecutionConfig {
    pub fn validate(self) -> Result<Self, BrokerError> {
        if !(1_000..=60_000).contains(&self.fill_timeout_ms)
            || !(50..=1_000).contains(&self.poll_interval_ms)
            || self.poll_interval_ms > self.fill_timeout_ms
        {
            return Err(BrokerError::InvalidConfiguration);
        }
        Ok(self)
    }
}

/// A native single-leg gateway. Implementations must return only after the
/// child reaches a known terminal state, or return Working/ReconcilePending
/// when broker truth is uncertain.
pub trait SequentialLegGateway {
    fn execute_leg(
        &mut self,
        request: &BrokerOrderRequest,
        leg_index: usize,
        leg: &BrokerOrderLeg,
        config: SequentialExecutionConfig,
    ) -> Result<BrokerChildOrder, BrokerError>;
}

/// Execute every BUY protection leg before any SELL risk leg. The function
/// never advances past an unfilled/partial/unknown child. A completed long leg
/// is retained as a protected residual instead of being dumped at market.
pub fn execute_buy_first<G: SequentialLegGateway>(
    gateway: &mut G,
    request: &BrokerOrderRequest,
    parent_order_id: String,
    config: SequentialExecutionConfig,
) -> Result<BrokerOrder, BrokerError> {
    let config = config.validate()?;
    if request.legs.len() < 2 || request.legs.len() > 4 {
        return Err(BrokerError::UnsupportedOrderShape);
    }
    let mut sequence: Vec<usize> = request
        .legs
        .iter()
        .enumerate()
        .filter_map(|(index, leg)| (leg.side == OrderSide::Buy).then_some(index))
        .collect();
    sequence.extend(
        request
            .legs
            .iter()
            .enumerate()
            .filter_map(|(index, leg)| (leg.side == OrderSide::Sell).then_some(index)),
    );
    if sequence.len() != request.legs.len()
        || !request.legs.iter().any(|leg| leg.side == OrderSide::Buy)
    {
        return Err(BrokerError::UnsupportedOrderShape);
    }

    let mut children = Vec::with_capacity(sequence.len());
    let mut uncertain = false;
    let mut known_failure = false;
    for index in sequence {
        let leg = &request.legs[index];
        match gateway.execute_leg(request, index, leg, config) {
            Ok(child) => {
                let complete = child.status == BrokerOrderStatus::Filled
                    && child.filled_quantity == child.quantity;
                uncertain = matches!(
                    child.status,
                    BrokerOrderStatus::Working | BrokerOrderStatus::ReconcilePending
                );
                known_failure = matches!(
                    child.status,
                    BrokerOrderStatus::PartialFill
                        | BrokerOrderStatus::Cancelled
                        | BrokerOrderStatus::Rejected
                );
                children.push(child);
                if !complete {
                    break;
                }
            }
            Err(BrokerError::Disconnected | BrokerError::NotReconciled) => {
                uncertain = true;
                break;
            }
            Err(_) => {
                known_failure = true;
                break;
            }
        }
    }

    let residual_exposure = uncertain
        || (children.iter().any(|child| child.filled_quantity > 0)
            && (children.len() != request.legs.len()
                || children.iter().any(|child| {
                    child.status != BrokerOrderStatus::Filled
                        || child.filled_quantity != child.quantity
                })));
    let all_filled = children.len() == request.legs.len()
        && children.iter().all(|child| {
            child.status == BrokerOrderStatus::Filled && child.filled_quantity == child.quantity
        });
    let status = if all_filled {
        BrokerOrderStatus::Filled
    } else if uncertain {
        BrokerOrderStatus::ReconcilePending
    } else if residual_exposure {
        BrokerOrderStatus::PartialFill
    } else if known_failure {
        BrokerOrderStatus::Rejected
    } else {
        BrokerOrderStatus::ReconcilePending
    };
    Ok(BrokerOrder {
        broker_order_id: parent_order_id,
        idempotency_key: request.idempotency_key.clone(),
        plan_hash: request.plan_hash.clone(),
        status,
        side: request.side,
        order_type: request.order_type,
        total_quantity: request.total_quantity,
        filled_quantity: if all_filled {
            request.total_quantity
        } else {
            0
        },
        submitted_price: request.submitted_price,
        legs: request.legs.clone(),
        child_orders: children,
        residual_exposure,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;
    use crate::{BrokerOrderType, OrderSide};
    use rust_decimal::Decimal;

    struct ScriptedGateway {
        results: VecDeque<Result<(BrokerOrderStatus, u32), BrokerError>>,
        seen: Vec<(usize, OrderSide)>,
    }

    impl SequentialLegGateway for ScriptedGateway {
        fn execute_leg(
            &mut self,
            _request: &BrokerOrderRequest,
            leg_index: usize,
            leg: &BrokerOrderLeg,
            _config: SequentialExecutionConfig,
        ) -> Result<BrokerChildOrder, BrokerError> {
            self.seen.push((leg_index, leg.side));
            let (status, filled_quantity) = self.results.pop_front().unwrap()?;
            Ok(BrokerChildOrder {
                broker_order_id: format!("child-{leg_index}"),
                leg_index,
                contract_id: leg.contract_id.clone(),
                side: leg.side,
                quantity: leg.quantity,
                filled_quantity,
                status,
                submitted_price: leg.submitted_price,
            })
        }
    }

    fn request() -> BrokerOrderRequest {
        BrokerOrderRequest {
            idempotency_key: "key".into(),
            plan_hash: "a".repeat(64),
            side: OrderSide::Sell,
            order_type: BrokerOrderType::Limit,
            total_quantity: 2,
            submitted_price: Some(Decimal::ONE),
            legs: vec![
                leg("short", OrderSide::Sell, Decimal::new(200, 2)),
                leg("long", OrderSide::Buy, Decimal::new(100, 2)),
            ],
        }
    }

    fn leg(contract_id: &str, side: OrderSide, price: Decimal) -> BrokerOrderLeg {
        BrokerOrderLeg {
            contract_id: contract_id.into(),
            side,
            quantity: 2,
            broker_contract_id: Some(contract_id.into()),
            symbol: Some("QQQ".into()),
            exchange: None,
            submitted_price: Some(price),
        }
    }

    fn config() -> SequentialExecutionConfig {
        SequentialExecutionConfig {
            fill_timeout_ms: 8_000,
            poll_interval_ms: 250,
        }
    }

    #[test]
    fn buy_fill_is_confirmed_before_sell_submission() {
        let mut gateway = ScriptedGateway {
            results: VecDeque::from([
                Ok((BrokerOrderStatus::Filled, 2)),
                Ok((BrokerOrderStatus::Filled, 2)),
            ]),
            seen: Vec::new(),
        };
        let parent =
            execute_buy_first(&mut gateway, &request(), "parent".into(), config()).unwrap();
        assert_eq!(
            gateway.seen,
            vec![(1, OrderSide::Buy), (0, OrderSide::Sell)]
        );
        assert_eq!(parent.status, BrokerOrderStatus::Filled);
        assert_eq!(parent.filled_quantity, 2);
        assert!(!parent.residual_exposure);
    }

    #[test]
    fn partial_buy_stops_before_sell_and_tracks_protected_residual() {
        let mut gateway = ScriptedGateway {
            results: VecDeque::from([Ok((BrokerOrderStatus::PartialFill, 1))]),
            seen: Vec::new(),
        };
        let parent =
            execute_buy_first(&mut gateway, &request(), "parent".into(), config()).unwrap();
        assert_eq!(gateway.seen, vec![(1, OrderSide::Buy)]);
        assert_eq!(parent.status, BrokerOrderStatus::PartialFill);
        assert_eq!(parent.filled_quantity, 0);
        assert!(parent.residual_exposure);
    }

    #[test]
    fn unknown_second_leg_never_submits_more_risk_and_requires_reconciliation() {
        let mut four = request();
        four.legs
            .push(leg("short-2", OrderSide::Sell, Decimal::new(150, 2)));
        let mut gateway = ScriptedGateway {
            results: VecDeque::from([
                Ok((BrokerOrderStatus::Filled, 2)),
                Err(BrokerError::Disconnected),
            ]),
            seen: Vec::new(),
        };
        let parent = execute_buy_first(&mut gateway, &four, "parent".into(), config()).unwrap();
        assert_eq!(gateway.seen.len(), 2);
        assert_eq!(parent.status, BrokerOrderStatus::ReconcilePending);
        assert!(parent.residual_exposure);
    }

    #[test]
    fn ambiguous_first_submit_marks_possible_exposure_and_requires_reconciliation() {
        let mut gateway = ScriptedGateway {
            results: VecDeque::from([Err(BrokerError::Disconnected)]),
            seen: Vec::new(),
        };
        let parent =
            execute_buy_first(&mut gateway, &request(), "parent".into(), config()).unwrap();
        assert_eq!(gateway.seen, vec![(1, OrderSide::Buy)]);
        assert_eq!(parent.status, BrokerOrderStatus::ReconcilePending);
        assert!(parent.child_orders.is_empty());
        assert!(parent.residual_exposure);
    }
}
