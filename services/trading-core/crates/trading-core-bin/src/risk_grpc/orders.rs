//! Order query and cancellation RPC handlers.

use super::*;

impl RiskExecutionServiceImpl {
    pub(super) async fn cancel_order_rpc(
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

    pub(super) async fn get_order_rpc(
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
}
