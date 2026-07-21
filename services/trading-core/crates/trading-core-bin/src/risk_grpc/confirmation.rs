//! Confirmation and broker-submission RPC handler.

use super::*;

impl RiskExecutionServiceImpl {
    pub(super) async fn confirm_candidate_rpc(
        &self,
        request: Request<ConfirmCandidateRequest>,
    ) -> Result<Response<ProtoOrder>, Status> {
        let now = (self.clock)();
        let raw = request.into_inner();
        let stored_plan = {
            let workflow = workflow_lock(self)
                .map_err(|()| Status::internal("execution workflow lock poisoned"))?;
            let staged = workflow
                .orders
                .get(&raw.order_id)
                .ok_or_else(|| Status::not_found("order not found"))?;
            if staged.record.plan_hash != raw.confirmed_plan_hash
                || staged.confirmation_token != raw.confirmation_token
            {
                return Err(Status::permission_denied(
                    "confirmation does not match staged plan",
                ));
            }
            if staged.record.state != OrderState::AwaitingConfirmation {
                return Ok(Response::new(order_proto(staged, now)));
            }
            staged.raw_plan.clone()
        };
        let decision = self
            .evaluate_raw(Some(&stored_plan), raw.event_context.as_ref(), now, true)
            .await?;
        let external_request =
            {
                let mut workflow = workflow_lock(self)
                    .map_err(|()| Status::internal("execution workflow lock poisoned"))?;
                let Workflow {
                    orders,
                    longbridge_paper,
                    ibkr_paper,
                    ..
                } = &mut *workflow;
                let staged = orders
                    .get_mut(&raw.order_id)
                    .ok_or_else(|| Status::not_found("order disappeared during confirmation"))?;
                if staged.record.state != OrderState::AwaitingConfirmation {
                    return Ok(Response::new(order_proto(staged, now)));
                }
                if decision.decision != RiskDecisionKind::Approved as i32 {
                    staged.risk_reasons = decision.reason_codes;
                    staged
                        .record
                        .final_risk_rejected(now)
                        .map_err(|_| Status::failed_precondition("order no longer confirmable"))?;
                    return Ok(Response::new(order_proto(staged, now)));
                }
                staged
                    .record
                    .confirm(
                        format!("confirm-{}", Uuid::new_v4().simple()),
                        &raw.confirmed_plan_hash,
                        now,
                    )
                    .map_err(|error| {
                        Status::failed_precondition(format!("confirmation failed: {error:?}"))
                    })?;
                let mode = ProtoMode::try_from(staged.raw_plan.execution_mode)
                    .map_err(|_| Status::internal("staged execution mode is invalid"))?;
                if matches!(mode, ProtoMode::Replay | ProtoMode::Shadow) {
                    staged
                        .record
                        .complete_shadow(now)
                        .map_err(|_| Status::internal("shadow transition failed"))?;
                    return Ok(Response::new(order_proto(staged, now)));
                }
                let priced = priced_broker_order(&staged.raw_plan, now);
                staged.record.begin_submit(now).map_err(|error| {
                    Status::failed_precondition(format!("submit blocked: {error:?}"))
                })?;
                let (order_side, order_type, submitted_price, adapter_legs) = match priced {
                    Ok(value) => value,
                    Err(_) => {
                        staged
                            .record
                            .submission_rejected(now)
                            .map_err(|_| Status::internal("pricing rejection transition failed"))?;
                        return Ok(Response::new(order_proto(staged, now)));
                    }
                };
                if self.execution_backend.is_simulated() {
                    let adapter = match ProtoBrokerId::try_from(staged.raw_plan.broker_id) {
                        Ok(ProtoBrokerId::Longbridge) => longbridge_paper,
                        Ok(ProtoBrokerId::Ibkr) => ibkr_paper,
                        _ => return Err(Status::internal("staged broker is invalid")),
                    };
                    if let Err(error) = submit_to_broker(
                        &mut staged.record,
                        adapter,
                        order_side,
                        order_type,
                        submitted_price,
                        adapter_legs,
                        now,
                    ) {
                        if matches!(
                            error,
                            execution::ExecutionError::Broker(BrokerError::Disconnected)
                                | execution::ExecutionError::Broker(BrokerError::NotReconciled)
                        ) {
                            staged.record.broker_disconnected(now).map_err(|_| {
                                Status::internal("broker disconnect transition failed")
                            })?;
                        } else {
                            staged.record.submission_rejected(now).map_err(|_| {
                                Status::internal("broker rejection transition failed")
                            })?;
                        }
                    }
                    return Ok(Response::new(order_proto(staged, now)));
                }
                if !self.execution_backend.is_external() {
                    staged
                        .record
                        .submission_rejected(now)
                        .map_err(|_| Status::internal("disabled route rejection failed"))?;
                    return Ok(Response::new(order_proto(staged, now)));
                }
                let broker_id = match ProtoBrokerId::try_from(staged.raw_plan.broker_id) {
                    Ok(ProtoBrokerId::Longbridge) => {
                        optiontrader_proto::broker_v1::BrokerId::Longbridge as i32
                    }
                    Ok(ProtoBrokerId::Ibkr) => optiontrader_proto::broker_v1::BrokerId::Ibkr as i32,
                    _ => return Err(Status::internal("staged broker is invalid")),
                };
                expected_request(
                    broker_id,
                    broker::BrokerOrderRequest {
                        idempotency_key: staged.raw_plan.idempotency_key.clone(),
                        plan_hash: staged.raw_plan.plan_hash.clone(),
                        side: order_side,
                        order_type,
                        total_quantity: staged.record.total_quantity,
                        submitted_price,
                        legs: adapter_legs,
                    },
                )
            };

        let submission = self.broker_mutations.submit_order(external_request).await;
        let route_requires_reconciliation = matches!(
            &submission,
            Err(BrokerMutationError::Disabled
                | BrokerMutationError::NotReady
                | BrokerMutationError::OutcomeUnknown)
        );
        let (response, must_reconcile) = {
            let mut workflow = workflow_lock(self)
                .map_err(|()| Status::internal("execution workflow lock poisoned"))?;
            let staged = workflow
                .orders
                .get_mut(&raw.order_id)
                .ok_or_else(|| Status::not_found("order disappeared after submission"))?;
            if staged.record.state != OrderState::Submitting {
                return Err(Status::failed_precondition(
                    "order changed while broker submission was in flight",
                ));
            }
            match submission {
                Ok(order) => {
                    if staged.record.apply_broker_order(&order, now).is_err() {
                        staged.record.broker_disconnected(now).map_err(|_| {
                            Status::internal("conflicting submit result could not reconcile")
                        })?;
                    }
                }
                Err(
                    BrokerMutationError::Disabled
                    | BrokerMutationError::NotReady
                    | BrokerMutationError::Rejected,
                ) => staged
                    .record
                    .submission_rejected(now)
                    .map_err(|_| Status::internal("broker rejection transition failed"))?,
                Err(BrokerMutationError::OutcomeUnknown) => staged
                    .record
                    .broker_disconnected(now)
                    .map_err(|_| Status::internal("unknown submit could not reconcile"))?,
            }
            (
                order_proto(staged, now),
                staged.record.state == OrderState::ReconcilePending
                    || staged.record.residual_exposure
                    || route_requires_reconciliation,
            )
        };
        if must_reconcile {
            let mut broker = self
                .broker
                .write()
                .map_err(|_| Status::internal("broker authority lock poisoned"))?;
            broker.health = BrokerHealth::Reconciling;
            broker.reconciled = false;
        }
        Ok(Response::new(response))
    }
}
