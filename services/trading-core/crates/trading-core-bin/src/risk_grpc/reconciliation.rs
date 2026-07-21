//! Execution-order and broker-account reconciliation RPC handlers.

use super::*;

impl RiskExecutionServiceImpl {
    pub(super) async fn reconcile_execution_order_rpc(
        &self,
        request: Request<ReconcileExecutionOrderRequest>,
    ) -> Result<Response<ProtoOrder>, Status> {
        let now = (self.clock)();
        let order_id = request.into_inner().order_id;
        let (raw_plan, broker_order_id, total_quantity) =
            {
                let workflow = workflow_lock(self)
                    .map_err(|()| Status::internal("execution workflow lock poisoned"))?;
                let staged = workflow
                    .orders
                    .get(&order_id)
                    .ok_or_else(|| Status::not_found("order not found"))?;
                if staged.record.state != OrderState::ReconcilePending {
                    return Err(Status::failed_precondition(
                        "order does not require broker reconciliation",
                    ));
                }
                (
                    staged.raw_plan.clone(),
                    staged.record.broker_order_id.clone().ok_or_else(|| {
                        Status::failed_precondition("broker order id is unavailable")
                    })?,
                    staged.record.total_quantity,
                )
            };
        let pricing_time = recovery_pricing_time(&raw_plan)
            .map_err(|_| Status::failed_precondition("durable pricing proof is invalid"))?;
        let (side, order_type, submitted_price, legs) =
            priced_broker_order(&raw_plan, pricing_time)
                .map_err(|_| Status::failed_precondition("durable order proof is invalid"))?;
        let expected = expected_request(
            match ProtoBrokerId::try_from(raw_plan.broker_id).ok() {
                Some(ProtoBrokerId::Longbridge) => {
                    optiontrader_proto::broker_v1::BrokerId::Longbridge as i32
                }
                Some(ProtoBrokerId::Ibkr) => optiontrader_proto::broker_v1::BrokerId::Ibkr as i32,
                _ => {
                    return Err(Status::failed_precondition(
                        "durable broker route is invalid",
                    ))
                }
            },
            broker::BrokerOrderRequest {
                idempotency_key: raw_plan.idempotency_key.clone(),
                plan_hash: raw_plan.plan_hash.clone(),
                side,
                order_type,
                total_quantity,
                submitted_price,
                legs,
            },
        );
        let recovered = match self
            .broker_snapshots
            .recover(expected.clone(), broker_order_id.clone(), now)
            .await
        {
            Ok(value) => value,
            Err(error) => {
                let mut broker = self
                    .broker
                    .write()
                    .map_err(|_| Status::internal("broker authority lock poisoned"))?;
                broker.health = BrokerHealth::Reconciling;
                broker.reconciled = false;
                return Err(match error {
                    BrokerRecoveryError::Unavailable => {
                        Status::unavailable("broker recovery authority is unavailable")
                    }
                    BrokerRecoveryError::UnsupportedBroker => {
                        Status::failed_precondition("broker recovery route is not certified")
                    }
                    BrokerRecoveryError::InvalidSnapshot
                    | BrokerRecoveryError::NotReconciled
                    | BrokerRecoveryError::OrderConflict => {
                        Status::failed_precondition("broker recovery proof did not reconcile")
                    }
                });
            }
        };
        if self.execution_backend == BrokerExecutionBackend::LongbridgePaper {
            if let Err(error) = self
                .broker_mutations
                .bind_recovered_order_for_mutation(expected, broker_order_id)
                .await
            {
                let mut broker = self
                    .broker
                    .write()
                    .map_err(|_| Status::internal("broker authority lock poisoned"))?;
                broker.health = BrokerHealth::Reconciling;
                broker.reconciled = false;
                return Err(match error {
                    BrokerRecoveryError::Unavailable => {
                        Status::unavailable("Longbridge mutation identity rebinding is unavailable")
                    }
                    BrokerRecoveryError::UnsupportedBroker
                    | BrokerRecoveryError::InvalidSnapshot
                    | BrokerRecoveryError::NotReconciled
                    | BrokerRecoveryError::OrderConflict => Status::failed_precondition(
                        "Longbridge mutation identity did not reconcile",
                    ),
                });
            }
        }
        let (response, reconciliation_remains) = {
            let mut workflow = workflow_lock(self)
                .map_err(|()| Status::internal("execution workflow lock poisoned"))?;
            let staged = workflow
                .orders
                .get_mut(&order_id)
                .ok_or_else(|| Status::not_found("order disappeared during reconciliation"))?;
            if staged.record.state != OrderState::ReconcilePending {
                return Err(Status::failed_precondition(
                    "order changed during broker reconciliation",
                ));
            }
            staged
                .record
                .apply_broker_order(&recovered.order, now)
                .map_err(|_| Status::failed_precondition("broker order conflicts with workflow"))?;
            let response = order_proto(staged, now);
            let remains = workflow.orders.values().any(|entry| {
                entry.record.state == OrderState::ReconcilePending || entry.record.residual_exposure
            });
            (response, remains)
        };
        let account_reconciliation_pending = self
            .broker_reconciliations
            .lock()
            .await
            .values()
            .any(|entry| !entry.committed);
        let mut broker = self
            .broker
            .write()
            .map_err(|_| Status::internal("broker authority lock poisoned"))?;
        broker.buying_power = recovered.buying_power;
        broker.health = if reconciliation_remains || account_reconciliation_pending {
            BrokerHealth::Reconciling
        } else {
            BrokerHealth::Healthy
        };
        broker.reconciled = !reconciliation_remains && !account_reconciliation_pending;
        Ok(Response::new(response))
    }

    pub(super) async fn begin_broker_reconciliation_rpc(
        &self,
        request: Request<BeginBrokerReconciliationRequest>,
    ) -> Result<Response<BrokerReconciliationBatch>, Status> {
        let now = (self.clock)();
        let broker_id = request.into_inner().broker_id;
        if !matches!(
            ProtoBrokerId::try_from(broker_id).ok(),
            Some(ProtoBrokerId::Ibkr | ProtoBrokerId::Longbridge)
        ) {
            return Err(Status::invalid_argument("broker route is invalid"));
        }
        if self
            .execution_backend
            .broker_route()
            .is_some_and(|route| route as i32 != broker_id)
        {
            return Err(Status::failed_precondition(
                "broker reconciliation route differs from execution backend",
            ));
        }
        let mut reconciliations = self.broker_reconciliations.lock().await;
        reconciliations.insert(
            broker_id,
            PendingBrokerReconciliation {
                snapshot_sequence: 0,
                snapshot_hash: String::new(),
                expires_at: now + chrono::Duration::seconds(15),
                buying_power: Decimal::ZERO,
                positions: BTreeMap::new(),
                committed: false,
            },
        );
        {
            let mut broker = self
                .broker
                .write()
                .map_err(|_| Status::internal("broker authority lock poisoned"))?;
            broker.health = BrokerHealth::Reconciling;
            broker.reconciled = false;
        }
        let ValidatedBrokerSnapshot {
            snapshot,
            snapshot_hash,
            buying_power,
        } = self
            .broker_snapshots
            .fetch_snapshot(broker_id, now)
            .await
            .map_err(|error| match error {
                BrokerRecoveryError::Unavailable => {
                    Status::unavailable("broker snapshot authority is unavailable")
                }
                BrokerRecoveryError::UnsupportedBroker => {
                    Status::failed_precondition("broker snapshot route is not certified")
                }
                BrokerRecoveryError::InvalidSnapshot
                | BrokerRecoveryError::NotReconciled
                | BrokerRecoveryError::OrderConflict => {
                    Status::failed_precondition("broker account snapshot did not reconcile")
                }
            })?;
        let positions = snapshot
            .positions
            .iter()
            .filter(|position| position.quantity != 0)
            .map(|position| (position.contract_id.clone(), position.quantity))
            .collect();
        let expires_at = now + chrono::Duration::seconds(15);
        reconciliations.insert(
            broker_id,
            PendingBrokerReconciliation {
                snapshot_sequence: snapshot.snapshot_sequence,
                snapshot_hash: snapshot_hash.clone(),
                expires_at,
                buying_power,
                positions,
                committed: false,
            },
        );
        Ok(Response::new(BrokerReconciliationBatch {
            schema_version: "1.0".into(),
            broker_id,
            snapshot_sequence: snapshot.snapshot_sequence,
            snapshot_hash,
            snapshot_protobuf: snapshot.encode_to_vec(),
            expires_at_utc: expires_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        }))
    }

    pub(super) async fn commit_broker_reconciliation_rpc(
        &self,
        request: Request<CommitBrokerReconciliationRequest>,
    ) -> Result<Response<CommitBrokerReconciliationResponse>, Status> {
        let now = (self.clock)();
        let raw = request.into_inner();
        if raw.snapshot_hash.len() != 64
            || !raw
                .snapshot_hash
                .bytes()
                .all(|value| value.is_ascii_hexdigit())
            || raw.mismatch_codes.len() > 100
        {
            return Err(Status::invalid_argument(
                "broker reconciliation receipt is invalid",
            ));
        }
        let mut pending = self.broker_reconciliations.lock().await;
        let entry = pending
            .get_mut(&raw.broker_id)
            .ok_or_else(|| Status::failed_precondition("no broker reconciliation is pending"))?;
        if entry.snapshot_sequence != raw.snapshot_sequence
            || entry.snapshot_hash != raw.snapshot_hash
        {
            return Err(Status::failed_precondition(
                "broker reconciliation receipt does not match pending snapshot",
            ));
        }
        if entry.expires_at < now {
            return Err(Status::failed_precondition(
                "broker reconciliation receipt expired",
            ));
        }
        if !raw.persistence_succeeded || !raw.mismatch_codes.is_empty() {
            return Ok(Response::new(CommitBrokerReconciliationResponse {
                accepted: true,
                broker_reconciled: false,
                reason_codes: if raw.persistence_succeeded {
                    raw.mismatch_codes
                } else {
                    vec!["PERSISTENCE_FAILED".into()]
                },
            }));
        }
        let buying_power = entry.buying_power;
        let positions = entry.positions.clone();
        let already_committed = entry.committed;
        let workflow_pending = workflow_lock(self)
            .map_err(|()| Status::internal("execution workflow lock poisoned"))?
            .orders
            .values()
            .any(|entry| {
                entry.record.state == OrderState::ReconcilePending || entry.record.residual_exposure
            });
        if workflow_pending {
            return Ok(Response::new(CommitBrokerReconciliationResponse {
                accepted: true,
                broker_reconciled: false,
                reason_codes: vec!["WORKFLOW_RECONCILIATION_PENDING".into()],
            }));
        }
        let mut broker = self
            .broker
            .write()
            .map_err(|_| Status::internal("broker authority lock poisoned"))?;
        if !already_committed {
            entry.committed = true;
            broker.buying_power = buying_power;
            broker.positions = positions;
            broker.health = BrokerHealth::Healthy;
            broker.reconciled = true;
        } else if broker.health != BrokerHealth::Healthy || !broker.reconciled {
            return Ok(Response::new(CommitBrokerReconciliationResponse {
                accepted: true,
                broker_reconciled: false,
                reason_codes: vec!["BROKER_AUTHORITY_CHANGED".into()],
            }));
        }
        Ok(Response::new(CommitBrokerReconciliationResponse {
            accepted: true,
            broker_reconciled: true,
            reason_codes: Vec::new(),
        }))
    }

    pub(super) async fn restore_workflow_rpc(
        &self,
        request: Request<RestoreWorkflowRequest>,
    ) -> Result<Response<RestoreWorkflowResponse>, Status> {
        let now = (self.clock)();
        let entries = request.into_inner().entries;
        if entries.len() > 10_000 {
            return Err(Status::invalid_argument("restore batch is too large"));
        }
        let mut restored = Vec::with_capacity(entries.len());
        let mut batch_order_ids = BTreeSet::new();
        let mut batch_keys = BTreeSet::new();
        for entry in entries {
            let (staged, reconciliation_required) =
                restore_entry(entry, now).map_err(|reason| {
                    Status::invalid_argument(format!("invalid restore entry: {reason}"))
                })?;
            if !batch_order_ids.insert(staged.record.order_id.clone())
                || !batch_keys.insert(staged.record.idempotency_key.clone())
            {
                return Err(Status::invalid_argument("duplicate restore identity"));
            }
            restored.push((staged, reconciliation_required));
        }

        let mut workflow = workflow_lock(self)
            .map_err(|()| Status::internal("execution workflow lock poisoned"))?;
        for (staged, _) in &restored {
            let existing_order = workflow.orders.get(&staged.record.order_id);
            let existing_key = workflow.order_by_key.get(&staged.record.idempotency_key);
            if existing_order.is_some() || existing_key.is_some() {
                let compatible = existing_order.is_some_and(|existing| {
                    existing.record.plan_hash == staged.record.plan_hash
                        && existing.record.idempotency_key == staged.record.idempotency_key
                        && (existing.record.state_version() >= staged.record.state_version()
                            || (existing.record.state == OrderState::ReconcilePending
                                && staged.record.state == OrderState::ReconcilePending
                                && existing.record.state_version().checked_add(1)
                                    == Some(staged.record.state_version())))
                }) && existing_key.is_some_and(|(hash, order_id)| {
                    hash == &staged.record.plan_hash && order_id == &staged.record.order_id
                });
                if !compatible {
                    return Err(Status::already_exists("workflow identity conflicts"));
                }
            }
        }
        let mut orders = Vec::with_capacity(restored.len());
        let mut reconciliation_order_ids = Vec::new();
        for (staged, reconciliation_required) in restored {
            let order_id = staged.record.order_id.clone();
            if let Some(existing) = workflow.orders.get(&order_id) {
                if existing.record.state == OrderState::ReconcilePending {
                    reconciliation_order_ids.push(order_id);
                }
                orders.push(order_proto(existing, now));
                continue;
            }
            workflow.order_by_key.insert(
                staged.record.idempotency_key.clone(),
                (staged.record.plan_hash.clone(), order_id.clone()),
            );
            if reconciliation_required {
                reconciliation_order_ids.push(order_id.clone());
            }
            orders.push(order_proto(&staged, now));
            workflow.orders.insert(order_id, staged);
        }
        let reconciliation_required = !reconciliation_order_ids.is_empty();
        drop(workflow);
        if reconciliation_required {
            let mut broker = self
                .broker
                .write()
                .map_err(|_| Status::internal("broker authority lock poisoned"))?;
            broker.health = BrokerHealth::Reconciling;
            broker.reconciled = false;
        }
        Ok(Response::new(RestoreWorkflowResponse {
            orders,
            reconciliation_order_ids,
        }))
    }
}
