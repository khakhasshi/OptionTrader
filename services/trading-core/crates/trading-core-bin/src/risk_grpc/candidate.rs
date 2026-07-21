//! Candidate evaluation and staging RPC handlers.

use super::*;

impl RiskExecutionServiceImpl {
    pub(super) async fn evaluate_candidate_rpc(
        &self,
        request: Request<EvaluateCandidateRequest>,
    ) -> Result<Response<ProtoDecision>, Status> {
        let now = (self.clock)();
        let raw = request.into_inner();
        Ok(Response::new(
            self.evaluate_raw(raw.plan.as_ref(), raw.event_context.as_ref(), now, false)
                .await?,
        ))
    }

    pub(super) async fn stage_candidate_rpc(
        &self,
        request: Request<EvaluateCandidateRequest>,
    ) -> Result<Response<StageCandidateResponse>, Status> {
        let now = (self.clock)();
        let raw = request.into_inner();
        let decision = self
            .evaluate_raw(raw.plan.as_ref(), raw.event_context.as_ref(), now, false)
            .await?;
        if decision.decision != RiskDecisionKind::Approved as i32 {
            return Ok(Response::new(StageCandidateResponse {
                initial_risk_decision: Some(decision),
                order: None,
                confirmation_token: String::new(),
            }));
        }
        let raw_plan = raw
            .plan
            .ok_or_else(|| Status::internal("approved decision had no plan"))?;
        let domain_plan = plan(&raw_plan)
            .map_err(|_| Status::internal("approved decision had an invalid plan"))?;
        let order_id = format!("order_{}", &raw_plan.plan_hash[..24]);
        let mut workflow = workflow_lock(self)
            .map_err(|()| Status::internal("execution workflow lock poisoned"))?;
        if let Some((existing_hash, existing_order_id)) =
            workflow.order_by_key.get(&raw_plan.idempotency_key)
        {
            if existing_hash != &raw_plan.plan_hash {
                let conflict = rejected(Some(&raw_plan), now, ProtoReason::DuplicateConflict);
                return Ok(Response::new(StageCandidateResponse {
                    initial_risk_decision: Some(conflict),
                    order: None,
                    confirmation_token: String::new(),
                }));
            }
            let existing = workflow
                .orders
                .get(existing_order_id)
                .ok_or_else(|| Status::internal("idempotency index references missing order"))?;
            return Ok(Response::new(StageCandidateResponse {
                initial_risk_decision: Some(decision),
                order: Some(order_proto(existing, now)),
                confirmation_token: existing.confirmation_token.clone(),
            }));
        }
        let total_quantity = domain_plan
            .legs
            .first()
            .ok_or_else(|| Status::internal("approved plan had no legs"))?
            .quantity;
        let mut record = OrderRecord::proposed(
            order_id.clone(),
            raw_plan.plan_id.clone(),
            raw_plan.plan_hash.clone(),
            raw_plan.idempotency_key.clone(),
            domain_plan.expires_at,
            total_quantity,
        )
        .map_err(|_| Status::internal("approved plan could not create order"))?;
        record
            .initial_risk(true, now)
            .map_err(|_| Status::internal("initial risk transition failed"))?;
        let confirmation_token = Uuid::new_v4().simple().to_string();
        let staged = StagedOrder {
            raw_plan: raw_plan.clone(),
            record,
            confirmation_token: confirmation_token.clone(),
            risk_reasons: Vec::new(),
        };
        workflow.order_by_key.insert(
            raw_plan.idempotency_key,
            (raw_plan.plan_hash, order_id.clone()),
        );
        let order = order_proto(&staged, now);
        workflow.orders.insert(order_id, staged);
        Ok(Response::new(StageCandidateResponse {
            initial_risk_decision: Some(decision),
            order: Some(order),
            confirmation_token,
        }))
    }
}
