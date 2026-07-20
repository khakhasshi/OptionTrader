//! Execution: order state machine and idempotent Broker submission.
//! Phase 0 placeholder — the order finite state machine lands in Phase 3.

/// Order lifecycle states, per DEVELOPMENT_PLAN.md §5.4 state machine.
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
}
