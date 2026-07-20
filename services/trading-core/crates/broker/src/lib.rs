//! Broker adapters: Longbridge and IBKR. Each CandidateTradePlan targets
//! exactly one broker_id; a plan is never submitted to two brokers.
//! Phase 0 placeholder — adapter implementations land in Phase 3.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerId {
    Longbridge,
    Ibkr,
}

/// Adapter contract both Longbridge and IBKR implement (account, positions,
/// orders, fills, margin). Defined now so Python contracts and Rust share the
/// same execution semantics; methods land in Phase 3.
pub trait BrokerAdapter {
    fn broker_id(&self) -> BrokerId;
}
