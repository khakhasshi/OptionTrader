//! Rust Risk & Execution Gateway: the final authority for all open-position
//! requests. Hard risk policy, two-phase risk checks, order state machine,
//! reconciliation and kill switch. Never accepts free-text instructions
//! lacking audit context.
//!
//! Phase 0: BrokerHealth + the new-position gate. Full policy lands in Phase 3.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BrokerHealth {
    Healthy,
    Degraded,
    Disconnected,
    Reconciling,
}

impl BrokerHealth {
    pub fn allows_new_position(self) -> bool {
        matches!(self, BrokerHealth::Healthy)
    }
}

/// New positions require HEALTHY data, HEALTHY broker, and completed
/// reconciliation. Fail closed on anything else (CLAUDE.md §2, §5).
pub fn new_position_allowed(data_healthy: bool, broker: BrokerHealth, reconciled: bool) -> bool {
    data_healthy && broker.allows_new_position() && reconciled
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_requires_all_conditions() {
        assert!(new_position_allowed(true, BrokerHealth::Healthy, true));
        assert!(!new_position_allowed(false, BrokerHealth::Healthy, true));
        assert!(!new_position_allowed(true, BrokerHealth::Degraded, true));
        assert!(!new_position_allowed(true, BrokerHealth::Healthy, false));
    }
}
