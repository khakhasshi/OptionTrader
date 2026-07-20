//! Rust Market Core: ThetaData ingestion, normalization, deterministic
//! low-level features (bar/VWAP/opening range/ATM/straddle/spread/quote age)
//! and DataHealth. Does NOT make Regime/Strategy decisions.
//!
//! Phase 0: DataHealth enum only. Ingestion lands in Phase 1.

use serde::{Deserialize, Serialize};

/// Market data health, per CLAUDE.md §5. `fail closed` outside HEALTHY.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DataHealth {
    Healthy,
    Degraded,
    Stale,
    Disconnected,
    Reconciling,
}

impl DataHealth {
    /// New positions are only allowed when data is HEALTHY.
    pub fn allows_new_position(self) -> bool {
        matches!(self, DataHealth::Healthy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_healthy_allows_new_position() {
        assert!(DataHealth::Healthy.allows_new_position());
        for h in [
            DataHealth::Degraded,
            DataHealth::Stale,
            DataHealth::Disconnected,
            DataHealth::Reconciling,
        ] {
            assert!(!h.allows_new_position());
        }
    }
}
