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

/// Immutable aggregated QQQ snapshot. Field names/types mirror
/// packages/contracts/jsonschema/market_snapshot.json; decimals are strings to
/// avoid binary float error. Phase 0 serves a deterministic fixture; Phase 1
/// replaces `fixture` with real ThetaData ingestion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketSnapshot {
    pub schema_version: String,
    pub snapshot_id: String,
    pub occurred_at_utc: String,
    pub timestamp_et: String,
    pub symbol: String,
    pub price: String,
    pub open: String,
    pub high: String,
    pub low: String,
    pub previous_close: String,
    pub vwap: String,
    pub volume: u64,
    pub opening_range_high: String,
    pub opening_range_low: String,
    pub premarket_high: Option<String>,
    pub premarket_low: Option<String>,
    pub sequence_number: u64,
    pub quote_age_ms: u64,
    pub data_health: DataHealth,
}

impl MarketSnapshot {
    /// Deterministic Phase 0 fixture. Mirrors
    /// packages/contracts/fixtures/market_snapshot.sample.json so the runtime
    /// Rust→Python→React path carries schema-valid data end to end.
    pub fn fixture() -> Self {
        MarketSnapshot {
            schema_version: "1.0".into(),
            snapshot_id: "mkt_20260720_094500_000123".into(),
            occurred_at_utc: "2026-07-20T13:45:00Z".into(),
            timestamp_et: "2026-07-20T09:45:00-04:00".into(),
            symbol: "QQQ.US".into(),
            price: "500.00".into(),
            open: "498.50".into(),
            high: "501.20".into(),
            low: "497.90".into(),
            previous_close: "497.20".into(),
            vwap: "499.40".into(),
            volume: 12_000_000,
            opening_range_high: "501.00".into(),
            opening_range_low: "497.80".into(),
            premarket_high: Some("499.80".into()),
            premarket_low: Some("496.50".into()),
            sequence_number: 123,
            quote_age_ms: 120,
            data_health: DataHealth::Healthy,
        }
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

    #[test]
    fn fixture_serializes_to_contract_shape() {
        let v = serde_json::to_value(MarketSnapshot::fixture()).unwrap();
        assert_eq!(v["schema_version"], "1.0");
        assert_eq!(v["symbol"], "QQQ.US");
        // decimals are strings, not floats
        assert_eq!(v["price"], "500.00");
        assert!(v["price"].is_string());
        // enum uses SCREAMING_SNAKE_CASE matching common.json#/$defs/dataHealth
        assert_eq!(v["data_health"], "HEALTHY");
        assert_eq!(v["volume"], 12_000_000);
    }
}
