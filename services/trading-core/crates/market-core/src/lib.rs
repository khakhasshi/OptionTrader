//! Rust Market Core: ThetaData ingestion, normalization, deterministic
//! low-level features (bar/VWAP/opening range/ATM/straddle/spread/quote age)
//! and DataHealth. Does NOT make Regime/Strategy decisions.
//!
//! Phase 0: DataHealth enum only. Ingestion lands in Phase 1.

use serde::{Deserialize, Serialize};
use std::fmt;

pub mod features;
pub mod health;
pub mod model;
pub mod replay;
pub mod theta;

pub use features::{
    assess_bar_health, atm_straddle, bid_ask_spread, historical_volatility, hv20_hv60,
    opening_range, quote_age_ms, session_vwap, FeatureValue, OpeningRange, StraddleMark,
};
pub use health::{DataHealthMachine, DataHealthStateRecord, HealthConfig};
pub use model::{normalize_bars, MarketBar, OptionQuote, OptionRight, SESSION_OPEN_MINUTE_ET};
pub use replay::{ReplayBar, ReplayConfig, ReplaySnapshotSource, SnapshotSource};
pub use theta::{
    parse_ohlc_backfill, parse_stream_message, subscribe_trade_request, ThetaBarAggregator,
    ThetaStreamEvent,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeatureError {
    EmptyInput,
    InvalidArgument(&'static str),
    InvalidMarket(&'static str),
    ConflictingDuplicate,
    IncompleteOpeningRange,
    InsufficientHistory,
    MissingOptionLeg,
    MismatchedQuoteTime,
    FutureQuote,
}

impl fmt::Display for FeatureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for FeatureError {}

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

    #[derive(Deserialize)]
    struct ExpectedFeatures {
        session_vwap: f64,
        opening_range_high: f64,
        opening_range_low: f64,
        hv20: f64,
        hv60: f64,
    }

    #[derive(Deserialize)]
    struct SharedFeatureFixture {
        bars: Vec<MarketBar>,
        daily_closes: Vec<f64>,
        expected: ExpectedFeatures,
    }

    fn bar(minute_et: u16, close: f64, volume: u64, vwap: Option<f64>) -> MarketBar {
        MarketBar {
            occurred_at_utc_ms: i64::from(minute_et) * 60_000,
            minute_et,
            open: close,
            high: close + 1.0,
            low: close - 1.0,
            close,
            volume,
            vwap,
        }
    }

    fn quote(expiry: &str, right: OptionRight, occurred_at_utc_ms: i64) -> OptionQuote {
        OptionQuote {
            underlying: "QQQ".into(),
            expiry: expiry.into(),
            strike: 500.0,
            right,
            occurred_at_utc_ms,
            bid: 2.0,
            ask: 2.2,
        }
    }

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

    #[test]
    fn provider_vwap_is_authoritative_and_fallback_degrades() {
        let bars = [
            bar(570, 100.0, 1, Some(90.0)),
            bar(571, 100.0, 3, Some(110.0)),
        ];
        let feature = session_vwap(&bars).unwrap();
        assert!((feature.value - 105.0).abs() < 1e-12);
        assert_eq!(feature.data_health, DataHealth::Healthy);

        let fallback = session_vwap(&[bar(570, 100.0, 1, None)]).unwrap();
        assert_eq!(fallback.data_health, DataHealth::Degraded);
    }

    #[test]
    fn market_core_downgrades_late_or_gapped_bars() {
        let healthy = [
            bar(570, 100.0, 1, Some(100.0)),
            bar(571, 101.0, 1, Some(101.0)),
        ];
        assert_eq!(
            assess_bar_health(&healthy).unwrap().data_health,
            DataHealth::Healthy
        );

        let late = [bar(600, 100.0, 1, Some(100.0))];
        assert_eq!(
            assess_bar_health(&late).unwrap().data_health,
            DataHealth::Degraded
        );

        let gapped = [
            bar(570, 100.0, 1, Some(100.0)),
            bar(572, 101.0, 1, Some(101.0)),
        ];
        let result = assess_bar_health(&gapped).unwrap();
        assert_eq!(result.data_health, DataHealth::Degraded);
        assert!(result.reasons.contains(&"minute_gap_or_duplicate"));
    }

    #[test]
    fn opening_range_is_anchored_at_0930_and_complete() {
        let valid = [
            bar(570, 100.0, 1, Some(100.0)),
            bar(571, 102.0, 1, Some(102.0)),
        ];
        let result = opening_range(&valid, 2).unwrap();
        assert_eq!(result.high, 103.0);
        assert_eq!(result.low, 99.0);

        let late = [
            bar(600, 100.0, 1, Some(100.0)),
            bar(601, 102.0, 1, Some(102.0)),
        ];
        assert_eq!(
            opening_range(&late, 2),
            Err(FeatureError::IncompleteOpeningRange)
        );
    }

    #[test]
    fn hv20_hv60_use_daily_closes() {
        let closes: Vec<f64> = (0..61).map(|index| 100.0 * 1.01_f64.powi(index)).collect();
        let (hv20, hv60) = hv20_hv60(&closes).unwrap();
        assert!(hv20.abs() < 1e-12);
        assert!(hv60.abs() < 1e-12);
    }

    #[test]
    fn straddle_rejects_mixed_expiry_and_timestamp() {
        let mixed_expiry = [
            quote("2026-07-09", OptionRight::C, 1_000),
            quote("2026-07-10", OptionRight::P, 1_000),
        ];
        assert_eq!(
            atm_straddle(&mixed_expiry, 500.0, "QQQ", "2026-07-09", 1_000, 100),
            Err(FeatureError::MissingOptionLeg)
        );

        let mixed_time = [
            quote("2026-07-09", OptionRight::C, 1_000),
            quote("2026-07-09", OptionRight::P, 999),
        ];
        assert_eq!(
            atm_straddle(&mixed_time, 500.0, "QQQ", "2026-07-09", 1_000, 100),
            Err(FeatureError::MismatchedQuoteTime)
        );
    }

    #[test]
    fn normalization_rejects_conflicting_duplicates() {
        let first = bar(570, 100.0, 1, Some(100.0));
        let mut conflict = first.clone();
        conflict.close = 101.0;
        assert_eq!(
            normalize_bars(vec![first, conflict]),
            Err(FeatureError::ConflictingDuplicate)
        );
    }

    #[test]
    fn shared_python_rust_feature_fixture_matches() {
        let fixture: SharedFeatureFixture = serde_json::from_str(include_str!(
            "../../../../../packages/contracts/fixtures/market_features.sample.json"
        ))
        .unwrap();
        let expected = fixture.expected;
        let vwap = session_vwap(&fixture.bars).unwrap();
        assert!((vwap.value - expected.session_vwap).abs() < 1e-12);
        let orange = opening_range(&fixture.bars, 2).unwrap();
        assert!((orange.high - expected.opening_range_high).abs() < 1e-12);
        assert!((orange.low - expected.opening_range_low).abs() < 1e-12);
        let (hv20, hv60) = hv20_hv60(&fixture.daily_closes).unwrap();
        assert!((hv20 - expected.hv20).abs() < 1e-12);
        assert!((hv60 - expected.hv60).abs() < 1e-12);
    }
}
