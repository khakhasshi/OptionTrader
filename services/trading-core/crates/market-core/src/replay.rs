//! Deterministic snapshot source (Phase 2). Turns a trading day's standardized
//! bars into an ordered `MarketSnapshot` stream — the same job the Python
//! `ReplayClock` does offline, but here it is the Rust Market Core runtime
//! authority feeding the gRPC stream. No wall clock, no RNG: identical input
//! yields an identical snapshot sequence.
//!
//! `SnapshotSource` abstracts the origin so the live ThetaData adapter can slot
//! in behind the same interface once entitlement is verified; until then
//! `LiveThetaSource` fails closed with `Unavailable`.

use crate::features::{opening_range, session_vwap};
use crate::health::{DataHealthMachine, HealthConfig};
use crate::model::{normalize_bars, MarketBar};
use crate::{DataHealth, FeatureError, MarketSnapshot};

/// One standardized input bar. Timestamps are the authoritative RFC3339 strings
/// produced by ingestion (`occurred_at_utc` ends in Z; `timestamp_et` is
/// display-only) so replay never re-derives them. `minute_et` drives the
/// opening-range window; `occurred_at_utc_ms` drives health timing.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ReplayBar {
    pub occurred_at_utc: String,
    pub timestamp_et: String,
    pub occurred_at_utc_ms: i64,
    pub minute_et: u16,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u64,
    pub vwap: Option<f64>,
}

impl ReplayBar {
    fn as_market_bar(&self) -> MarketBar {
        MarketBar {
            occurred_at_utc_ms: self.occurred_at_utc_ms,
            minute_et: self.minute_et,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            vwap: self.vwap,
        }
    }
}

/// Replay knobs. Defaults match the QQQ intraday design.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReplayConfig {
    pub opening_range_minutes: u16,
    pub previous_close: Option<f64>,
    pub health: HealthConfig,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        ReplayConfig {
            opening_range_minutes: 15,
            previous_close: None,
            health: HealthConfig::default(),
        }
    }
}

/// A source of ordered market snapshots for one trading session.
pub trait SnapshotSource {
    /// Produce all snapshots for the session in deterministic order.
    fn snapshots(&self) -> Result<Vec<MarketSnapshot>, FeatureError>;
}

/// Replay a fixed set of standardized bars as a snapshot stream.
pub struct ReplaySnapshotSource {
    bars: Vec<ReplayBar>,
    cfg: ReplayConfig,
}

impl ReplaySnapshotSource {
    pub fn new(bars: Vec<ReplayBar>, cfg: ReplayConfig) -> Result<Self, FeatureError> {
        if bars.is_empty() {
            return Err(FeatureError::EmptyInput);
        }
        // Validate + sort + dedup via the shared normalizer (rejects conflicting
        // duplicates), keeping the ReplayBar metadata aligned to the result.
        let normalized = normalize_bars(bars.iter().map(ReplayBar::as_market_bar).collect())?;
        let mut ordered: Vec<ReplayBar> = Vec::with_capacity(normalized.len());
        for mb in &normalized {
            let src = bars
                .iter()
                .find(|b| b.occurred_at_utc_ms == mb.occurred_at_utc_ms)
                .expect("normalized bar came from input");
            ordered.push(src.clone());
        }
        Ok(ReplaySnapshotSource { bars: ordered, cfg })
    }

    /// Parse newline-delimited JSON `ReplayBar` records (one per line).
    pub fn from_ndjson(ndjson: &str, cfg: ReplayConfig) -> Result<Self, FeatureError> {
        let mut bars = Vec::new();
        for line in ndjson.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let bar: ReplayBar = serde_json::from_str(trimmed)
                .map_err(|_| FeatureError::InvalidArgument("malformed replay bar json"))?;
            bars.push(bar);
        }
        Self::new(bars, cfg)
    }
}

fn dec2(value: f64) -> String {
    format!("{value:.2}")
}

impl SnapshotSource for ReplaySnapshotSource {
    fn snapshots(&self) -> Result<Vec<MarketSnapshot>, FeatureError> {
        let mut machine = DataHealthMachine::new(self.cfg.health);
        let mut out = Vec::with_capacity(self.bars.len());
        let session_open = self.bars[0].open;

        for (index, bar) in self.bars.iter().enumerate() {
            machine.observe_bar(bar.occurred_at_utc_ms);

            let prefix: Vec<MarketBar> = self.bars[..=index]
                .iter()
                .map(ReplayBar::as_market_bar)
                .collect();

            let vwap = session_vwap(&prefix)?;
            let mut running_high = f64::NEG_INFINITY;
            let mut running_low = f64::INFINITY;
            let mut cumulative_volume = 0_u64;
            for b in &prefix {
                running_high = running_high.max(b.high);
                running_low = running_low.min(b.low);
                cumulative_volume = cumulative_volume.saturating_add(b.volume);
            }

            // Opening range is available only once its full window has elapsed.
            let or = opening_range(&prefix, self.cfg.opening_range_minutes).ok();
            let (or_high, or_low) = match or {
                Some(range) => (dec2(range.high), dec2(range.low)),
                None => (String::new(), String::new()),
            };

            let seq = (index + 1) as u64;
            let snapshot_id = format!("mkt_{}_{:06}", bar.occurred_at_utc_ms, seq);

            out.push(MarketSnapshot {
                schema_version: "1.0".into(),
                snapshot_id,
                occurred_at_utc: bar.occurred_at_utc.clone(),
                timestamp_et: bar.timestamp_et.clone(),
                symbol: "QQQ.US".into(),
                price: dec2(bar.close),
                open: dec2(session_open),
                high: dec2(running_high),
                low: dec2(running_low),
                previous_close: self.cfg.previous_close.map(dec2).unwrap_or_default(),
                vwap: dec2(vwap.value),
                volume: cumulative_volume,
                opening_range_high: or_high,
                opening_range_low: or_low,
                premarket_high: None,
                premarket_low: None,
                sequence_number: seq,
                quote_age_ms: 0,
                // Health reflects both the arrival-pattern machine and the
                // per-feature VWAP fallback; worst-of the two, fail closed.
                data_health: worst(machine.status(), vwap.data_health),
            });
        }
        Ok(out)
    }
}

/// Combine two health readings, keeping the less-healthy one (fail closed).
fn worst(a: DataHealth, b: DataHealth) -> DataHealth {
    fn rank(h: DataHealth) -> u8 {
        match h {
            DataHealth::Healthy => 0,
            DataHealth::Degraded => 1,
            DataHealth::Reconciling => 2,
            DataHealth::Stale => 3,
            DataHealth::Disconnected => 4,
        }
    }
    if rank(a) >= rank(b) {
        a
    } else {
        b
    }
}

/// Live ThetaData snapshot source. Interface placeholder until real-time
/// entitlement + field mapping are verified (TASKS.md). Fails closed.
pub struct LiveThetaSource;

impl SnapshotSource for LiveThetaSource {
    fn snapshots(&self) -> Result<Vec<MarketSnapshot>, FeatureError> {
        Err(FeatureError::InvalidArgument(
            "live ThetaData source not yet entitled/verified",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bar(minute_et: u16, close: f64, volume: u64) -> ReplayBar {
        let ms = i64::from(minute_et) * 60_000;
        ReplayBar {
            occurred_at_utc: format!(
                "2026-07-20T{:02}:{:02}:00Z",
                13 + minute_et / 60,
                minute_et % 60
            ),
            timestamp_et: format!(
                "2026-07-20T{:02}:{:02}:00-04:00",
                9 + minute_et / 60,
                minute_et % 60
            ),
            occurred_at_utc_ms: ms,
            minute_et,
            open: close,
            high: close + 0.5,
            low: close - 0.5,
            close,
            volume,
            vwap: Some(close),
        }
    }

    fn session(minutes: u16) -> Vec<ReplayBar> {
        (0..minutes)
            .map(|i| bar(570 + i, 500.0 + f64::from(i) * 0.1, 1000 + u64::from(i)))
            .collect()
    }

    fn cfg(or_minutes: u16) -> ReplayConfig {
        ReplayConfig {
            opening_range_minutes: or_minutes,
            previous_close: Some(497.20),
            health: HealthConfig::default(),
        }
    }

    #[test]
    fn replay_is_deterministic_and_ordered() {
        let src1 = ReplaySnapshotSource::new(session(10), cfg(3)).unwrap();
        let src2 = ReplaySnapshotSource::new(session(10), cfg(3)).unwrap();
        let a = src1.snapshots().unwrap();
        let b = src2.snapshots().unwrap();
        assert_eq!(a, b);
        assert_eq!(a.len(), 10);
        // occurred_at_utc strictly increasing; sequence 1..=n
        for (i, snap) in a.iter().enumerate() {
            assert_eq!(snap.sequence_number, (i + 1) as u64);
        }
        assert!(a
            .windows(2)
            .all(|w| w[0].occurred_at_utc < w[1].occurred_at_utc));
    }

    #[test]
    fn opening_range_absent_until_window_closes_then_present() {
        let src = ReplaySnapshotSource::new(session(6), cfg(3)).unwrap();
        let snaps = src.snapshots().unwrap();
        // Window is 3 minutes: snapshots 0,1 have no OR; from index 2 it is set.
        assert_eq!(snaps[0].opening_range_high, "");
        assert_eq!(snaps[1].opening_range_low, "");
        assert_ne!(snaps[2].opening_range_high, "");
        assert_ne!(snaps[5].opening_range_low, "");
    }

    #[test]
    fn clean_session_is_healthy_throughout() {
        let src = ReplaySnapshotSource::new(session(8), cfg(3)).unwrap();
        for snap in src.snapshots().unwrap() {
            assert_eq!(snap.data_health, DataHealth::Healthy);
        }
    }

    #[test]
    fn gap_in_bars_degrades_that_snapshot() {
        let mut bars = session(4);
        // Drop minute index 2 to create a 2-minute gap at that point.
        bars.remove(2);
        let src = ReplaySnapshotSource::new(bars, cfg(2)).unwrap();
        let snaps = src.snapshots().unwrap();
        // The snapshot after the gap is degraded.
        assert_eq!(snaps[2].data_health, DataHealth::Degraded);
    }

    #[test]
    fn empty_input_is_rejected() {
        assert!(matches!(
            ReplaySnapshotSource::new(vec![], cfg(3)),
            Err(FeatureError::EmptyInput)
        ));
    }

    #[test]
    fn ndjson_round_trips() {
        let bars = session(4);
        let ndjson: String = bars
            .iter()
            .map(|b| serde_json::to_string(b).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        let src = ReplaySnapshotSource::from_ndjson(&ndjson, cfg(2)).unwrap();
        assert_eq!(src.snapshots().unwrap().len(), 4);
    }

    #[test]
    fn live_source_fails_closed() {
        assert!(LiveThetaSource.snapshots().is_err());
    }

    #[test]
    fn snapshots_serialize_to_contract_shape() {
        let src = ReplaySnapshotSource::new(session(4), cfg(2)).unwrap();
        let snaps = src.snapshots().unwrap();
        let json = serde_json::to_value(&snaps[3]).unwrap();
        assert_eq!(json["schema_version"], "1.0");
        assert_eq!(json["symbol"], "QQQ.US");
        assert_eq!(json["data_health"], "HEALTHY");
        assert!(json["price"].as_str().unwrap().contains('.'));
    }
}
