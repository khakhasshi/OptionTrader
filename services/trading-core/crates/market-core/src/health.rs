//! DataHealth state machine (CLAUDE.md §5). Drives the HEALTHY → DEGRADED →
//! STALE → DISCONNECTED → RECONCILING transitions from the *arrival pattern* of
//! market records, so the trading-permission gate fails closed the moment the
//! feed degrades. This is the single Rust authority for runtime DataHealth;
//! Python only consumes the resulting state, never recomputes it.

use crate::DataHealth;

/// Timing thresholds, in milliseconds. Defaults target QQQ 1-minute bars.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HealthConfig {
    /// Expected spacing between consecutive records (1 minute for bars).
    pub expected_interval_ms: i64,
    /// A gap beyond this (but within `disconnect_after_ms`) is STALE.
    pub stale_after_ms: i64,
    /// A gap beyond this is DISCONNECTED.
    pub disconnect_after_ms: i64,
}

impl Default for HealthConfig {
    fn default() -> Self {
        HealthConfig {
            expected_interval_ms: 60_000,
            stale_after_ms: 180_000,
            disconnect_after_ms: 600_000,
        }
    }
}

/// Runtime DataHealth record. Mirrors
/// `packages/contracts/jsonschema/health.json#/$defs/DataHealthState` and the
/// `DataHealthState` proto message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataHealthStateRecord {
    pub occurred_at_utc: String,
    pub status: DataHealth,
    pub market_event_lag_ms: u64,
    pub quote_age_ms: u64,
    pub out_of_order_count: u64,
    pub reconnect_count: u64,
    pub reason: Option<String>,
}

/// Incremental DataHealth evaluator. Feed it each record's `occurred_at_utc_ms`
/// in arrival order; query `state()` for the current health.
#[derive(Debug, Clone)]
pub struct DataHealthMachine {
    cfg: HealthConfig,
    status: DataHealth,
    last_bar_ms: Option<i64>,
    last_lag_ms: u64,
    out_of_order_count: u64,
    reconnect_count: u64,
    reason: Option<&'static str>,
}

impl DataHealthMachine {
    pub fn new(cfg: HealthConfig) -> Self {
        DataHealthMachine {
            cfg,
            // Before the first record arrives we are reconciling, never healthy.
            status: DataHealth::Reconciling,
            last_bar_ms: None,
            last_lag_ms: 0,
            out_of_order_count: 0,
            reconnect_count: 0,
            reason: Some("awaiting_first_record"),
        }
    }

    pub fn status(&self) -> DataHealth {
        self.status
    }

    pub fn out_of_order_count(&self) -> u64 {
        self.out_of_order_count
    }

    pub fn reconnect_count(&self) -> u64 {
        self.reconnect_count
    }

    /// Record a stream reconnection: counts it and drops back to RECONCILING
    /// until the next record confirms the feed is flowing.
    pub fn observe_reconnect(&mut self) {
        self.reconnect_count += 1;
        self.status = DataHealth::Reconciling;
        self.reason = Some("reconnecting");
    }

    /// Ingest one record's business timestamp (arrival order).
    pub fn observe_bar(&mut self, occurred_at_utc_ms: i64) {
        match self.last_bar_ms {
            None => {
                // First record (or first after a reconnect) confirms flow.
                self.status = DataHealth::Healthy;
                self.last_lag_ms = 0;
                self.reason = None;
                self.last_bar_ms = Some(occurred_at_utc_ms);
            }
            Some(last) if occurred_at_utc_ms <= last => {
                // Out of order or duplicate: count it, degrade, do not advance.
                self.out_of_order_count += 1;
                self.status = DataHealth::Degraded;
                self.reason = Some("out_of_order");
            }
            Some(last) => {
                let gap = occurred_at_utc_ms - last;
                self.last_lag_ms = gap.max(0) as u64;
                self.status = if gap <= self.cfg.expected_interval_ms {
                    self.reason = None;
                    DataHealth::Healthy
                } else if gap <= self.cfg.stale_after_ms {
                    self.reason = Some("minute_gap");
                    DataHealth::Degraded
                } else if gap <= self.cfg.disconnect_after_ms {
                    self.reason = Some("stale_gap");
                    DataHealth::Stale
                } else {
                    self.reason = Some("disconnected_gap");
                    DataHealth::Disconnected
                };
                self.last_bar_ms = Some(occurred_at_utc_ms);
            }
        }
    }

    /// Evaluate silence: with no new record, how healthy are we as of `now_ms`?
    /// Used by a live feed watchdog (replay never calls this).
    pub fn observe_silence(&mut self, now_ms: i64) {
        if let Some(last) = self.last_bar_ms {
            let gap = (now_ms - last).max(0);
            self.last_lag_ms = gap as u64;
            self.status = if gap <= self.cfg.expected_interval_ms {
                self.status
            } else if gap <= self.cfg.stale_after_ms {
                self.reason = Some("silence_degraded");
                DataHealth::Degraded
            } else if gap <= self.cfg.disconnect_after_ms {
                self.reason = Some("silence_stale");
                DataHealth::Stale
            } else {
                self.reason = Some("silence_disconnected");
                DataHealth::Disconnected
            };
        }
    }

    /// Snapshot the current health as a contract record stamped at `occurred_at_utc`.
    pub fn state(&self, occurred_at_utc: impl Into<String>) -> DataHealthStateRecord {
        DataHealthStateRecord {
            occurred_at_utc: occurred_at_utc.into(),
            status: self.status,
            market_event_lag_ms: self.last_lag_ms,
            quote_age_ms: self.last_lag_ms,
            out_of_order_count: self.out_of_order_count,
            reconnect_count: self.reconnect_count,
            reason: self.reason.map(str::to_owned),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: i64 = 60_000;

    #[test]
    fn starts_reconciling_and_first_bar_goes_healthy() {
        let mut machine = DataHealthMachine::new(HealthConfig::default());
        assert_eq!(machine.status(), DataHealth::Reconciling);
        machine.observe_bar(0);
        assert_eq!(machine.status(), DataHealth::Healthy);
    }

    #[test]
    fn steady_one_minute_cadence_stays_healthy() {
        let mut machine = DataHealthMachine::new(HealthConfig::default());
        for i in 0..5 {
            machine.observe_bar(i * MIN);
            assert_eq!(machine.status(), DataHealth::Healthy);
        }
        assert_eq!(machine.out_of_order_count(), 0);
    }

    #[test]
    fn one_missed_minute_degrades() {
        let mut machine = DataHealthMachine::new(HealthConfig::default());
        machine.observe_bar(0);
        machine.observe_bar(2 * MIN); // skipped minute 1
        assert_eq!(machine.status(), DataHealth::Degraded);
        assert_eq!(machine.state("t").market_event_lag_ms, 2 * MIN as u64);
    }

    #[test]
    fn large_gap_goes_stale_then_disconnected() {
        let mut machine = DataHealthMachine::new(HealthConfig::default());
        machine.observe_bar(0);
        machine.observe_bar(5 * MIN); // 5 min gap -> stale
        assert_eq!(machine.status(), DataHealth::Stale);
        machine.observe_bar(5 * MIN + 20 * MIN); // 20 min gap -> disconnected
        assert_eq!(machine.status(), DataHealth::Disconnected);
    }

    #[test]
    fn out_of_order_is_counted_and_degrades_without_advancing() {
        let mut machine = DataHealthMachine::new(HealthConfig::default());
        machine.observe_bar(3 * MIN);
        machine.observe_bar(2 * MIN); // older than last
        assert_eq!(machine.status(), DataHealth::Degraded);
        assert_eq!(machine.out_of_order_count(), 1);
        // last_bar unchanged, so the next in-order bar is evaluated against 3*MIN
        machine.observe_bar(4 * MIN);
        assert_eq!(machine.status(), DataHealth::Healthy);
    }

    #[test]
    fn reconnect_counts_and_returns_to_reconciling_then_recovers() {
        let mut machine = DataHealthMachine::new(HealthConfig::default());
        machine.observe_bar(0);
        machine.observe_reconnect();
        assert_eq!(machine.status(), DataHealth::Reconciling);
        assert_eq!(machine.reconnect_count(), 1);
        machine.observe_bar(MIN);
        assert_eq!(machine.status(), DataHealth::Healthy);
    }

    #[test]
    fn silence_watchdog_escalates_without_new_bars() {
        let mut machine = DataHealthMachine::new(HealthConfig::default());
        machine.observe_bar(0);
        machine.observe_silence(2 * MIN);
        assert_eq!(machine.status(), DataHealth::Degraded);
        machine.observe_silence(5 * MIN);
        assert_eq!(machine.status(), DataHealth::Stale);
        machine.observe_silence(30 * MIN);
        assert_eq!(machine.status(), DataHealth::Disconnected);
    }
}
