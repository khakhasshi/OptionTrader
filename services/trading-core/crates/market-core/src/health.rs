//! DataHealth state machine (CLAUDE.md §5). Drives the HEALTHY → DEGRADED →
//! STALE → DISCONNECTED → RECONCILING transitions from the *arrival pattern* of
//! market records, so the trading-permission gate fails closed the moment the
//! feed degrades. This is the single Rust authority for runtime DataHealth;
//! Python only consumes the resulting state, never recomputes it.
//!
//! Fail-closed posture (per Phase 2 review):
//!   * The first record must land on the fixed session open (09:30 ET). A late
//!     start keeps the feed out of HEALTHY (backfill/reconciliation required).
//!   * A gap, out-of-order record, stale/disconnected span, or reconnect sets a
//!     sticky "needs reconciliation" flag. Health cannot return to HEALTHY on
//!     the next well-spaced record alone — only an explicit `mark_reconciled()`
//!     (backfill confirmed) clears it. "The gap healed by itself" is not enough.

use crate::DataHealth;

/// ET minute-of-day for the regular-session open (09:30). The first record of a
/// session must carry this minute to open HEALTHY.
pub const SESSION_OPEN_MINUTE_ET: u16 = 9 * 60 + 30;

/// Timing thresholds, in milliseconds. Defaults target QQQ 1-minute bars.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HealthConfig {
    /// Expected spacing between consecutive records (1 minute for bars).
    pub expected_interval_ms: i64,
    /// A gap beyond this (but within `disconnect_after_ms`) is STALE.
    pub stale_after_ms: i64,
    /// A gap beyond this is DISCONNECTED.
    pub disconnect_after_ms: i64,
    /// ET minute the first record must land on to open HEALTHY (09:30).
    pub session_open_minute_et: u16,
}

impl Default for HealthConfig {
    fn default() -> Self {
        HealthConfig {
            expected_interval_ms: 60_000,
            stale_after_ms: 180_000,
            disconnect_after_ms: 600_000,
            session_open_minute_et: SESSION_OPEN_MINUTE_ET,
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

/// Incremental DataHealth evaluator. Feed it each record's `minute_et` +
/// `occurred_at_utc_ms` in arrival order; query `status()`/`state()`.
#[derive(Debug, Clone)]
pub struct DataHealthMachine {
    cfg: HealthConfig,
    status: DataHealth,
    last_bar_ms: Option<i64>,
    last_lag_ms: u64,
    out_of_order_count: u64,
    reconnect_count: u64,
    reason: Option<&'static str>,
    /// Sticky: once a gap/out-of-order/reconnect/late-start degrades the feed,
    /// HEALTHY is unreachable until `mark_reconciled()` confirms backfill.
    needs_reconcile: bool,
    /// occurred_at_utc of the last observed record, for stamping health records.
    last_occurred_at_utc: Option<String>,
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
            needs_reconcile: false,
            last_occurred_at_utc: None,
        }
    }

    pub fn status(&self) -> DataHealth {
        self.status
    }

    /// occurred_at_utc of the last observed record (None before the first).
    pub fn last_occurred_at_utc(&self) -> Option<&str> {
        self.last_occurred_at_utc.as_deref()
    }

    /// Observe a record and remember its occurred_at_utc string for stamping.
    pub fn observe_bar_at(
        &mut self,
        minute_et: u16,
        occurred_at_utc_ms: i64,
        occurred_at_utc: &str,
    ) {
        self.observe_bar(minute_et, occurred_at_utc_ms);
        self.last_occurred_at_utc = Some(occurred_at_utc.to_owned());
    }

    pub fn out_of_order_count(&self) -> u64 {
        self.out_of_order_count
    }

    pub fn reconnect_count(&self) -> u64 {
        self.reconnect_count
    }

    /// True while a gap/late-start/reconnect awaits explicit reconciliation.
    pub fn needs_reconcile(&self) -> bool {
        self.needs_reconcile
    }

    /// Confirm backfill/reconciliation completed. Clears the sticky flag so the
    /// next well-spaced record can return the feed to HEALTHY. This is the ONLY
    /// way out of the degraded-after-gap state — a self-healed cadence is not.
    pub fn mark_reconciled(&mut self) {
        self.needs_reconcile = false;
    }

    /// Record a stream reconnection: counts it, drops to RECONCILING, and sets
    /// the sticky flag so a single post-reconnect record cannot clear it.
    pub fn observe_reconnect(&mut self) {
        self.reconnect_count += 1;
        self.status = DataHealth::Reconciling;
        self.reason = Some("reconnecting");
        self.needs_reconcile = true;
    }

    /// Ingest one record's ET minute + business timestamp (arrival order).
    pub fn observe_bar(&mut self, minute_et: u16, occurred_at_utc_ms: i64) {
        match self.last_bar_ms {
            None => {
                // First record. It must land on the fixed session open to open
                // HEALTHY; a late start stays RECONCILING pending backfill.
                self.last_lag_ms = 0;
                self.last_bar_ms = Some(occurred_at_utc_ms);
                if minute_et == self.cfg.session_open_minute_et {
                    self.status = DataHealth::Healthy;
                    self.reason = None;
                } else {
                    self.status = DataHealth::Reconciling;
                    self.reason = Some("late_start_no_session_open");
                    self.needs_reconcile = true;
                }
            }
            Some(last) if occurred_at_utc_ms <= last => {
                // Out of order or duplicate: count it, degrade, do not advance.
                self.out_of_order_count += 1;
                self.status = DataHealth::Degraded;
                self.reason = Some("out_of_order");
                self.needs_reconcile = true;
            }
            Some(last) => {
                let gap = occurred_at_utc_ms - last;
                self.last_lag_ms = gap.max(0) as u64;
                self.last_bar_ms = Some(occurred_at_utc_ms);
                if gap > self.cfg.expected_interval_ms {
                    // Any gap is sticky: recovery requires explicit reconciliation.
                    self.needs_reconcile = true;
                }
                self.status = if gap > self.cfg.disconnect_after_ms {
                    self.reason = Some("disconnected_gap");
                    DataHealth::Disconnected
                } else if gap > self.cfg.stale_after_ms {
                    self.reason = Some("stale_gap");
                    DataHealth::Stale
                } else if gap > self.cfg.expected_interval_ms {
                    self.reason = Some("minute_gap");
                    DataHealth::Degraded
                } else if self.needs_reconcile {
                    // Cadence is fine, but an earlier fault has not been
                    // reconciled — stay DEGRADED, do not silently self-heal.
                    self.reason = Some("awaiting_reconciliation");
                    DataHealth::Degraded
                } else {
                    self.reason = None;
                    DataHealth::Healthy
                };
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
    const OPEN: u16 = SESSION_OPEN_MINUTE_ET; // 570

    /// ET minute for the i-th minute after open, and its ms since open.
    fn at(i: i64) -> (u16, i64) {
        ((OPEN as i64 + i) as u16, i * MIN)
    }

    #[test]
    fn starts_reconciling_and_session_open_bar_goes_healthy() {
        let mut machine = DataHealthMachine::new(HealthConfig::default());
        assert_eq!(machine.status(), DataHealth::Reconciling);
        let (m, ms) = at(0);
        machine.observe_bar(m, ms);
        assert_eq!(machine.status(), DataHealth::Healthy);
    }

    #[test]
    fn late_start_stays_reconciling_until_reconciled() {
        // Data begins at 10:00 (not 09:30): must not open HEALTHY.
        let mut machine = DataHealthMachine::new(HealthConfig::default());
        machine.observe_bar(OPEN + 30, 30 * MIN);
        assert_eq!(machine.status(), DataHealth::Reconciling);
        assert!(machine.needs_reconcile());
        // A well-spaced next bar alone does NOT heal it.
        machine.observe_bar(OPEN + 31, 31 * MIN);
        assert_eq!(machine.status(), DataHealth::Degraded);
        // Only explicit reconciliation lets it return to HEALTHY.
        machine.mark_reconciled();
        machine.observe_bar(OPEN + 32, 32 * MIN);
        assert_eq!(machine.status(), DataHealth::Healthy);
    }

    #[test]
    fn steady_one_minute_cadence_from_open_stays_healthy() {
        let mut machine = DataHealthMachine::new(HealthConfig::default());
        for i in 0..5 {
            let (m, ms) = at(i);
            machine.observe_bar(m, ms);
            assert_eq!(machine.status(), DataHealth::Healthy);
        }
        assert_eq!(machine.out_of_order_count(), 0);
    }

    #[test]
    fn gap_degrades_and_stays_degraded_until_reconciled() {
        let mut machine = DataHealthMachine::new(HealthConfig::default());
        let (m0, ms0) = at(0);
        machine.observe_bar(m0, ms0);
        // skip a minute: 2-min gap -> degraded, sticky
        machine.observe_bar(OPEN + 2, 2 * MIN);
        assert_eq!(machine.status(), DataHealth::Degraded);
        assert!(machine.needs_reconcile());
        // next well-spaced bar must NOT self-heal to HEALTHY (the key fix)
        machine.observe_bar(OPEN + 3, 3 * MIN);
        assert_eq!(machine.status(), DataHealth::Degraded);
        assert_eq!(
            machine.state("t").reason.as_deref(),
            Some("awaiting_reconciliation")
        );
        // reconciliation confirmed -> next bar recovers
        machine.mark_reconciled();
        machine.observe_bar(OPEN + 4, 4 * MIN);
        assert_eq!(machine.status(), DataHealth::Healthy);
    }

    #[test]
    fn large_gap_goes_stale_then_disconnected() {
        let mut machine = DataHealthMachine::new(HealthConfig::default());
        let (m0, ms0) = at(0);
        machine.observe_bar(m0, ms0);
        machine.observe_bar(OPEN + 5, 5 * MIN); // 5 min gap -> stale
        assert_eq!(machine.status(), DataHealth::Stale);
        machine.observe_bar(OPEN + 25, 25 * MIN); // 20 min gap -> disconnected
        assert_eq!(machine.status(), DataHealth::Disconnected);
    }

    #[test]
    fn out_of_order_is_counted_and_stays_degraded_until_reconciled() {
        let mut machine = DataHealthMachine::new(HealthConfig::default());
        machine.observe_bar(OPEN + 3, 3 * MIN);
        machine.observe_bar(OPEN + 2, 2 * MIN); // older than last
        assert_eq!(machine.status(), DataHealth::Degraded);
        assert_eq!(machine.out_of_order_count(), 1);
        assert!(machine.needs_reconcile());
        // next in-order bar does NOT self-heal (out-of-order is a real fault)
        machine.observe_bar(OPEN + 4, 4 * MIN);
        assert_eq!(machine.status(), DataHealth::Degraded);
        machine.mark_reconciled();
        machine.observe_bar(OPEN + 5, 5 * MIN);
        assert_eq!(machine.status(), DataHealth::Healthy);
    }

    #[test]
    fn reconnect_needs_reconciliation_not_just_one_record() {
        let mut machine = DataHealthMachine::new(HealthConfig::default());
        let (m0, ms0) = at(0);
        machine.observe_bar(m0, ms0);
        machine.observe_reconnect();
        assert_eq!(machine.status(), DataHealth::Reconciling);
        assert_eq!(machine.reconnect_count(), 1);
        // one record after reconnect does NOT clear RECONCILING
        machine.observe_bar(OPEN + 1, MIN);
        assert_eq!(machine.status(), DataHealth::Degraded);
        machine.mark_reconciled();
        machine.observe_bar(OPEN + 2, 2 * MIN);
        assert_eq!(machine.status(), DataHealth::Healthy);
    }

    #[test]
    fn silence_watchdog_escalates_without_new_bars() {
        let mut machine = DataHealthMachine::new(HealthConfig::default());
        let (m0, ms0) = at(0);
        machine.observe_bar(m0, ms0);
        machine.observe_silence(2 * MIN);
        assert_eq!(machine.status(), DataHealth::Degraded);
        machine.observe_silence(5 * MIN);
        assert_eq!(machine.status(), DataHealth::Stale);
        machine.observe_silence(30 * MIN);
        assert_eq!(machine.status(), DataHealth::Disconnected);
    }
}
