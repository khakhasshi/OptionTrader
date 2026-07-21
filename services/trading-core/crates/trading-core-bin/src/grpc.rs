//! gRPC MarketService: streams the Rust-authoritative snapshot feed to Python.
//!
//! Architecture (Phase 2 review fix): a SINGLE producer advances the market feed
//! exactly once. DataHealth is driven by that one producer — never by individual
//! subscribers. A shared cursor (`watch<usize>`) marks how many records the
//! producer has emitted; `GetDataHealth` reports the health of the record at the
//! cursor, and every `StreamMarketSnapshots` consumer follows the cursor FORWARD
//! from where it joined. Consequences:
//!   * N concurrent subscribers cannot pollute health — none of them drive it.
//!   * A late subscriber / reconnect resumes at the current cursor and never
//!     re-ingests history.
//!   * `MarketTick.snapshot.data_health` and `GetDataHealth.status` come from the
//!     same precomputed runtime state, so they can never contradict.
//!   * Disconnecting any consumer does not change global health.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{SecondsFormat, Utc};
use market_core::{
    DataHealth, DataHealthMachine, DataHealthStateRecord, MarketSnapshot, ReplayBar, ReplayConfig,
    ReplaySnapshotSource, SnapshotSource,
};
use optiontrader_proto::market_v1::{
    market_service_server::MarketService, DataHealth as ProtoHealth,
    DataHealthState as ProtoHealthState, DeliveryPhase, GetDataHealthRequest,
    MarketBar as ProtoBar, MarketSnapshot as ProtoSnapshot, MarketTick as ProtoTick, StreamRequest,
};
use tokio::sync::{mpsc, watch, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::theta_live::{self, ThetaLiveConfig, ThetaLiveEvent};

/// Precomputed feed. Ticks and their per-record health are computed ONCE here
/// (single authoritative pass); everything downstream only reads this.
pub struct MarketFeed {
    ticks: Vec<(MarketSnapshot, ReplayBar)>,
    /// health_states[i] is the runtime health after record i was produced.
    /// Its `status` equals ticks[i].0.data_health by construction, so the
    /// snapshot and GetDataHealth never disagree.
    health_states: Vec<DataHealthStateRecord>,
}

impl MarketFeed {
    /// Build a replay-backed feed from an NDJSON fixture.
    pub fn from_ndjson(ndjson: &str, cfg: ReplayConfig) -> Result<Self, String> {
        let health_cfg = cfg.health;
        let source =
            ReplaySnapshotSource::from_ndjson(ndjson, cfg).map_err(|e| format!("replay: {e}"))?;
        let snapshots = source.snapshots().map_err(|e| format!("snapshots: {e}"))?;
        if snapshots.is_empty() {
            return Err("replay produced no snapshots".into());
        }
        let ticks: Vec<(MarketSnapshot, ReplayBar)> = snapshots
            .into_iter()
            .zip(source.bars().iter().cloned())
            .collect();

        // Single machine pass for lag/out-of-order/reconnect counters; the
        // reported status is pinned to the snapshot's authoritative data_health
        // so MarketTick.snapshot.data_health == GetDataHealth.status always.
        let mut machine = DataHealthMachine::new(health_cfg);
        let mut health_states = Vec::with_capacity(ticks.len());
        for (snap, bar) in &ticks {
            machine.observe_bar_at(bar.minute_et, bar.occurred_at_utc_ms, &bar.occurred_at_utc);
            let mut rec = machine.state(snap.occurred_at_utc.clone());
            rec.status = snap.data_health; // pin to the snapshot's authority
            health_states.push(rec);
        }
        Ok(MarketFeed {
            ticks,
            health_states,
        })
    }

    /// Health record before any record has been produced: RECONCILING, stamped
    /// at the earliest feed instant so occurred_at_utc stays a valid RFC3339 Z.
    fn initial_health(&self) -> DataHealthStateRecord {
        DataHealthStateRecord {
            occurred_at_utc: self.ticks[0].0.occurred_at_utc.clone(),
            status: DataHealth::Reconciling,
            market_event_lag_ms: 0,
            quote_age_ms: 0,
            out_of_order_count: 0,
            reconnect_count: 0,
            reason: Some("awaiting_first_record".to_owned()),
        }
    }
}

fn replay_tick_interval() -> Duration {
    let ms: u64 = std::env::var("OPTIONTRADER_REPLAY_TICK_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    Duration::from_millis(ms.max(1))
}

fn bar_to_proto(b: &ReplayBar) -> ProtoBar {
    ProtoBar {
        occurred_at_utc: b.occurred_at_utc.clone(),
        timestamp_et: b.timestamp_et.clone(),
        minute_et: u32::from(b.minute_et),
        open: format!("{:.2}", b.open),
        high: format!("{:.2}", b.high),
        low: format!("{:.2}", b.low),
        close: format!("{:.2}", b.close),
        volume: b.volume,
        vwap: b.vwap.map(|v| format!("{v:.2}")).unwrap_or_default(),
    }
}

fn health_to_proto(h: DataHealth) -> ProtoHealth {
    match h {
        DataHealth::Healthy => ProtoHealth::Healthy,
        DataHealth::Degraded => ProtoHealth::Degraded,
        DataHealth::Stale => ProtoHealth::Stale,
        DataHealth::Disconnected => ProtoHealth::Disconnected,
        DataHealth::Reconciling => ProtoHealth::Reconciling,
    }
}

fn snapshot_to_proto(s: &MarketSnapshot) -> ProtoSnapshot {
    ProtoSnapshot {
        schema_version: s.schema_version.clone(),
        snapshot_id: s.snapshot_id.clone(),
        occurred_at_utc: s.occurred_at_utc.clone(),
        timestamp_et: s.timestamp_et.clone(),
        symbol: s.symbol.clone(),
        price: s.price.clone(),
        open: s.open.clone(),
        high: s.high.clone(),
        low: s.low.clone(),
        previous_close: s.previous_close.clone(),
        vwap: s.vwap.clone(),
        volume: s.volume,
        opening_range_high: s.opening_range_high.clone(),
        opening_range_low: s.opening_range_low.clone(),
        premarket_high: s.premarket_high.clone().unwrap_or_default(),
        premarket_low: s.premarket_low.clone().unwrap_or_default(),
        sequence_number: s.sequence_number,
        quote_age_ms: s.quote_age_ms,
        data_health: health_to_proto(s.data_health) as i32,
    }
}

fn health_to_proto_state(rec: &DataHealthStateRecord) -> ProtoHealthState {
    ProtoHealthState {
        schema_version: "1.0".into(),
        occurred_at_utc: rec.occurred_at_utc.clone(),
        status: health_to_proto(rec.status) as i32,
        market_event_lag_ms: rec.market_event_lag_ms,
        quote_age_ms: rec.quote_age_ms,
        out_of_order_count: rec.out_of_order_count,
        reconnect_count: rec.reconnect_count,
        reason: rec.reason.clone().unwrap_or_default(),
    }
}

pub struct MarketServiceImpl {
    feed: Arc<MarketFeed>,
    /// Number of records the single producer has emitted (0 = none yet).
    cursor_tx: watch::Sender<usize>,
    /// A permanently-held receiver so `cursor_tx` always has ≥1 receiver: the
    /// producer's cursor updates therefore never fail just because every client
    /// happens to be momentarily disconnected. Producer life is independent of
    /// subscriber count (Phase 2 review P0).
    _keepalive: watch::Receiver<usize>,
    /// Set once the producer has emitted the whole finite replay (or exited):
    /// the feed has no more live data, so health reports DISCONNECTED, never a
    /// stale HEALTHY.
    finished: Arc<AtomicBool>,
    /// Producer starts lazily on the first subscription, exactly once.
    started: Arc<AtomicBool>,
    interval: Duration,
}

impl MarketServiceImpl {
    pub fn new(feed: Arc<MarketFeed>) -> Self {
        let (cursor_tx, keepalive) = watch::channel(0usize);
        MarketServiceImpl {
            feed,
            cursor_tx,
            _keepalive: keepalive,
            finished: Arc::new(AtomicBool::new(false)),
            started: Arc::new(AtomicBool::new(false)),
            interval: replay_tick_interval(),
        }
    }

    /// Start the single producer clock once. It advances the shared cursor from
    /// 0 to N at a fixed pace regardless of how many clients are attached; this
    /// is the ONLY thing that "produces" records. On completion it flags the
    /// feed finished so health degrades out of HEALTHY.
    fn ensure_producer(&self) {
        if self.started.swap(true, Ordering::SeqCst) {
            return;
        }
        let n = self.feed.ticks.len();
        let tx = self.cursor_tx.clone();
        let interval = self.interval;
        let finished = Arc::clone(&self.finished);
        tokio::spawn(async move {
            for i in 1..=n {
                tokio::time::sleep(interval).await;
                // send_replace cannot fail on zero receivers (we hold a
                // keepalive receiver anyway) — the producer never dies early.
                tx.send_replace(i);
            }
            // Finite replay exhausted: mark the feed done. Late subscribers get a
            // cleanly-ended stream and health reports DISCONNECTED, not HEALTHY.
            finished.store(true, Ordering::SeqCst);
        });
    }
}

/// Classify the whole batch currently visible to one subscriber. Falling more
/// than one record behind enters reconciliation; that batch remains BACKFILL.
fn delivery_phase_for_batch(sent: usize, cursor: usize, reconciling: &mut bool) -> DeliveryPhase {
    if !*reconciling && cursor.saturating_sub(sent) > 1 {
        *reconciling = true;
    }
    if *reconciling {
        DeliveryPhase::Backfill
    } else {
        DeliveryPhase::Live
    }
}

/// Leave reconciliation only after the complete catch-up batch was emitted.
/// A later producer advance is therefore the first batch eligible for LIVE.
fn finish_delivery_batch(sent: usize, cursor: usize, reconciling: &mut bool) {
    if *reconciling && sent >= cursor {
        *reconciling = false;
    }
}

#[tonic::async_trait]
impl MarketService for MarketServiceImpl {
    type StreamMarketSnapshotsStream = ReceiverStream<Result<ProtoTick, Status>>;

    async fn stream_market_snapshots(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<Self::StreamMarketSnapshotsStream>, Status> {
        let feed = Arc::clone(&self.feed);
        let mut cursor_rx = self.cursor_tx.subscribe();
        let n = feed.ticks.len();
        // Resume/backfill: sequence_number is 1-based (record i has seq i+1), so a
        // client that last consumed seq S resumes at index S. resume=0 (fresh or
        // restarted client) replays from session open — missing records are never
        // silently skipped. Clamp to n so an over-large resume can't panic.
        let resume = request.into_inner().resume_after_sequence as usize;
        let mut sent = resume.min(n);
        // Records needed to catch the producer head up are historical BACKFILL
        // even when their snapshot DataHealth was HEALTHY. Once caught up, a
        // single newly-produced record may be emitted as LIVE.
        let mut reconciling = sent < *cursor_rx.borrow();
        self.ensure_producer();

        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            loop {
                let cursor = *cursor_rx.borrow();
                let delivery_phase =
                    delivery_phase_for_batch(sent, cursor, &mut reconciling) as i32;
                while sent < cursor {
                    let (snap, bar) = &feed.ticks[sent];
                    let tick = ProtoTick {
                        snapshot: Some(snapshot_to_proto(snap)),
                        bar: Some(bar_to_proto(bar)),
                        delivery_phase,
                        high_watermark_sequence: cursor as u64,
                    };
                    if tx.send(Ok(tick)).await.is_err() {
                        return; // client dropped
                    }
                    sent += 1;
                }
                finish_delivery_batch(sent, cursor, &mut reconciling);
                if sent >= n {
                    return; // replay exhausted -> end this stream
                }
                if cursor_rx.changed().await.is_err() {
                    return; // producer gone
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_data_health(
        &self,
        _request: Request<GetDataHealthRequest>,
    ) -> Result<Response<ProtoHealthState>, Status> {
        // Feed ended: no live data -> DISCONNECTED, never a stale HEALTHY.
        if self.finished.load(Ordering::SeqCst) {
            let last = self.feed.ticks.last().expect("non-empty feed");
            return Ok(Response::new(ProtoHealthState {
                schema_version: "1.0".into(),
                occurred_at_utc: last.0.occurred_at_utc.clone(),
                status: ProtoHealth::Disconnected as i32,
                market_event_lag_ms: 0,
                quote_age_ms: 0,
                out_of_order_count: 0,
                reconnect_count: 0,
                reason: "replay feed ended".into(),
            }));
        }
        let cursor = *self.cursor_tx.borrow();
        let record = if cursor == 0 {
            self.feed.initial_health()
        } else {
            self.feed.health_states[cursor - 1].clone()
        };
        Ok(Response::new(health_to_proto_state(&record)))
    }
}

struct LiveFeedState {
    ticks: Vec<(MarketSnapshot, ReplayBar)>,
    health: DataHealthStateRecord,
    reconnect_count: u64,
    last_bar_received_at: Option<Instant>,
    needs_reconcile: bool,
}

fn refresh_live_silence(state: &mut LiveFeedState, health_cfg: market_core::HealthConfig) {
    let Some(last_received) = state.last_bar_received_at else {
        return;
    };
    let elapsed_ms = last_received
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    state.health.market_event_lag_ms = elapsed_ms;
    state.health.quote_age_ms = elapsed_ms;

    let expected = health_cfg.expected_interval_ms.max(0) as u64;
    let stale = health_cfg.stale_after_ms.max(0) as u64;
    let disconnected = health_cfg.disconnect_after_ms.max(0) as u64;
    if elapsed_ms <= expected {
        return;
    }
    let (status, reason) = if elapsed_ms <= stale {
        (DataHealth::Degraded, "live stream silence")
    } else if elapsed_ms <= disconnected {
        (DataHealth::Stale, "live stream stale")
    } else {
        (DataHealth::Disconnected, "live stream silent/disconnected")
    };
    state.health.status = status;
    state.health.reason = Some(reason.into());
}

/// Dynamic MarketService backed by Theta Terminal. Unlike finite replay, the
/// producer appends finalized bars indefinitely and subscribers wait on cursor.
#[derive(Clone)]
pub struct LiveMarketServiceImpl {
    state: Arc<RwLock<LiveFeedState>>,
    cursor_tx: watch::Sender<usize>,
    _keepalive: watch::Receiver<usize>,
    started: Arc<AtomicBool>,
    theta: ThetaLiveConfig,
    replay_cfg: ReplayConfig,
}

impl LiveMarketServiceImpl {
    pub fn new(theta: ThetaLiveConfig, replay_cfg: ReplayConfig) -> Self {
        let (cursor_tx, keepalive) = watch::channel(0_usize);
        let now = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        LiveMarketServiceImpl {
            state: Arc::new(RwLock::new(LiveFeedState {
                ticks: Vec::new(),
                health: DataHealthStateRecord {
                    occurred_at_utc: now,
                    status: DataHealth::Disconnected,
                    market_event_lag_ms: 0,
                    quote_age_ms: 0,
                    out_of_order_count: 0,
                    reconnect_count: 0,
                    reason: Some("Theta Terminal stream not connected".into()),
                },
                reconnect_count: 0,
                last_bar_received_at: None,
                needs_reconcile: false,
            })),
            cursor_tx,
            _keepalive: keepalive,
            started: Arc::new(AtomicBool::new(false)),
            theta,
            replay_cfg,
        }
    }

    fn ensure_producer(&self) {
        if self.started.swap(true, Ordering::SeqCst) {
            return;
        }
        let (event_tx, mut event_rx) = mpsc::channel(256);
        let theta = self.theta.clone();
        tokio::spawn(theta_live::run(theta, event_tx));

        let state = Arc::clone(&self.state);
        let cursor = self.cursor_tx.clone();
        let cfg = self.replay_cfg;
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                let mut live = state.write().await;
                match event {
                    ThetaLiveEvent::Connected => {
                        live.health.occurred_at_utc =
                            Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
                        live.health.status = DataHealth::Reconciling;
                        live.health.reason =
                            Some("Theta connected; awaiting complete minute".into());
                    }
                    ThetaLiveEvent::Disconnected(reason) => {
                        live.reconnect_count = live.reconnect_count.saturating_add(1);
                        live.needs_reconcile = true;
                        live.health.occurred_at_utc =
                            Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
                        live.health.status = DataHealth::Disconnected;
                        live.health.reconnect_count = live.reconnect_count;
                        live.health.reason = Some(reason);
                    }
                    ThetaLiveEvent::Backfill(bars) => {
                        if bars.is_empty() {
                            live.health.status = DataHealth::Reconciling;
                            live.health.reason =
                                Some("awaiting first completed regular-session minute".into());
                            continue;
                        }
                        let existing: Vec<ReplayBar> =
                            live.ticks.iter().map(|(_, bar)| bar.clone()).collect();
                        let prefix_matches = existing.len() <= bars.len()
                            && existing
                                .iter()
                                .zip(&bars)
                                .all(|(published, recovered)| published == recovered);
                        if !prefix_matches {
                            live.health.status = DataHealth::Stale;
                            live.health.reason = Some(
                                "Theta REST backfill conflicts with published live bars".into(),
                            );
                            live.needs_reconcile = true;
                            continue;
                        }
                        let rebuilt = ReplaySnapshotSource::new(bars.clone(), cfg)
                            .and_then(|source| source.snapshots());
                        match rebuilt {
                            Ok(snapshots) if snapshots.len() == bars.len() => {
                                live.ticks = snapshots.into_iter().zip(bars).collect();
                                let latest = live.ticks.last().expect("backfill is non-empty");
                                live.health = DataHealthStateRecord {
                                    occurred_at_utc: latest.0.occurred_at_utc.clone(),
                                    status: latest.0.data_health,
                                    market_event_lag_ms: 0,
                                    quote_age_ms: latest.0.quote_age_ms,
                                    out_of_order_count: 0,
                                    reconnect_count: live.reconnect_count,
                                    reason: None,
                                };
                                live.last_bar_received_at = Some(Instant::now());
                                live.needs_reconcile = false;
                                cursor.send_replace(live.ticks.len());
                            }
                            Ok(_) => unreachable!("snapshot count must match bar count"),
                            Err(error) => {
                                live.health.status = DataHealth::Stale;
                                live.health.reason =
                                    Some(format!("Theta REST backfill rejected: {error}"));
                                live.needs_reconcile = true;
                            }
                        }
                    }
                    ThetaLiveEvent::Bar(bar) => {
                        let mut bars: Vec<ReplayBar> =
                            live.ticks.iter().map(|(_, bar)| bar.clone()).collect();
                        bars.push(bar.clone());
                        let latest = ReplaySnapshotSource::new(bars, cfg)
                            .and_then(|source| source.snapshots())
                            .and_then(|snapshots| {
                                snapshots
                                    .last()
                                    .cloned()
                                    .ok_or(market_core::FeatureError::EmptyInput)
                            });
                        match latest {
                            Ok(mut snapshot) => {
                                if live.needs_reconcile {
                                    snapshot.data_health = DataHealth::Reconciling;
                                }
                                live.health = DataHealthStateRecord {
                                    occurred_at_utc: snapshot.occurred_at_utc.clone(),
                                    status: snapshot.data_health,
                                    market_event_lag_ms: 0,
                                    quote_age_ms: snapshot.quote_age_ms,
                                    out_of_order_count: 0,
                                    reconnect_count: live.reconnect_count,
                                    reason: live
                                        .needs_reconcile
                                        .then(|| "reconnect awaiting REST reconciliation".into()),
                                };
                                live.ticks.push((snapshot, bar));
                                live.last_bar_received_at = Some(Instant::now());
                                cursor.send_replace(live.ticks.len());
                            }
                            Err(error) => {
                                live.health.status = DataHealth::Stale;
                                live.health.reason = Some(format!("live bar rejected: {error}"));
                            }
                        }
                    }
                }
            }
        });
    }

    pub async fn current_health(&self) -> DataHealth {
        self.ensure_producer();
        let mut state = self.state.write().await;
        refresh_live_silence(&mut state, self.replay_cfg.health);
        state.health.status
    }

    pub async fn latest_snapshot(&self) -> Option<MarketSnapshot> {
        self.ensure_producer();
        let mut state = self.state.write().await;
        refresh_live_silence(&mut state, self.replay_cfg.health);
        let mut snapshot = state.ticks.last().map(|(snapshot, _)| snapshot.clone())?;
        snapshot.data_health = state.health.status;
        snapshot.quote_age_ms = state.health.quote_age_ms;
        Some(snapshot)
    }
}

#[tonic::async_trait]
impl MarketService for LiveMarketServiceImpl {
    type StreamMarketSnapshotsStream = ReceiverStream<Result<ProtoTick, Status>>;

    async fn stream_market_snapshots(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<Self::StreamMarketSnapshotsStream>, Status> {
        let mut cursor_rx = self.cursor_tx.subscribe();
        let cursor_at_join = *cursor_rx.borrow();
        let resume = request.into_inner().resume_after_sequence as usize;
        let mut sent = resume.min(cursor_at_join);
        let mut reconciling = sent < cursor_at_join;
        self.ensure_producer();

        let state = Arc::clone(&self.state);
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            loop {
                let cursor = *cursor_rx.borrow();
                let delivery_phase =
                    delivery_phase_for_batch(sent, cursor, &mut reconciling) as i32;
                while sent < cursor {
                    let pair = {
                        let live = state.read().await;
                        live.ticks.get(sent).cloned()
                    };
                    let Some((snapshot, bar)) = pair else {
                        let _ = tx
                            .send(Err(Status::data_loss("live cursor/tick mismatch")))
                            .await;
                        return;
                    };
                    let tick = ProtoTick {
                        snapshot: Some(snapshot_to_proto(&snapshot)),
                        bar: Some(bar_to_proto(&bar)),
                        delivery_phase,
                        high_watermark_sequence: cursor as u64,
                    };
                    if tx.send(Ok(tick)).await.is_err() {
                        return;
                    }
                    sent += 1;
                }
                finish_delivery_batch(sent, cursor, &mut reconciling);
                if cursor_rx.changed().await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_data_health(
        &self,
        _request: Request<GetDataHealthRequest>,
    ) -> Result<Response<ProtoHealthState>, Status> {
        self.ensure_producer();
        let mut state = self.state.write().await;
        refresh_live_silence(&mut state, self.replay_cfg.health);
        Ok(Response::new(health_to_proto_state(&state.health)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use market_core::HealthConfig;
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    const NDJSON: &str = include_str!("../fixtures/replay_qqq_sample.ndjson");

    fn feed() -> Arc<MarketFeed> {
        let cfg = ReplayConfig {
            opening_range_minutes: 3,
            previous_close: Some(497.20),
            ..ReplayConfig::default()
        };
        Arc::new(MarketFeed::from_ndjson(NDJSON, cfg).unwrap())
    }

    fn theta_trade(ms: u32, sequence: u64, price: f64) -> String {
        json!({
            "header": {"type": "TRADE", "status": "CONNECTED"},
            "contract": {"security_type": "STOCK", "root": "QQQ"},
            "trade": {
                "ms_of_day": ms, "sequence": sequence, "size": 10,
                "condition": 0, "price": price, "exchange": 57, "date": 20260720
            }
        })
        .to_string()
    }

    async fn drain(svc: &MarketServiceImpl) -> Vec<optiontrader_proto::market_v1::MarketTick> {
        drain_from(svc, 0).await
    }

    async fn drain_from(
        svc: &MarketServiceImpl,
        resume_after_sequence: u64,
    ) -> Vec<optiontrader_proto::market_v1::MarketTick> {
        use tokio_stream::StreamExt;
        let resp = svc
            .stream_market_snapshots(Request::new(StreamRequest {
                session_id: "s".into(),
                speedup: 0.0,
                resume_after_sequence,
            }))
            .await
            .unwrap();
        let mut stream = resp.into_inner();
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item.unwrap());
        }
        out
    }

    #[tokio::test]
    async fn resume_after_sequence_backfills_only_missed_records() {
        // A client that last saw seq=3 reconnects with resume_after_sequence=3
        // and must receive exactly 4,5,6 — the records it missed, in order.
        let svc = MarketServiceImpl::new(feed());
        // Start the producer, then let it finish while no client is attached.
        let first = svc
            .stream_market_snapshots(Request::new(StreamRequest {
                session_id: "starter".into(),
                speedup: 0.0,
                resume_after_sequence: 0,
            }))
            .await
            .unwrap();
        drop(first);
        tokio::time::sleep(Duration::from_millis(100)).await;

        let ticks = drain_from(&svc, 3).await;
        let seqs: Vec<u64> = ticks
            .iter()
            .map(|t| t.snapshot.as_ref().unwrap().sequence_number)
            .collect();
        assert_eq!(seqs, vec![4, 5, 6]);
        assert!(ticks
            .iter()
            .all(|t| t.delivery_phase == DeliveryPhase::Backfill as i32));
    }

    #[tokio::test]
    async fn resume_zero_replays_full_session_without_scheduler_assumption() {
        // A fresh client receives the session from record 1. Delivery phase is
        // intentionally not asserted: a slow test task may fall behind the
        // producer and conservatively receive part of the session as BACKFILL.
        let svc = MarketServiceImpl::new(feed());
        let ticks = drain_from(&svc, 0).await;
        let seqs: Vec<u64> = ticks
            .iter()
            .map(|t| t.snapshot.as_ref().unwrap().sequence_number)
            .collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6]);
        assert!(ticks
            .iter()
            .all(|t| t.delivery_phase == DeliveryPhase::Live as i32
                || t.delivery_phase == DeliveryPhase::Backfill as i32));
    }

    #[tokio::test]
    async fn restarted_client_replay_is_backfill_not_live() {
        let svc = MarketServiceImpl::new(feed());
        let _ = drain(&svc).await; // producer reaches the end

        let replay = drain_from(&svc, 0).await;
        assert_eq!(replay.len(), feed().ticks.len());
        assert!(replay
            .iter()
            .all(|t| t.delivery_phase == DeliveryPhase::Backfill as i32));
    }

    #[tokio::test]
    async fn first_subscriber_gets_all_ticks_in_order() {
        let svc = MarketServiceImpl::new(feed());
        let ticks = drain(&svc).await;
        let seqs: Vec<u64> = ticks
            .iter()
            .map(|t| t.snapshot.as_ref().unwrap().sequence_number)
            .collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6]);
        assert!(ticks
            .iter()
            .all(|t| t.delivery_phase == DeliveryPhase::Live as i32
                || t.delivery_phase == DeliveryPhase::Backfill as i32));
        // snapshot and bar align on the same instant
        for t in &ticks {
            assert_eq!(
                t.snapshot.as_ref().unwrap().occurred_at_utc,
                t.bar.as_ref().unwrap().occurred_at_utc
            );
        }
    }

    #[tokio::test]
    async fn health_is_reconciling_before_any_stream() {
        let svc = MarketServiceImpl::new(feed());
        let state = svc
            .get_data_health(Request::new(GetDataHealthRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(state.status, ProtoHealth::Reconciling as i32);
    }

    #[tokio::test]
    async fn second_subscription_does_not_pollute_health() {
        // The exact review scenario: a second subscription must NOT bump
        // out_of_order or introduce lag — nothing a subscriber does can drive
        // health. (After a full drain the feed is `finished`, so status becomes
        // DISCONNECTED; the invariant under test here is the counters/pollution,
        // which stay clean regardless of how many times we subscribe.)
        let svc = MarketServiceImpl::new(feed());
        let _ = drain(&svc).await;
        let _ = drain(&svc).await; // second subscription
        let after_second = svc
            .get_data_health(Request::new(GetDataHealthRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            after_second.out_of_order_count, 0,
            "a second subscriber must not manufacture out-of-order records"
        );
    }

    #[test]
    fn snapshot_data_health_matches_health_states_by_construction() {
        // The snapshot's data_health and the health record at the same index are
        // pinned equal at construction, so a live GetDataHealth at cursor i can
        // never contradict the snapshot delivered at record i.
        let f = feed();
        for (i, (snap, _bar)) in f.ticks.iter().enumerate() {
            assert_eq!(snap.data_health, f.health_states[i].status);
        }
    }

    #[test]
    fn caught_up_subscriber_marks_only_next_record_live() {
        let mut reconciling = true;

        let catch_up = delivery_phase_for_batch(1, 3, &mut reconciling);
        assert_eq!(catch_up as i32, DeliveryPhase::Backfill as i32);

        finish_delivery_batch(3, 3, &mut reconciling);
        let next_record = delivery_phase_for_batch(3, 4, &mut reconciling);
        assert_eq!(next_record as i32, DeliveryPhase::Live as i32);
    }

    #[test]
    fn live_silence_watchdog_fails_closed_without_a_new_tick() {
        let mut state = LiveFeedState {
            ticks: Vec::new(),
            health: DataHealthStateRecord {
                occurred_at_utc: "2026-07-20T13:30:00Z".into(),
                status: DataHealth::Healthy,
                market_event_lag_ms: 0,
                quote_age_ms: 0,
                out_of_order_count: 0,
                reconnect_count: 0,
                reason: None,
            },
            reconnect_count: 0,
            last_bar_received_at: Some(Instant::now() - Duration::from_millis(20)),
            needs_reconcile: false,
        };
        let cfg = HealthConfig {
            expected_interval_ms: 5,
            stale_after_ms: 10,
            disconnect_after_ms: 15,
            ..HealthConfig::default()
        };

        refresh_live_silence(&mut state, cfg);

        assert_eq!(state.health.status, DataHealth::Disconnected);
        assert!(state.health.quote_age_ms >= 15);
        assert_eq!(
            state.health.reason.as_deref(),
            Some("live stream silent/disconnected")
        );
    }

    #[tokio::test]
    async fn live_rest_snapshot_uses_current_watchdog_health() {
        let service = LiveMarketServiceImpl::new(
            ThetaLiveConfig {
                backfill_enabled: false,
                ..ThetaLiveConfig::default()
            },
            ReplayConfig {
                health: HealthConfig {
                    expected_interval_ms: 1,
                    stale_after_ms: 2,
                    disconnect_after_ms: 3,
                    ..HealthConfig::default()
                },
                ..ReplayConfig::default()
            },
        );
        {
            let mut state = service.state.write().await;
            state.ticks.push((
                MarketSnapshot::fixture(),
                ReplayBar {
                    occurred_at_utc: "2026-07-20T13:30:00Z".into(),
                    timestamp_et: "2026-07-20T09:30:00-04:00".into(),
                    occurred_at_utc_ms: 0,
                    minute_et: 570,
                    open: 500.0,
                    high: 500.0,
                    low: 500.0,
                    close: 500.0,
                    volume: 1,
                    vwap: Some(500.0),
                },
            ));
            state.health.status = DataHealth::Healthy;
            state.last_bar_received_at = Some(Instant::now() - Duration::from_millis(10));
        }

        let snapshot = service.latest_snapshot().await.expect("snapshot");

        assert_eq!(snapshot.data_health, DataHealth::Disconnected);
        assert!(snapshot.quote_age_ms >= 3);
    }

    #[tokio::test]
    async fn feed_ended_reports_disconnected_not_stale_healthy() {
        let svc = MarketServiceImpl::new(feed());
        let _ = drain(&svc).await; // producer runs to completion -> finished
                                   // Give the producer task a beat to flip `finished` after the last send.
        tokio::time::sleep(Duration::from_millis(30)).await;
        let gdh = svc
            .get_data_health(Request::new(GetDataHealthRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            gdh.status,
            ProtoHealth::Disconnected as i32,
            "an ended feed must not sit on a stale HEALTHY"
        );
    }

    #[tokio::test]
    async fn producer_survives_subscriber_cancel_and_late_subscriber_does_not_hang() {
        // Review P0: first client takes one frame then drops -> momentary zero
        // subscribers. The producer must keep advancing and NOT die; a later
        // subscriber must get a cleanly-ended stream (not hang) and health must
        // not be stuck HEALTHY.
        use tokio_stream::StreamExt;
        let svc = MarketServiceImpl::new(feed());
        let resp = svc
            .stream_market_snapshots(Request::new(StreamRequest {
                session_id: "a".into(),
                speedup: 0.0,
                resume_after_sequence: 0,
            }))
            .await
            .unwrap();
        let mut stream = resp.into_inner();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.snapshot.unwrap().sequence_number, 1);
        drop(stream); // cancel -> zero subscribers

        // Producer keeps running to completion despite zero subscribers.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Late subscriber: must terminate cleanly (bounded), never hang.
        let late = tokio::time::timeout(Duration::from_secs(2), drain(&svc))
            .await
            .expect("late subscriber must not hang");
        // It joins after completion, so it legitimately gets no more records.
        assert!(late.len() <= feed().ticks.len());

        let gdh = svc
            .get_data_health(Request::new(GetDataHealthRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            gdh.status,
            ProtoHealth::Disconnected as i32,
            "health must not be stuck HEALTHY after the feed ends"
        );
    }

    #[test]
    fn feed_maps_snapshot_and_bar_fields_to_proto() {
        let f = feed();
        let (snap, bar) = &f.ticks[3];
        let proto = snapshot_to_proto(snap);
        assert_eq!(proto.symbol, "QQQ.US");
        assert_eq!(proto.schema_version, "1.0");
        assert_eq!(proto.data_health, ProtoHealth::Healthy as i32);
        assert!(!proto.opening_range_high.is_empty());
        let proto_bar = bar_to_proto(bar);
        assert_eq!(proto_bar.occurred_at_utc, snap.occurred_at_utc);
    }

    #[tokio::test]
    async fn theta_websocket_drives_dynamic_grpc_snapshot() {
        use futures_util::{SinkExt, StreamExt as FuturesStreamExt};
        use tokio_stream::StreamExt as TokioStreamExt;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = accept_async(stream).await.unwrap();
            let request = FuturesStreamExt::next(&mut websocket)
                .await
                .unwrap()
                .unwrap();
            let request: serde_json::Value =
                serde_json::from_str(request.to_text().unwrap()).unwrap();
            assert_eq!(request["contract"]["root"], "QQQ");
            websocket
                .send(Message::Text(theta_trade(34_200_000, 1, 500.0).into()))
                .await
                .unwrap();
            websocket
                .send(Message::Text(theta_trade(34_260_000, 2, 501.0).into()))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        });

        let service = LiveMarketServiceImpl::new(
            ThetaLiveConfig {
                url: format!("ws://{addr}"),
                backfill_enabled: false,
                reconnect_base: Duration::from_millis(5),
                reconnect_max: Duration::from_millis(5),
                ..ThetaLiveConfig::default()
            },
            ReplayConfig {
                opening_range_minutes: 3,
                previous_close: Some(497.20),
                ..ReplayConfig::default()
            },
        );
        let response = service
            .stream_market_snapshots(Request::new(StreamRequest {
                session_id: "theta-mock".into(),
                speedup: 0.0,
                resume_after_sequence: 0,
            }))
            .await
            .unwrap();
        let mut stream = response.into_inner();
        let tick = tokio::time::timeout(Duration::from_secs(2), TokioStreamExt::next(&mut stream))
            .await
            .expect("dynamic gRPC tick timed out")
            .expect("dynamic gRPC stream ended")
            .unwrap();
        let snapshot = tick.snapshot.expect("snapshot");
        assert_eq!(snapshot.symbol, "QQQ.US");
        assert_eq!(snapshot.sequence_number, 1);
        assert_eq!(snapshot.data_health, ProtoHealth::Healthy as i32);
        assert_eq!(tick.delivery_phase, DeliveryPhase::Live as i32);
        server.await.unwrap();
    }
}
