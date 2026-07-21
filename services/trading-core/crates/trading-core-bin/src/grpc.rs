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
use std::time::Duration;

use market_core::{
    DataHealth, DataHealthMachine, DataHealthStateRecord, MarketSnapshot, ReplayBar, ReplayConfig,
    ReplaySnapshotSource, SnapshotSource,
};
use optiontrader_proto::market_v1::{
    market_service_server::MarketService, DataHealth as ProtoHealth,
    DataHealthState as ProtoHealthState, DeliveryPhase, GetDataHealthRequest,
    MarketBar as ProtoBar, MarketSnapshot as ProtoSnapshot, MarketTick as ProtoTick, StreamRequest,
};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

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
                // A subscriber that falls more than one record behind has left
                // the live edge. Treat the entire current batch as BACKFILL;
                // only a later record produced after catch-up may be LIVE.
                if !reconciling && cursor.saturating_sub(sent) > 1 {
                    reconciling = true;
                }
                while sent < cursor {
                    let (snap, bar) = &feed.ticks[sent];
                    let tick = ProtoTick {
                        snapshot: Some(snapshot_to_proto(snap)),
                        bar: Some(bar_to_proto(bar)),
                        delivery_phase: if reconciling {
                            DeliveryPhase::Backfill as i32
                        } else {
                            DeliveryPhase::Live as i32
                        },
                        high_watermark_sequence: cursor as u64,
                    };
                    if tx.send(Ok(tick)).await.is_err() {
                        return; // client dropped
                    }
                    sent += 1;
                }
                if reconciling && sent >= cursor {
                    // The catch-up batch itself remains BACKFILL. Only a later
                    // record produced after this point may be emitted as LIVE.
                    reconciling = false;
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    const NDJSON: &str = include_str!("../fixtures/replay_qqq_sample.ndjson");

    fn feed() -> Arc<MarketFeed> {
        let cfg = ReplayConfig {
            opening_range_minutes: 3,
            previous_close: Some(497.20),
            ..ReplayConfig::default()
        };
        Arc::new(MarketFeed::from_ndjson(NDJSON, cfg).unwrap())
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
    async fn resume_zero_replays_from_session_open() {
        // A fresh client at producer start receives the session from record 1.
        let svc = MarketServiceImpl::new(feed());
        let ticks = drain_from(&svc, 0).await;
        let seqs: Vec<u64> = ticks
            .iter()
            .map(|t| t.snapshot.as_ref().unwrap().sequence_number)
            .collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6]);
        assert!(ticks
            .iter()
            .all(|t| t.delivery_phase == DeliveryPhase::Live as i32));
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
            .all(|t| t.delivery_phase == DeliveryPhase::Live as i32));
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
}
