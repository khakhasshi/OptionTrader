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
    DataHealthState as ProtoHealthState, GetDataHealthRequest, MarketBar as ProtoBar,
    MarketSnapshot as ProtoSnapshot, MarketTick as ProtoTick, StreamRequest,
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
    /// Producer starts lazily on the first subscription, exactly once.
    started: Arc<AtomicBool>,
    interval: Duration,
}

impl MarketServiceImpl {
    pub fn new(feed: Arc<MarketFeed>) -> Self {
        let (cursor_tx, _rx) = watch::channel(0usize);
        MarketServiceImpl {
            feed,
            cursor_tx,
            started: Arc::new(AtomicBool::new(false)),
            interval: replay_tick_interval(),
        }
    }

    /// Start the single producer clock once. It advances the shared cursor from
    /// 0 to N at a fixed pace; this is the ONLY thing that "produces" records.
    fn ensure_producer(&self) {
        if self.started.swap(true, Ordering::SeqCst) {
            return;
        }
        let n = self.feed.ticks.len();
        let tx = self.cursor_tx.clone();
        let interval = self.interval;
        tokio::spawn(async move {
            for i in 1..=n {
                tokio::time::sleep(interval).await;
                if tx.send(i).is_err() {
                    break; // no receivers and none will come
                }
            }
        });
    }
}

#[tonic::async_trait]
impl MarketService for MarketServiceImpl {
    type StreamMarketSnapshotsStream = ReceiverStream<Result<ProtoTick, Status>>;

    async fn stream_market_snapshots(
        &self,
        _request: Request<StreamRequest>,
    ) -> Result<Response<Self::StreamMarketSnapshotsStream>, Status> {
        let feed = Arc::clone(&self.feed);
        let mut cursor_rx = self.cursor_tx.subscribe();
        // Join at the CURRENT cursor: a late subscriber / reconnect resumes from
        // here and never re-ingests already-produced history.
        let mut sent = *cursor_rx.borrow();
        let n = feed.ticks.len();
        self.ensure_producer();

        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            loop {
                let cursor = *cursor_rx.borrow();
                while sent < cursor {
                    let (snap, bar) = &feed.ticks[sent];
                    let tick = ProtoTick {
                        snapshot: Some(snapshot_to_proto(snap)),
                        bar: Some(bar_to_proto(bar)),
                    };
                    if tx.send(Ok(tick)).await.is_err() {
                        return; // client dropped
                    }
                    sent += 1;
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
        use tokio_stream::StreamExt;
        let resp = svc
            .stream_market_snapshots(Request::new(StreamRequest {
                session_id: "s".into(),
                speedup: 0.0,
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
    async fn first_subscriber_gets_all_ticks_in_order() {
        let svc = MarketServiceImpl::new(feed());
        let ticks = drain(&svc).await;
        let seqs: Vec<u64> = ticks
            .iter()
            .map(|t| t.snapshot.as_ref().unwrap().sequence_number)
            .collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6]);
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
        // The exact review scenario: after a full drain, a second subscription
        // must NOT flip health to DEGRADED / bump out_of_order.
        let svc = MarketServiceImpl::new(feed());
        let _ = drain(&svc).await;
        let after_first = svc
            .get_data_health(Request::new(GetDataHealthRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(after_first.status, ProtoHealth::Healthy as i32);
        assert_eq!(after_first.out_of_order_count, 0);

        let _ = drain(&svc).await; // second subscription
        let after_second = svc
            .get_data_health(Request::new(GetDataHealthRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            after_second.status,
            ProtoHealth::Healthy as i32,
            "a second subscriber must not degrade global health"
        );
        assert_eq!(after_second.out_of_order_count, 0);
    }

    #[tokio::test]
    async fn snapshot_data_health_matches_get_data_health() {
        let svc = MarketServiceImpl::new(feed());
        let ticks = drain(&svc).await;
        let last_snap_health = ticks.last().unwrap().snapshot.as_ref().unwrap().data_health;
        let gdh = svc
            .get_data_health(Request::new(GetDataHealthRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(last_snap_health, gdh.status);
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
