//! gRPC MarketService: streams the Rust-authoritative snapshot feed to Python.
//!
//! The server is backed by a `SnapshotSource` (replay by default; the live
//! ThetaData adapter slots in behind the same trait once entitled). Snapshots
//! are precomputed deterministically, then streamed in order; `GetDataHealth`
//! reports the health of the most recently emitted snapshot.

use std::sync::Arc;

use market_core::{
    DataHealth, MarketSnapshot, ReplayBar, ReplayConfig, ReplaySnapshotSource, SnapshotSource,
};
use optiontrader_proto::market_v1::{
    market_service_server::MarketService, DataHealth as ProtoHealth,
    DataHealthState as ProtoHealthState, GetDataHealthRequest, MarketBar as ProtoBar,
    MarketSnapshot as ProtoSnapshot, MarketTick as ProtoTick, StreamRequest,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

/// Precomputed tick feed shared by all RPCs. Each tick pairs the aggregate
/// snapshot with its originating per-minute bar. Immutable after construction,
/// so streaming and health queries observe a consistent deterministic sequence.
pub struct MarketFeed {
    ticks: Vec<(MarketSnapshot, ReplayBar)>,
}

impl MarketFeed {
    /// Build a replay-backed feed from an NDJSON fixture.
    pub fn from_ndjson(ndjson: &str, cfg: ReplayConfig) -> Result<Self, String> {
        let source =
            ReplaySnapshotSource::from_ndjson(ndjson, cfg).map_err(|e| format!("replay: {e}"))?;
        let snapshots = source.snapshots().map_err(|e| format!("snapshots: {e}"))?;
        if snapshots.is_empty() {
            return Err("replay produced no snapshots".into());
        }
        // snapshots() emits one snapshot per bar, in the same order as bars().
        let ticks: Vec<(MarketSnapshot, ReplayBar)> = snapshots
            .into_iter()
            .zip(source.bars().iter().cloned())
            .collect();
        Ok(MarketFeed { ticks })
    }

    fn latest(&self) -> &MarketSnapshot {
        &self.ticks.last().expect("feed is non-empty").0
    }
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

pub struct MarketServiceImpl {
    feed: Arc<MarketFeed>,
}

impl MarketServiceImpl {
    pub fn new(feed: Arc<MarketFeed>) -> Self {
        MarketServiceImpl { feed }
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
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            for (snap, bar) in &feed.ticks {
                let tick = ProtoTick {
                    snapshot: Some(snapshot_to_proto(snap)),
                    bar: Some(bar_to_proto(bar)),
                };
                if tx.send(Ok(tick)).await.is_err() {
                    break; // client dropped
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_data_health(
        &self,
        _request: Request<GetDataHealthRequest>,
    ) -> Result<Response<ProtoHealthState>, Status> {
        let latest = self.feed.latest();
        Ok(Response::new(ProtoHealthState {
            schema_version: "1.0".into(),
            occurred_at_utc: latest.occurred_at_utc.clone(),
            status: health_to_proto(latest.data_health) as i32,
            market_event_lag_ms: 0,
            quote_age_ms: latest.quote_age_ms,
            out_of_order_count: 0,
            reconnect_count: 0,
            reason: String::new(),
        }))
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

    #[tokio::test]
    async fn stream_emits_all_snapshots_in_order() {
        let svc = MarketServiceImpl::new(feed());
        let resp = svc
            .stream_market_snapshots(Request::new(StreamRequest {
                session_id: "s".into(),
                speedup: 0.0,
            }))
            .await
            .unwrap();
        let mut stream = resp.into_inner();
        use tokio_stream::StreamExt;
        let mut seqs = Vec::new();
        while let Some(item) = stream.next().await {
            let tick = item.unwrap();
            let snap = tick.snapshot.expect("tick carries snapshot");
            let bar = tick.bar.expect("tick carries bar");
            // Bar and snapshot line up on the same business instant.
            assert_eq!(bar.occurred_at_utc, snap.occurred_at_utc);
            seqs.push(snap.sequence_number);
        }
        assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn get_data_health_reports_latest() {
        let svc = MarketServiceImpl::new(feed());
        let resp = svc
            .get_data_health(Request::new(GetDataHealthRequest {}))
            .await
            .unwrap();
        let state = resp.into_inner();
        assert_eq!(state.schema_version, "1.0");
        assert_eq!(state.status, ProtoHealth::Healthy as i32);
    }

    #[test]
    fn feed_maps_snapshot_and_bar_fields_to_proto() {
        let f = feed();
        let (snap, bar) = &f.ticks[3];
        let proto = snapshot_to_proto(snap);
        assert_eq!(proto.symbol, "QQQ.US");
        assert_eq!(proto.schema_version, "1.0");
        assert_eq!(proto.data_health, ProtoHealth::Healthy as i32);
        assert!(!proto.opening_range_high.is_empty()); // OR ready by index 3
        let proto_bar = bar_to_proto(bar);
        assert_eq!(proto_bar.occurred_at_utc, snap.occurred_at_utc);
        assert!(proto_bar.close.contains('.'));
        assert!(proto_bar.minute_et >= 570);
    }
}
