//! ThetaData Python SDK bridge client. Credentials and provider DataFrames stay
//! in Python; this transport accepts only the internal protobuf bar contract.

use std::time::Duration;

use chrono::{DateTime, NaiveDate, Offset, Timelike, Utc};
use chrono_tz::America::New_York;
use market_core::ReplayBar;
use optiontrader_proto::market_v1::{
    theta_data_sdk_service_client::ThetaDataSdkServiceClient, MarketBar, ThetaSdkBarBatch,
    ThetaSdkStreamRequest,
};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct ThetaLiveConfig {
    pub endpoint: String,
    pub symbol: String,
    pub venue: String,
    pub poll_interval_ms: u32,
    pub max_batch_age: Duration,
    pub reconnect_base: Duration,
    pub reconnect_max: Duration,
}

impl Default for ThetaLiveConfig {
    fn default() -> Self {
        ThetaLiveConfig {
            endpoint: "http://127.0.0.1:50052".into(),
            symbol: "QQQ".into(),
            venue: "nqb".into(),
            poll_interval_ms: 2_000,
            max_batch_age: Duration::from_secs(30),
            reconnect_base: Duration::from_millis(250),
            reconnect_max: Duration::from_secs(10),
        }
    }
}

#[derive(Debug)]
pub enum ThetaLiveEvent {
    Connected,
    Disconnected(String),
    Backfill(Vec<ReplayBar>),
    Bar(ReplayBar),
}

fn parse_decimal(value: &str, field: &'static str, optional: bool) -> Result<Option<f64>, String> {
    if optional && value.is_empty() {
        return Ok(None);
    }
    let parsed = value
        .parse::<f64>()
        .map_err(|_| format!("invalid SDK {field}"))?;
    if !parsed.is_finite() || parsed <= 0.0 {
        return Err(format!("invalid SDK {field}"));
    }
    Ok(Some(parsed))
}

fn sdk_bar_to_replay(bar: MarketBar) -> Result<ReplayBar, String> {
    if !bar.occurred_at_utc.ends_with('Z') {
        return Err("SDK occurred_at_utc must use UTC Z".into());
    }
    let utc = DateTime::parse_from_rfc3339(&bar.occurred_at_utc)
        .map_err(|_| "invalid SDK occurred_at_utc")?;
    let et =
        DateTime::parse_from_rfc3339(&bar.timestamp_et).map_err(|_| "invalid SDK timestamp_et")?;
    if utc.timestamp_millis() != et.timestamp_millis() {
        return Err("SDK UTC/ET timestamps disagree".into());
    }
    let expected_et = utc.with_timezone(&New_York);
    if et.naive_local() != expected_et.naive_local()
        || et.offset().local_minus_utc() != expected_et.offset().fix().local_minus_utc()
    {
        return Err("SDK timestamp_et is not America/New_York".into());
    }
    if !(570..960).contains(&bar.minute_et) {
        return Err("SDK minute is outside regular trading hours".into());
    }
    let timestamp_minute = expected_et.hour() * 60 + expected_et.minute();
    if timestamp_minute != bar.minute_et {
        return Err("SDK minute does not match timestamp_et".into());
    }
    let open = parse_decimal(&bar.open, "open", false)?.expect("required decimal");
    let high = parse_decimal(&bar.high, "high", false)?.expect("required decimal");
    let low = parse_decimal(&bar.low, "low", false)?.expect("required decimal");
    let close = parse_decimal(&bar.close, "close", false)?.expect("required decimal");
    let vwap = parse_decimal(&bar.vwap, "vwap", true)?;
    if !(low <= open && open <= high && low <= close && close <= high) {
        return Err("SDK OHLC price is outside the bar range".into());
    }
    Ok(ReplayBar {
        occurred_at_utc: bar.occurred_at_utc,
        timestamp_et: bar.timestamp_et,
        occurred_at_utc_ms: utc.timestamp_millis(),
        minute_et: bar.minute_et as u16,
        open,
        high,
        low,
        close,
        volume: bar.volume,
        vwap,
    })
}

fn validate_batch(
    batch: ThetaSdkBarBatch,
    first_batch: bool,
    last_minute: Option<u16>,
    received_at: DateTime<Utc>,
    max_batch_age: Duration,
) -> Result<Vec<ReplayBar>, String> {
    if !batch.fetched_at_utc.ends_with('Z') {
        return Err("invalid SDK batch fetched_at_utc".into());
    }
    let fetched_at = DateTime::parse_from_rfc3339(&batch.fetched_at_utc)
        .map_err(|_| "invalid SDK batch fetched_at_utc")?;
    let age_ms = received_at.timestamp_millis() - fetched_at.timestamp_millis();
    if age_ms < -5_000 {
        return Err("SDK batch fetched_at_utc is in the future".into());
    }
    if age_ms.max(0) as u128 > max_batch_age.as_millis() {
        return Err("SDK batch is stale".into());
    }
    let session_date = NaiveDate::parse_from_str(&batch.session_date, "%Y-%m-%d")
        .map_err(|_| "invalid SDK batch session_date")?;
    if fetched_at.with_timezone(&New_York).date_naive() != session_date {
        return Err("SDK batch session_date does not match fetch time".into());
    }
    let complete_through = batch.complete_through_minute_et;
    if complete_through != 0 && !(570..960).contains(&complete_through) {
        return Err("invalid SDK complete-through watermark".into());
    }
    if batch.backfill != first_batch {
        return Err("SDK bridge backfill phase violation".into());
    }
    let bars: Vec<ReplayBar> = batch
        .bars
        .into_iter()
        .map(sdk_bar_to_replay)
        .collect::<Result<_, _>>()?;
    if bars.iter().any(|bar| {
        DateTime::parse_from_rfc3339(&bar.timestamp_et)
            .map_or(true, |timestamp| timestamp.date_naive() != session_date)
    }) {
        return Err("SDK bar date does not match batch session".into());
    }
    if bars
        .windows(2)
        .any(|pair| pair[1].minute_et != pair[0].minute_et + 1)
    {
        return Err("SDK batch contains a minute gap or disorder".into());
    }
    if let Some(first) = bars.first() {
        let expected = last_minute.map_or(570, |minute| minute + 1);
        if first.minute_et != expected {
            return Err(format!(
                "SDK stream minute gap: expected {expected}, got {}",
                first.minute_et
            ));
        }
    }
    match (complete_through, bars.last()) {
        (0, None) => {}
        (0, Some(_)) => return Err("SDK emitted bars before the first complete RTH minute".into()),
        (_, Some(last)) if u32::from(last.minute_et) == complete_through => {}
        (_, Some(last)) => {
            return Err(format!(
                "SDK backfill incomplete: expected through {complete_through}, got {}",
                last.minute_et
            ))
        }
        (_, None) => return Err("SDK backfill is empty after the first complete RTH minute".into()),
    }
    Ok(bars)
}

async fn connect_once(
    config: &ThetaLiveConfig,
    tx: &mpsc::Sender<ThetaLiveEvent>,
) -> Result<(), String> {
    let mut client = ThetaDataSdkServiceClient::connect(config.endpoint.clone())
        .await
        .map_err(|error| format!("SDK bridge connect: {error}"))?;
    let response = client
        .stream_completed_bars(ThetaSdkStreamRequest {
            symbol: config.symbol.clone(),
            venue: config.venue.clone(),
            poll_interval_ms: config.poll_interval_ms,
        })
        .await
        .map_err(|error| format!("SDK bridge subscribe: {error}"))?;
    let mut stream = response.into_inner();
    let mut first_batch = true;
    let mut last_minute = None;
    while let Some(batch) = stream
        .message()
        .await
        .map_err(|error| format!("SDK bridge receive: {error}"))?
    {
        let is_backfill = batch.backfill;
        let bars = validate_batch(
            batch,
            first_batch,
            last_minute,
            Utc::now(),
            config.max_batch_age,
        )?;
        let batch_last_minute = bars.last().map(|bar| bar.minute_et);
        if first_batch && tx.send(ThetaLiveEvent::Connected).await.is_err() {
            return Ok(());
        }
        if is_backfill {
            if tx.send(ThetaLiveEvent::Backfill(bars)).await.is_err() {
                return Ok(());
            }
        } else {
            for bar in bars {
                last_minute = Some(bar.minute_et);
                if tx.send(ThetaLiveEvent::Bar(bar)).await.is_err() {
                    return Ok(());
                }
            }
        }
        last_minute = batch_last_minute.or(last_minute);
        first_batch = false;
    }
    Err("SDK bridge stream ended".into())
}

pub async fn run(config: ThetaLiveConfig, tx: mpsc::Sender<ThetaLiveEvent>) {
    let mut backoff = config.reconnect_base;
    loop {
        match connect_once(&config, &tx).await {
            Ok(()) => return,
            Err(reason) => {
                if tx.send(ThetaLiveEvent::Disconnected(reason)).await.is_err() {
                    return;
                }
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(config.reconnect_max);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use optiontrader_proto::market_v1::{
        theta_data_sdk_service_server::{ThetaDataSdkService, ThetaDataSdkServiceServer},
        ThetaSdkStreamRequest,
    };
    use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
    use tonic::{Request, Response, Status};

    fn proto_bar(minute: u32) -> MarketBar {
        let hour = minute / 60;
        let minute_of_hour = minute % 60;
        MarketBar {
            occurred_at_utc: format!("2026-07-20T{}:{minute_of_hour:02}:00Z", hour + 4),
            timestamp_et: format!("2026-07-20T{hour:02}:{minute_of_hour:02}:00-04:00"),
            minute_et: minute,
            open: "500".into(),
            high: "501".into(),
            low: "499".into(),
            close: "500.5".into(),
            volume: 100,
            vwap: "500.25".into(),
        }
    }

    fn batch(backfill: bool, bars: Vec<MarketBar>) -> ThetaSdkBarBatch {
        let complete_through_minute_et = bars.last().map_or(0, |bar| bar.minute_et);
        ThetaSdkBarBatch {
            bars,
            backfill,
            fetched_at_utc: "2026-07-20T13:32:00Z".into(),
            complete_through_minute_et,
            session_date: "2026-07-20".into(),
        }
    }

    fn received_at() -> DateTime<Utc> {
        "2026-07-20T13:32:05Z".parse().unwrap()
    }

    #[test]
    fn validates_backfill_and_incremental_sequence() {
        let initial = validate_batch(
            batch(true, vec![proto_bar(570), proto_bar(571)]),
            true,
            None,
            received_at(),
            Duration::from_secs(30),
        )
        .unwrap();
        assert_eq!(initial.len(), 2);
        assert!(validate_batch(
            batch(false, vec![proto_bar(573)]),
            false,
            Some(571),
            received_at(),
            Duration::from_secs(30),
        )
        .is_err());
        assert!(validate_batch(
            batch(true, vec![]),
            false,
            Some(571),
            received_at(),
            Duration::from_secs(30),
        )
        .is_err());
        let mut partial = batch(true, vec![proto_bar(570), proto_bar(571)]);
        partial.complete_through_minute_et = 572;
        assert!(
            validate_batch(partial, true, None, received_at(), Duration::from_secs(30),).is_err()
        );
    }

    #[test]
    fn rejects_stale_or_future_sdk_batches() {
        let current = batch(true, vec![proto_bar(570), proto_bar(571)]);
        assert!(validate_batch(
            current.clone(),
            true,
            None,
            "2026-07-20T13:33:00Z".parse().unwrap(),
            Duration::from_secs(30),
        )
        .is_err());
        assert!(validate_batch(
            current,
            true,
            None,
            "2026-07-20T13:31:50Z".parse().unwrap(),
            Duration::from_secs(30),
        )
        .is_err());
    }

    #[test]
    fn rejects_timestamp_disagreement_and_invalid_ohlc() {
        let mut timestamp = proto_bar(570);
        timestamp.occurred_at_utc = "2026-07-20T13:31:00Z".into();
        assert!(sdk_bar_to_replay(timestamp).is_err());

        let mut range = proto_bar(570);
        range.close = "502".into();
        assert!(sdk_bar_to_replay(range).is_err());

        let mut minute = proto_bar(570);
        minute.minute_et = 571;
        assert!(sdk_bar_to_replay(minute).is_err());
    }

    #[derive(Default)]
    struct FakeSdkService;

    #[tonic::async_trait]
    impl ThetaDataSdkService for FakeSdkService {
        type StreamCompletedBarsStream = ReceiverStream<Result<ThetaSdkBarBatch, Status>>;

        async fn stream_completed_bars(
            &self,
            request: Request<ThetaSdkStreamRequest>,
        ) -> Result<Response<Self::StreamCompletedBarsStream>, Status> {
            assert_eq!(request.into_inner().symbol, "QQQ");
            let (tx, rx) = mpsc::channel(4);
            tokio::spawn(async move {
                tx.send(Ok(batch(true, vec![proto_bar(570), proto_bar(571)])))
                    .await
                    .unwrap();
                tx.send(Ok(batch(false, vec![proto_bar(572)])))
                    .await
                    .unwrap();
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        }

        async fn get_option_snapshots(
            &self,
            _request: Request<optiontrader_proto::market_v1::ThetaOptionSnapshotRequest>,
        ) -> Result<Response<optiontrader_proto::market_v1::ThetaOptionSnapshotBatch>, Status>
        {
            Err(Status::unimplemented(
                "option snapshots are not used by this fixture",
            ))
        }
    }

    #[tokio::test]
    async fn grpc_bridge_emits_reconciled_backfill_then_live_bar() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(ThetaDataSdkServiceServer::new(FakeSdkService))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let (tx, mut rx) = mpsc::channel(8);
        let config = ThetaLiveConfig {
            endpoint: format!("http://{addr}"),
            max_batch_age: Duration::from_secs(2 * 24 * 60 * 60),
            ..ThetaLiveConfig::default()
        };

        let result = connect_once(&config, &tx).await;

        assert_eq!(result.unwrap_err(), "SDK bridge stream ended");
        assert!(matches!(rx.recv().await, Some(ThetaLiveEvent::Connected)));
        let Some(ThetaLiveEvent::Backfill(backfill)) = rx.recv().await else {
            panic!("expected SDK backfill")
        };
        assert_eq!(backfill.len(), 2);
        let Some(ThetaLiveEvent::Bar(live)) = rx.recv().await else {
            panic!("expected incremental SDK bar")
        };
        assert_eq!(live.minute_et, 572);
        server.abort();
    }
}
