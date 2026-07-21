//! Async Theta Terminal WebSocket transport. One connection owns all stream
//! requests, matching ThetaData's single-connection requirement.

use std::time::Duration;

use chrono::{Timelike, Utc};
use chrono_tz::America::New_York;
use futures_util::{SinkExt, StreamExt};
use market_core::{
    parse_ohlc_backfill, parse_stream_message, subscribe_trade_request, ReplayBar,
    ThetaBarAggregator, ThetaStreamEvent,
};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Debug, Clone)]
pub struct ThetaLiveConfig {
    pub url: String,
    pub rest_url: String,
    pub symbol: String,
    pub backfill_enabled: bool,
    pub reconnect_base: Duration,
    pub reconnect_max: Duration,
}

impl Default for ThetaLiveConfig {
    fn default() -> Self {
        ThetaLiveConfig {
            url: "ws://127.0.0.1:25520/v1/events".into(),
            rest_url: "http://127.0.0.1:25503/v3".into(),
            symbol: "QQQ".into(),
            backfill_enabled: true,
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

async fn fetch_backfill(config: &ThetaLiveConfig) -> Result<Vec<ReplayBar>, String> {
    let now_et = Utc::now().with_timezone(&New_York);
    let date = now_et.date_naive();
    let current_minute = (now_et.hour() * 60 + now_et.minute()) as u16;
    if current_minute <= 570 {
        return Ok(Vec::new());
    }
    let complete_through = current_minute.saturating_sub(1).min(959);
    let end_time = format!(
        "{:02}:{:02}:00.000",
        complete_through / 60,
        complete_through % 60
    );
    let date_param = date.format("%Y%m%d").to_string();
    let response = reqwest::Client::new()
        .get(format!(
            "{}/stock/history/ohlc",
            config.rest_url.trim_end_matches('/')
        ))
        .query(&[
            ("symbol", config.symbol.as_str()),
            ("date", date_param.as_str()),
            ("interval", "1m"),
            ("start_time", "09:30:00.000"),
            ("end_time", end_time.as_str()),
            ("venue", "nqb"),
            ("format", "json"),
        ])
        .send()
        .await
        .map_err(|error| format!("backfill request: {error}"))?
        .error_for_status()
        .map_err(|error| format!("backfill status: {error}"))?;
    let raw = response
        .text()
        .await
        .map_err(|error| format!("backfill body: {error}"))?;
    let bars =
        parse_ohlc_backfill(&raw, date).map_err(|error| format!("backfill protocol: {error}"))?;
    if bars.first().map(|bar| bar.minute_et) != Some(570)
        || bars.last().map(|bar| bar.minute_et) != Some(complete_through)
    {
        return Err(format!(
            "backfill incomplete: expected 570..={complete_through}, got {:?}..={:?}",
            bars.first().map(|bar| bar.minute_et),
            bars.last().map(|bar| bar.minute_et)
        ));
    }
    Ok(bars)
}

async fn connect_once(
    config: &ThetaLiveConfig,
    request_id: u64,
    aggregator: &mut ThetaBarAggregator,
    tx: &mpsc::Sender<ThetaLiveEvent>,
) -> Result<(), String> {
    let (mut socket, _) = connect_async(&config.url)
        .await
        .map_err(|error| format!("connect: {error}"))?;
    let request = subscribe_trade_request(&config.symbol, request_id).to_string();
    socket
        .send(Message::Text(request.into()))
        .await
        .map_err(|error| format!("subscribe: {error}"))?;

    // Subscribe first so current-minute trades buffer in the socket while the
    // REST call recovers every completed minute from the session open.
    if config.backfill_enabled {
        let bars = fetch_backfill(config).await?;
        if tx.send(ThetaLiveEvent::Backfill(bars)).await.is_err() {
            return Ok(());
        }
    }

    while let Some(message) = socket.next().await {
        let message = message.map_err(|error| format!("receive: {error}"))?;
        match message {
            Message::Text(text) => match parse_stream_message(&text, &config.symbol)
                .map_err(|error| format!("protocol: {error}"))?
            {
                ThetaStreamEvent::Connected => {
                    if tx.send(ThetaLiveEvent::Connected).await.is_err() {
                        return Ok(());
                    }
                }
                ThetaStreamEvent::Disconnected => return Err("terminal disconnected".into()),
                ThetaStreamEvent::Trade(trade) => {
                    if let Some(bar) = aggregator
                        .push(trade)
                        .map_err(|error| format!("aggregate: {error}"))?
                    {
                        if tx.send(ThetaLiveEvent::Bar(bar)).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                ThetaStreamEvent::Ignored => {}
            },
            Message::Close(_) => return Err("terminal closed websocket".into()),
            Message::Ping(payload) => socket
                .send(Message::Pong(payload))
                .await
                .map_err(|error| format!("pong: {error}"))?,
            _ => {}
        }
    }
    Err("terminal websocket ended".into())
}

pub async fn run(config: ThetaLiveConfig, tx: mpsc::Sender<ThetaLiveEvent>) {
    let mut request_id = 1_u64;
    let mut backoff = config.reconnect_base;
    loop {
        // Sequence values and a partial minute cannot be trusted across a new
        // socket. REST backfill becomes authoritative for completed minutes.
        let mut aggregator = ThetaBarAggregator::default();
        match connect_once(&config, request_id, &mut aggregator, &tx).await {
            Ok(()) => return,
            Err(reason) => {
                if tx.send(ThetaLiveEvent::Disconnected(reason)).await.is_err() {
                    return;
                }
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(config.reconnect_max);
        request_id = request_id.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    fn trade(ms: u32, sequence: u64, price: f64) -> String {
        json!({
            "header": {"type": "TRADE", "status": "CONNECTED"},
            "contract": {"security_type": "STOCK", "root": "QQQ"},
            "trade": {
                "ms_of_day": ms, "sequence": sequence, "size": 1,
                "condition": 0, "price": price, "exchange": 57, "date": 20260720
            }
        })
        .to_string()
    }

    #[tokio::test]
    async fn websocket_transport_subscribes_and_emits_finalized_bar() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = accept_async(stream).await.unwrap();
            let request = websocket.next().await.unwrap().unwrap();
            let request: serde_json::Value =
                serde_json::from_str(request.to_text().unwrap()).unwrap();
            assert_eq!(request["contract"]["root"], "QQQ");
            websocket
                .send(Message::Text(trade(34_200_000, 1, 500.0).into()))
                .await
                .unwrap();
            websocket
                .send(Message::Text(trade(34_260_000, 2, 501.0).into()))
                .await
                .unwrap();
            websocket.close(None).await.unwrap();
        });

        let (tx, mut rx) = mpsc::channel(8);
        let mut aggregator = ThetaBarAggregator::default();
        let config = ThetaLiveConfig {
            url: format!("ws://{addr}"),
            backfill_enabled: false,
            reconnect_base: Duration::from_millis(1),
            reconnect_max: Duration::from_millis(1),
            ..ThetaLiveConfig::default()
        };
        let result = connect_once(&config, 7, &mut aggregator, &tx).await;
        assert!(result.is_err());
        let ThetaLiveEvent::Bar(bar) = rx.recv().await.unwrap() else {
            panic!("expected bar")
        };
        assert_eq!(bar.minute_et, 570);
        assert_eq!(bar.close, 500.0);
        server.await.unwrap();
    }
}
