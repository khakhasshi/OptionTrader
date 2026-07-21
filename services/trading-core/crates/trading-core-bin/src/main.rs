//! trading-core process entrypoint. Serves HTTP `/health` (contract:
//! health.json#/$defs/ServiceHealth) and `/market/snapshot` (contract:
//! market_snapshot.json), plus the gRPC MarketService (StreamMarketSnapshots,
//! GetDataHealth) backed by deterministic replay or Theta Terminal live data.
//!
//! HTTP and gRPC run concurrently in one process. Market DataHealth derives
//! from the selected source; broker reconciliation remains a Phase 3 concern.

mod grpc;
mod theta_live;

use std::env;
use std::sync::Arc;

use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
use grpc::{LiveMarketServiceImpl, MarketFeed, MarketServiceImpl};
use market_core::{DataHealth, MarketSnapshot, ReplayConfig};
use optiontrader_proto::market_v1::market_service_server::MarketServiceServer;
use risk_gateway::{new_position_allowed, BrokerHealth};
use serde_json::{json, Value};
use theta_live::ThetaLiveConfig;

#[derive(Clone)]
struct AppState {
    live: Option<LiveMarketServiceImpl>,
}

/// Default replay dataset bundled with the binary for local/replay runs.
const REPLAY_NDJSON: &str = include_str!("../fixtures/replay_qqq_sample.ndjson");

/// Phase 0 runtime posture. Real values come from ingestion + reconciliation in
/// Phase 1/3. `OPTIONTRADER_SCENARIO=healthy` lets the e2e smoke exercise the
/// tradable path deterministically; the default is the safe fail-closed state.
fn scenario() -> (DataHealth, BrokerHealth, bool) {
    match env::var("OPTIONTRADER_SCENARIO").as_deref() {
        Ok("healthy") => (DataHealth::Healthy, BrokerHealth::Healthy, true),
        _ => (DataHealth::Disconnected, BrokerHealth::Disconnected, false),
    }
}

async fn health(State(state): State<AppState>) -> Json<Value> {
    let (data, broker, reconciled) = if let Some(live) = state.live {
        (
            live.current_health().await,
            BrokerHealth::Disconnected,
            false,
        )
    } else {
        scenario()
    };
    let allowed = new_position_allowed(data.allows_new_position(), broker, reconciled);
    Json(json!({
        "schema_version": "1.0",
        "status": "ok",
        "service": "trading-core",
        "environment": env::var("OPTIONTRADER_ENV").unwrap_or_else(|_| "local".into()),
        "data_health": data,
        "broker_health": broker,
        "reconciled": reconciled,
        "new_position_allowed": allowed,
    }))
}

async fn market_snapshot(
    State(state): State<AppState>,
) -> Result<Json<MarketSnapshot>, StatusCode> {
    if let Some(live) = state.live {
        return live
            .latest_snapshot()
            .await
            .map(Json)
            .ok_or(StatusCode::SERVICE_UNAVAILABLE);
    }
    Ok(Json(MarketSnapshot::fixture()))
}

fn replay_config() -> ReplayConfig {
    ReplayConfig {
        opening_range_minutes: 3,
        previous_close: Some(497.20),
        ..ReplayConfig::default()
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let http_port: u16 = env::var("TRADING_CORE_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let grpc_port: u16 = env::var("TRADING_CORE_GRPC_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(50051);

    let market_source = env::var("OPTIONTRADER_MARKET_SOURCE").unwrap_or_else(|_| "replay".into());
    if market_source != "replay" && market_source != "theta" {
        panic!("OPTIONTRADER_MARKET_SOURCE must be replay or theta");
    }
    let live_service = (market_source == "theta").then(|| {
        LiveMarketServiceImpl::new(
            ThetaLiveConfig {
                url: env::var("THETADATA_WS_URL")
                    .unwrap_or_else(|_| "ws://127.0.0.1:25520/v1/events".into()),
                rest_url: env::var("THETADATA_BASE_URL")
                    .unwrap_or_else(|_| "http://127.0.0.1:25503/v3".into()),
                ..ThetaLiveConfig::default()
            },
            replay_config(),
        )
    });

    let http_app = Router::new()
        .route("/health", get(health))
        .route("/market/snapshot", get(market_snapshot))
        .with_state(AppState {
            live: live_service.clone(),
        });
    let http_addr = format!("0.0.0.0:{http_port}");
    let http_listener = tokio::net::TcpListener::bind(&http_addr)
        .await
        .expect("bind trading-core health port");

    let grpc_addr = format!("0.0.0.0:{grpc_port}")
        .parse()
        .expect("parse grpc addr");
    tracing::info!("trading-core source={market_source}, HTTP on {http_addr}, gRPC on {grpc_addr}");

    let http = async {
        axum::serve(http_listener, http_app)
            .await
            .expect("trading-core HTTP server error");
    };
    let grpc = async {
        if let Some(live) = live_service {
            tonic::transport::Server::builder()
                .add_service(MarketServiceServer::new(live))
                .serve(grpc_addr)
                .await
                .expect("trading-core live gRPC server error");
        } else {
            let feed = Arc::new(
                MarketFeed::from_ndjson(REPLAY_NDJSON, replay_config())
                    .expect("build replay snapshot feed"),
            );
            tonic::transport::Server::builder()
                .add_service(MarketServiceServer::new(MarketServiceImpl::new(feed)))
                .serve(grpc_addr)
                .await
                .expect("trading-core replay gRPC server error");
        }
    };

    // Run both servers; if either exits, the process exits.
    tokio::select! {
        _ = http => {},
        _ = grpc => {},
    }
}
