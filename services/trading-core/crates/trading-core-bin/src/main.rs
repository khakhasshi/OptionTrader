//! trading-core process entrypoint. Serves HTTP `/health` (contract:
//! health.json#/$defs/ServiceHealth) and `/market/snapshot` (contract:
//! market_snapshot.json), plus the gRPC MarketService (StreamMarketSnapshots,
//! GetDataHealth) backed by the deterministic replay snapshot source.
//!
//! HTTP (Phase 0) and gRPC (Phase 2) run concurrently in one process. In later
//! phases health derives from live ingestion + broker reconciliation instead of
//! the env-driven scenario, and the live ThetaData adapter replaces replay.

mod grpc;

use std::env;
use std::sync::Arc;

use axum::{routing::get, Json, Router};
use grpc::{MarketFeed, MarketServiceImpl};
use market_core::{DataHealth, MarketSnapshot, ReplayConfig};
use optiontrader_proto::market_v1::market_service_server::MarketServiceServer;
use risk_gateway::{new_position_allowed, BrokerHealth};
use serde_json::{json, Value};

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

async fn health() -> Json<Value> {
    let (data, broker, reconciled) = scenario();
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

async fn market_snapshot() -> Json<MarketSnapshot> {
    // Phase 0 compatibility: deterministic fixture. The live snapshot feed is
    // now served over gRPC (StreamMarketSnapshots).
    Json(MarketSnapshot::fixture())
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

    let feed = Arc::new(
        MarketFeed::from_ndjson(REPLAY_NDJSON, replay_config())
            .expect("build replay snapshot feed"),
    );

    let http_app = Router::new()
        .route("/health", get(health))
        .route("/market/snapshot", get(market_snapshot));
    let http_addr = format!("0.0.0.0:{http_port}");
    let http_listener = tokio::net::TcpListener::bind(&http_addr)
        .await
        .expect("bind trading-core health port");

    let grpc_addr = format!("0.0.0.0:{grpc_port}")
        .parse()
        .expect("parse grpc addr");
    let market_service = MarketServiceServer::new(MarketServiceImpl::new(feed));

    tracing::info!("trading-core HTTP on {http_addr}, gRPC on {grpc_addr}");

    let http = async {
        axum::serve(http_listener, http_app)
            .await
            .expect("trading-core HTTP server error");
    };
    let grpc = async {
        tonic::transport::Server::builder()
            .add_service(market_service)
            .serve(grpc_addr)
            .await
            .expect("trading-core gRPC server error");
    };

    // Run both servers; if either exits, the process exits.
    tokio::select! {
        _ = http => {},
        _ = grpc => {},
    }
}
