//! trading-core process entrypoint. Serves HTTP `/health` (contract:
//! health.json#/$defs/ServiceHealth) and `/market/snapshot` (contract:
//! market_snapshot.json), plus the gRPC MarketService (StreamMarketSnapshots,
//! GetDataHealth) backed by deterministic replay or ThetaData SDK live data.
//!
//! HTTP and gRPC run concurrently in one process. Market DataHealth derives
//! from the selected source; broker reconciliation remains a Phase 3 concern.

mod broker_registry;
mod grpc;
mod option_registry;
mod risk_grpc;
mod theta_live;

use std::env;
use std::sync::{Arc, RwLock};

use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
use grpc::{LiveMarketServiceImpl, MarketFeed, MarketServiceImpl};
use market_core::{DataHealth, MarketSnapshot, ReplayConfig};
use optiontrader_proto::execution_v1::risk_execution_service_server::RiskExecutionServiceServer;
use optiontrader_proto::market_v1::market_service_server::MarketServiceServer;
use risk_gateway::BrokerHealth;
use risk_grpc::{BrokerAuthority, MarketAuthority, RiskExecutionServiceImpl};
use serde_json::{json, Value};
use theta_live::ThetaLiveConfig;

#[derive(Clone)]
struct AppState {
    live: Option<LiveMarketServiceImpl>,
    broker: Arc<RwLock<BrokerAuthority>>,
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

async fn health(State(state): State<AppState>) -> Result<Json<Value>, StatusCode> {
    let data = if let Some(live) = state.live {
        live.current_health().await
    } else {
        scenario().0
    };
    let broker = state
        .broker
        .read()
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        .clone();
    let allowed = broker.allows_new_position(data.allows_new_position(), chrono::Utc::now());
    Ok(Json(json!({
        "schema_version": "1.0",
        "status": "ok",
        "service": "trading-core",
        "environment": env::var("OPTIONTRADER_ENV").unwrap_or_else(|_| "local".into()),
        "data_health": data,
        "broker_health": broker.health,
        "reconciled": broker.reconciled,
        "new_position_allowed": allowed,
    })))
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
    let http_bind = env::var("TRADING_CORE_HTTP_BIND").unwrap_or_else(|_| "127.0.0.1".into());
    let grpc_bind = env::var("TRADING_CORE_GRPC_BIND").unwrap_or_else(|_| "127.0.0.1".into());

    let market_source = env::var("OPTIONTRADER_MARKET_SOURCE").unwrap_or_else(|_| "replay".into());
    if market_source != "replay" && market_source != "theta-sdk" {
        panic!("OPTIONTRADER_MARKET_SOURCE must be replay or theta-sdk");
    }
    let live_service = (market_source == "theta-sdk").then(|| {
        LiveMarketServiceImpl::new(
            ThetaLiveConfig {
                endpoint: env::var("THETADATA_SDK_GRPC")
                    .unwrap_or_else(|_| "http://127.0.0.1:50052".into()),
                ..ThetaLiveConfig::default()
            },
            replay_config(),
        )
    });
    let replay_service = (market_source == "replay").then(|| {
        let feed = Arc::new(
            MarketFeed::from_ndjson(REPLAY_NDJSON, replay_config())
                .expect("build replay snapshot feed"),
        );
        MarketServiceImpl::new(feed)
    });
    let market_authority = if let Some(live) = live_service.clone() {
        MarketAuthority::Live(live)
    } else {
        MarketAuthority::Replay(
            replay_service
                .clone()
                .expect("replay service exists for replay source"),
        )
    };
    let (_, broker_health, broker_reconciled) = scenario();
    let broker_authority = BrokerAuthority::from_env(broker_health, broker_reconciled)
        .expect("valid OPTIONTRADER risk authority configuration");
    let risk_service = RiskExecutionServiceImpl::new(market_authority, broker_authority)
        .expect("valid Phase 3 broker execution configuration");
    let broker_handle = risk_service.broker_handle();

    let http_app = Router::new()
        .route("/health", get(health))
        .route("/market/snapshot", get(market_snapshot))
        .with_state(AppState {
            live: live_service.clone(),
            broker: broker_handle,
        });
    let http_addr = format!("{http_bind}:{http_port}");
    let http_listener = tokio::net::TcpListener::bind(&http_addr)
        .await
        .expect("bind trading-core health port");

    let grpc_addr = format!("{grpc_bind}:{grpc_port}")
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
                .add_service(RiskExecutionServiceServer::new(risk_service))
                .serve(grpc_addr)
                .await
                .expect("trading-core live gRPC server error");
        } else {
            tonic::transport::Server::builder()
                .add_service(MarketServiceServer::new(
                    replay_service.expect("replay service exists for replay source"),
                ))
                .add_service(RiskExecutionServiceServer::new(risk_service))
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
