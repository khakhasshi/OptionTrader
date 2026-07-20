//! trading-core process entrypoint. Phase 0: exposes an HTTP health endpoint.
//! In later phases this process also serves the gRPC API (StreamMarketSnapshots,
//! EvaluateRisk, SubmitApprovedPlan, ...) with market/risk/execution/broker
//! crates isolated internally.

use std::env;

use axum::{routing::get, Json, Router};
use market_core::DataHealth;
use risk_gateway::BrokerHealth;
use serde_json::{json, Value};

async fn health() -> Json<Value> {
    // Phase 0: static skeleton values. Real health derives from live ingestion
    // and broker reconciliation in Phase 1/3.
    Json(json!({
        "status": "ok",
        "service": "trading-core",
        "environment": env::var("OPTIONTRADER_ENV").unwrap_or_else(|_| "local".into()),
        "data_health": DataHealth::Disconnected,
        "broker_health": BrokerHealth::Disconnected,
    }))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let port: u16 = env::var("TRADING_CORE_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);

    let app = Router::new().route("/health", get(health));
    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("bind trading-core health port");

    tracing::info!("trading-core health server on {addr}");
    axum::serve(listener, app)
        .await
        .expect("trading-core server error");
}
