//! trading-core process entrypoint. Phase 0: exposes HTTP `/health` (contract:
//! health.json#/$defs/ServiceHealth) and `/market/snapshot` (contract:
//! market_snapshot.json) serving a deterministic fixture.
//!
//! In later phases this process also serves the gRPC API (StreamMarketSnapshots,
//! EvaluateRisk, SubmitApprovedPlan, ...) with market/risk/execution/broker
//! crates isolated internally, and health derives from live ingestion +
//! broker reconciliation instead of the Phase 0 env-driven scenario.

use std::env;

use axum::{routing::get, Json, Router};
use market_core::{DataHealth, MarketSnapshot};
use risk_gateway::{new_position_allowed, BrokerHealth};
use serde_json::{json, Value};

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
    // Phase 0: deterministic fixture. Phase 1 replaces with live ThetaData.
    Json(MarketSnapshot::fixture())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let port: u16 = env::var("TRADING_CORE_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);

    let app = Router::new()
        .route("/health", get(health))
        .route("/market/snapshot", get(market_snapshot));
    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("bind trading-core health port");

    tracing::info!("trading-core server on {addr}");
    axum::serve(listener, app)
        .await
        .expect("trading-core server error");
}
