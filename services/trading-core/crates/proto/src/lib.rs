//! Generated gRPC types for the Python<->Rust market data boundary.
//!
//! The `market_v1` module is produced at build time from
//! `packages/contracts/proto/market.proto` by `build.rs` (tonic-build). Nothing
//! here is hand-written; re-run `cargo build` after editing the proto.

pub mod market_v1 {
    tonic::include_proto!("optiontrader.market.v1");
}
