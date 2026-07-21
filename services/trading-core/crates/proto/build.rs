//! Compile the shared Protobuf contract into Rust gRPC code at build time.
//!
//! The single source of truth is `packages/contracts/proto/market.proto`
//! (CLAUDE.md section 4: contracts first). Generated code is never committed —
//! `cargo build` regenerates it, so Rust and Python cannot drift from the proto.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // crates/proto -> crates -> trading-core -> services -> repo root
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest
        .ancestors()
        .nth(4)
        .expect("repo root above crates/proto");
    let proto_dir = repo_root.join("packages/contracts/proto");
    let market_proto = proto_dir.join("market.proto");
    let execution_proto = proto_dir.join("execution.proto");
    let broker_proto = proto_dir.join("broker.proto");
    let protos = [&market_proto, &execution_proto, &broker_proto];

    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[market_proto, execution_proto, broker_proto], &[proto_dir])?;
    Ok(())
}
