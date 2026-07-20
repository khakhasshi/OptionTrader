#!/usr/bin/env bash
# Generate cross-language types from contracts.
# Validates JSON Schema + fixtures, then generates Python gRPC stubs. Rust gRPC
# code is generated at build time by crates/proto/build.rs (tonic-build).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT/packages/contracts"

echo "==> Validating JSON Schema + fixtures"
uv run --with jsonschema --with pytest --with referencing pytest "$ROOT/tests/contract" -q

echo "==> Protobuf codegen (Python gRPC stubs -> app/grpc_gen/)"
bash "$ROOT/scripts/gen_python_grpc.sh"

echo "==> Rust gRPC codegen runs at build time via crates/proto/build.rs (cargo build)"
echo "==> OpenAPI client codegen: pending FastAPI app (P0-3)"
echo "contracts step complete"
