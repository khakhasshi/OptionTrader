#!/usr/bin/env bash
# Generate cross-language types from contracts.
# Phase 0: validate JSON Schema + fixtures. Codegen (Protobuf->TS/Py/Rust,
# JSON Schema->Pydantic/TS types) is wired in as proto/ and generators land.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT/packages/contracts"

echo "==> Validating JSON Schema + fixtures"
uv run --with jsonschema --with pytest pytest "$ROOT/tests/contract" -q

echo "==> Protobuf codegen: pending proto/ definitions (Phase 0 P0-5 follow-up)"
echo "==> OpenAPI client codegen: pending FastAPI app (P0-3)"
echo "contracts step complete"
