"""Contract tests: every JSON Schema compiles and fixtures validate.

Run: uv run --with jsonschema --with pytest pytest tests/contract
Part of `make test` once application-api wires its test runner.
"""
import glob
import json
import os

import pytest
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCHEMA_DIR = os.path.join(ROOT, "packages", "contracts", "jsonschema")
FIXTURE_DIR = os.path.join(ROOT, "packages", "contracts", "fixtures")


def _registry():
    res = {
        os.path.basename(f): Resource.from_contents(json.load(open(f)))
        for f in glob.glob(os.path.join(SCHEMA_DIR, "*.json"))
    }
    return Registry().with_resources(list(res.items())), res


@pytest.mark.parametrize("schema_file", glob.glob(os.path.join(SCHEMA_DIR, "*.json")))
def test_schema_compiles(schema_file):
    Draft202012Validator.check_schema(json.load(open(schema_file)))


def test_market_snapshot_fixture_validates():
    reg, res = _registry()
    fx = json.load(open(os.path.join(FIXTURE_DIR, "market_snapshot.sample.json")))
    v = Draft202012Validator(res["market_snapshot.json"].contents, registry=reg)
    assert list(v.iter_errors(fx)) == []


def test_market_snapshot_rejects_invalid():
    reg, res = _registry()
    bad = {
        "schema_version": "1.0", "snapshot_id": "x",
        "occurred_at_utc": "2026-07-20T13:45:00Z", "symbol": "SPY.US",
        "price": 500.0, "open": "1", "vwap": "1",
        "sequence_number": 1, "data_health": "WHAT",
    }
    v = Draft202012Validator(res["market_snapshot.json"].contents, registry=reg)
    assert len(list(v.iter_errors(bad))) >= 3


def test_snapshot_unavailable_fixture_validates():
    reg, res = _registry()
    fx = json.load(open(os.path.join(FIXTURE_DIR, "snapshot_unavailable.sample.json")))
    v = Draft202012Validator(res["snapshot_unavailable.json"].contents, registry=reg)
    assert list(v.iter_errors(fx)) == []


def test_snapshot_unavailable_rejects_fake_marketsnapshot():
    """A partial MarketSnapshot (has price/snapshot_id) must NOT validate as
    SnapshotUnavailable — the two contracts are deliberately disjoint."""
    reg, res = _registry()
    fake = {
        "schema_version": "1.0",
        "error": "snapshot_unavailable",
        "reason": "x",
        "data_health": "STALE",
        "price": "500.0",
        "snapshot_id": "sneaky",
    }
    v = Draft202012Validator(res["snapshot_unavailable.json"].contents, registry=reg)
    assert len(list(v.iter_errors(fake))) >= 1


def _service_health_validator():
    reg, res = _registry()
    schema = {"$ref": "health.json#/$defs/ServiceHealth"}
    return Draft202012Validator(schema, registry=reg)


@pytest.mark.parametrize(
    "fixture", ["service_health.healthy.json", "service_health.unreachable.json"]
)
def test_service_health_fixture_validates(fixture):
    v = _service_health_validator()
    fx = json.load(open(os.path.join(FIXTURE_DIR, fixture)))
    assert list(v.iter_errors(fx)) == []


@pytest.mark.parametrize(
    "fixture", ["service_health.healthy.json", "service_health.unreachable.json"]
)
def test_service_health_gate_invariant(fixture):
    """new_position_allowed must equal the fail-closed conjunction across all tiers."""
    fx = json.load(open(os.path.join(FIXTURE_DIR, fixture)))
    expected = (
        fx["data_health"] == "HEALTHY"
        and fx["broker_health"] == "HEALTHY"
        and fx["reconciled"] is True
    )
    assert fx["new_position_allowed"] is expected


def test_cockpit_state_fixture_validates():
    reg, res = _registry()
    fx = json.load(open(os.path.join(FIXTURE_DIR, "cockpit_state.sample.json")))
    v = Draft202012Validator(res["cockpit_state.json"].contents, registry=reg)
    assert list(v.iter_errors(fx)) == []


def test_cockpit_state_allows_null_derivations_when_fail_closed():
    """A fail-closed frame carries no snapshot/regime/vol/signal and must not
    permit new positions — the disconnected/stale case the UI renders as No Trade."""
    reg, res = _registry()
    frame = {
        "schema_version": "1.0",
        "seq": 0,
        "session_id": "sess_x",
        "server_time_utc": "2026-07-20T13:45:00Z",
        "connection": "DISCONNECTED",
        "new_position_allowed": False,
        "snapshot": None,
        "regime": None,
        "vol": None,
        "signal": None,
        "risk_flags": ["upstream stream disconnected"],
    }
    v = Draft202012Validator(res["cockpit_state.json"].contents, registry=reg)
    assert list(v.iter_errors(frame)) == []


def test_cockpit_state_rejects_bad_enums_and_extra_fields():
    reg, res = _registry()
    bad = {
        "schema_version": "1.0",
        "seq": 1,
        "session_id": "s",
        "server_time_utc": "2026-07-20T13:45:00Z",
        "connection": "MAYBE",
        "new_position_allowed": True,
        "risk_flags": [],
        "unexpected": "field",
    }
    v = Draft202012Validator(res["cockpit_state.json"].contents, registry=reg)
    assert len(list(v.iter_errors(bad))) >= 2
