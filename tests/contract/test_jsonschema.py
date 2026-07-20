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
