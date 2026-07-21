from __future__ import annotations

from datetime import datetime, timezone
from dataclasses import replace
import json
from pathlib import Path

from jsonschema import Draft202012Validator
import pytest
from referencing import Registry, Resource

from app.trading import CandidateInputs, QuotedLeg, build_candidate_plan
from app.trading.candidate import canonical_plan_hash

UTC = timezone.utc
_ROOT = Path(__file__).resolve().parents[3]


def _validator() -> Draft202012Validator:
    schema_dir = _ROOT / "packages/contracts/jsonschema"
    resources = {
        path.name: Resource.from_contents(json.loads(path.read_text()))
        for path in schema_dir.glob("*.json")
    }
    return Draft202012Validator(
        resources["candidate_trade_plan.json"].contents,
        registry=Registry().with_resources(list(resources.items())),
    )


def _inputs(**updates: object) -> CandidateInputs:
    values: dict[str, object] = {
        "session_id": "session-2026-07-20",
        "signal_id": "signal-1",
        "strategy": "LongGamma",
        "broker_id": "ibkr",
        "execution_mode": "PAPER",
        "occurred_at_utc": datetime(2026, 7, 20, 14, 30, tzinfo=UTC),
        "quoted_legs": (
            QuotedLeg(
                side="BUY",
                option_right="CALL",
                contract_id="QQQ-20260720-500-C",
                expiry="2026-07-20",
                strike="500",
                bid="2.40",
                ask="2.50",
                bid_size=20,
                ask_size=25,
                quote_at_utc=datetime(2026, 7, 20, 14, 29, 59, 800000, tzinfo=UTC),
                delta="0.52",
                gamma="0.08",
                theta="-0.12",
                vega="0.05",
                chain_snapshot_id="opt-1",
                broker_contract_id="123456",
            ),
        ),
        "risk_budget": "1000",
        "max_contracts": 2,
        "max_slippage": "0.10",
        "ttl_seconds": 60,
        "rule_version": "rules_p3_1.0.0",
        "data_snapshot_ids": ("mkt-1", "opt-1"),
    }
    values.update(updates)
    return CandidateInputs(**values)  # type: ignore[arg-type]


def test_long_gamma_plan_is_deterministic_sized_and_schema_valid() -> None:
    first = build_candidate_plan(_inputs())
    second = build_candidate_plan(_inputs())

    assert first == second
    assert first.legs[0].quantity == 2
    assert first.max_loss == "500.00"
    assert first.limit_price == "2.50"
    assert first.plan_hash == canonical_plan_hash(first)
    assert first.idempotency_key == f"submit_{first.plan_hash}"
    assert list(_validator().iter_errors(first.model_dump(mode="json", exclude_none=True))) == []


def test_defined_risk_credit_spread_sizes_by_width_minus_credit() -> None:
    legs = (
        QuotedLeg(
            "SELL",
            "CALL",
            "short",
            "2026-07-20",
            "500",
            "1.50",
            "1.55",
            20,
            20,
            datetime(2026, 7, 20, 14, 29, 59, 800000, tzinfo=UTC),
            "-0.40",
            "0.05",
            "-0.10",
            "0.04",
            "opt-1",
            "101",
        ),
        QuotedLeg(
            "BUY",
            "CALL",
            "hedge",
            "2026-07-20",
            "505",
            "0.45",
            "0.50",
            20,
            20,
            datetime(2026, 7, 20, 14, 29, 59, 800000, tzinfo=UTC),
            "0.20",
            "0.03",
            "-0.05",
            "0.02",
            "opt-1",
            "102",
        ),
    )
    plan = build_candidate_plan(
        _inputs(strategy="ShortPremium", quoted_legs=legs, risk_budget="900", max_contracts=5)
    )

    assert plan.limit_price == "1.00"
    assert plan.legs[0].quantity == 2
    assert plan.max_loss == "800.00"


def test_naked_short_crossed_market_and_insufficient_budget_fail_closed() -> None:
    naked = (
        QuotedLeg(
            "SELL",
            "CALL",
            "short",
            "2026-07-20",
            "500",
            "1.5",
            "1.6",
            10,
            10,
            datetime(2026, 7, 20, 14, 29, 59, tzinfo=UTC),
            "-0.4",
            "0.05",
            "-0.1",
            "0.04",
            "opt-1",
            "101",
        ),
    )
    with pytest.raises(ValueError, match="defined-risk spread"):
        build_candidate_plan(_inputs(strategy="ShortPremium", quoted_legs=naked))

    crossed = (
        QuotedLeg(
            "BUY",
            "CALL",
            "long",
            "2026-07-20",
            "500",
            "2.6",
            "2.5",
            10,
            10,
            datetime(2026, 7, 20, 14, 29, 59, tzinfo=UTC),
            "0.5",
            "0.05",
            "-0.1",
            "0.04",
            "opt-1",
            "101",
        ),
    )
    with pytest.raises(ValueError, match="crossed"):
        build_candidate_plan(_inputs(quoted_legs=crossed))

    with pytest.raises(ValueError, match="cannot fund"):
        build_candidate_plan(_inputs(risk_budget="100"))


def test_live_and_controlled_auto_generation_are_disabled() -> None:
    with pytest.raises(ValueError, match="disabled"):
        build_candidate_plan(_inputs(execution_mode="CONTROLLED_AUTO"))


def test_adaptive_limit_policy_is_hash_bound_and_schema_valid() -> None:
    plan = build_candidate_plan(
        _inputs(order_type="ADAPTIVE_LIMIT", adaptive_initial_aggressiveness_bps=2_500)
    )
    assert plan.order_side == "BUY"
    assert plan.adaptive_limit is not None
    assert plan.adaptive_limit.initial_aggressiveness_bps == 2_500
    assert plan.plan_hash == canonical_plan_hash(plan)
    assert list(_validator().iter_errors(plan.model_dump(mode="json", exclude_none=True))) == []


def test_native_contract_required_and_longbridge_combo_is_buy_first_eligible() -> None:
    leg = _inputs().quoted_legs[0]
    with pytest.raises(ValueError, match="broker-native"):
        build_candidate_plan(_inputs(quoted_legs=(replace(leg, broker_contract_id=None),)))
    hedge = replace(
        leg,
        contract_id="QQQ-20260720-505-C",
        strike="505",
        broker_contract_id="QQQ260720C00505000.US",
    )
    plan = build_candidate_plan(
        _inputs(
            broker_id="longbridge",
            quoted_legs=(
                replace(leg, broker_contract_id="QQQ260720C00500000.US"),
                hedge,
            ),
        )
    )
    assert len(plan.legs) == 2


def test_non_thetadata_market_or_option_proof_fails_closed() -> None:
    with pytest.raises(ValueError, match="market data provider"):
        build_candidate_plan(_inputs(market_data_provider="BROKER"))
    leg = replace(_inputs().quoted_legs[0], quote_provider="BROKER")
    with pytest.raises(ValueError, match="quote and Greeks provider"):
        build_candidate_plan(_inputs(quoted_legs=(leg,)))
