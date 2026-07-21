from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient
import grpc
import pytest

from app import main
from app.trading import reconciliation
from app.trading.models import (
    CandidateTradePlan,
    ExecutionOrder,
    RiskDecision,
    StageCandidateResult,
)

_ROOT = Path(__file__).resolve().parents[3]
_FIXTURES = _ROOT / "packages/contracts/fixtures"


def _plan() -> CandidateTradePlan:
    return CandidateTradePlan.model_validate(
        json.loads((_FIXTURES / "candidate_trade_plan.sample.json").read_text())
    )


def _result(state: str = "AWAITING_CONFIRMATION") -> StageCandidateResult:
    plan = _plan()
    decision = RiskDecision.model_validate(
        json.loads((_FIXTURES / "risk_decision.sample.json").read_text())
    )
    order = ExecutionOrder.model_validate(
        {
            "schema_version": "1.1",
            "order_id": "order_demo_001",
            "plan_id": plan.plan_id,
            "plan_hash": plan.plan_hash,
            "idempotency_key": plan.idempotency_key,
            "session_id": plan.session_id,
            "broker_id": plan.broker_id,
            "execution_mode": plan.execution_mode,
            "state": state,
            "total_quantity": 1,
            "filled_quantity": 0,
            "broker_order_id": "paper-order-1" if state == "WORKING" else None,
            "expires_at_utc": plan.expires_at_utc,
            "updated_at_utc": "2026-07-20T14:30:02Z",
            "state_version": 1,
            "broker_child_order_ids": [],
            "broker_child_orders": [],
            "residual_exposure": False,
            "risk_reason_codes": [],
        }
    )
    return StageCandidateResult(
        initial_risk_decision=decision,
        order=order,
        confirmation_token="confirmation-secret",
    )


def test_stage_fails_before_gateway_when_audit_database_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    called = False

    def forbidden(*_args: object, **_kwargs: object) -> StageCandidateResult:
        nonlocal called
        called = True
        return _result()

    monkeypatch.setattr(main, "grpc_stage_candidate", forbidden)
    response = TestClient(main.app).post(
        "/api/v1/trading/candidates/stage", json=_plan().model_dump(mode="json")
    )
    assert response.status_code == 503
    assert response.json()["detail"] == "execution_audit_unavailable"
    assert called is False


def test_stage_fails_before_gateway_when_confirmation_store_key_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(main, "_require_execution_engine", lambda: object())
    monkeypatch.delenv("OPTIONTRADER_CONFIRMATION_FERNET_KEY", raising=False)
    called = False

    def forbidden(*_args: object, **_kwargs: object) -> StageCandidateResult:
        nonlocal called
        called = True
        return _result()

    monkeypatch.setattr(main, "grpc_stage_candidate", forbidden)
    response = TestClient(main.app).post(
        "/api/v1/trading/candidates/stage", json=_plan().model_dump(mode="json")
    )
    assert response.status_code == 503
    assert response.json()["detail"] == "confirmation_store_unavailable"
    assert called is False


def test_stage_returns_challenge_but_persistence_never_receives_it_as_separate_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _result()
    observed: dict[str, Any] = {}
    cipher = object()
    monkeypatch.setattr(main, "_require_execution_engine", lambda: object())
    monkeypatch.setattr(main, "_require_confirmation_cipher", lambda: cipher)
    monkeypatch.setattr(main, "staged_plan_projection", lambda *_args: None)
    monkeypatch.setattr(main, "grpc_stage_candidate", lambda *_args: result)

    def persist(
        _engine: object,
        plan: CandidateTradePlan,
        staged: StageCandidateResult,
        received_cipher: object,
    ) -> bool:
        observed["plan"] = plan
        observed["staged"] = staged
        observed["cipher"] = received_cipher
        return True

    monkeypatch.setattr(main, "persist_staged_candidate", persist)
    response = TestClient(main.app).post(
        "/api/v1/trading/candidates/stage", json=_plan().model_dump(mode="json")
    )
    assert response.status_code == 200
    assert "confirmation_token" not in response.json()
    assert observed["plan"].plan_hash == _plan().plan_hash
    assert observed["cipher"] is cipher


def test_confirmation_intent_is_durable_before_gateway_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _result()
    assert result.order is not None
    working = result.order.model_copy(update={"state": "WORKING", "broker_order_id": "paper-1"})
    calls: list[str] = []
    monkeypatch.setattr(main, "_require_execution_engine", lambda: object())
    monkeypatch.setattr(main, "_require_confirmation_cipher", lambda: object())

    def intent(*_args: object, **_kwargs: object) -> str:
        calls.append("intent")
        return "confirmation-secret"

    def confirm(*_args: object, **_kwargs: object) -> ExecutionOrder:
        calls.append("gateway")
        return working

    def projection(*_args: object, **_kwargs: object) -> bool:
        calls.append("projection")
        return True

    monkeypatch.setattr(main, "claim_confirmation_intent", intent)
    monkeypatch.setattr(main, "grpc_confirm_candidate", confirm)
    monkeypatch.setattr(main, "persist_order_projection", projection)
    response = TestClient(main.app).post(
        "/api/v1/trading/orders/order_demo_001/confirm",
        json={"plan_hash": _plan().plan_hash},
    )
    assert response.status_code == 200
    assert response.json()["state"] == "WORKING"
    assert calls == ["intent", "gateway", "projection"]


def test_repeated_confirmation_returns_existing_terminal_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _result()
    assert result.order is not None
    working = result.order.model_copy(update={"state": "WORKING", "broker_order_id": "paper-1"})
    monkeypatch.setattr(main, "_require_execution_engine", lambda: object())
    monkeypatch.setattr(main, "_require_confirmation_cipher", lambda: object())
    monkeypatch.setattr(main, "claim_confirmation_intent", lambda *_args: None)
    monkeypatch.setattr(main, "grpc_get_order", lambda *_args: working)
    monkeypatch.setattr(main, "persist_order_projection", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        main,
        "grpc_confirm_candidate",
        lambda *_args: (_ for _ in ()).throw(AssertionError("must not resubmit")),
    )
    response = TestClient(main.app).post(
        "/api/v1/trading/orders/order_demo_001/confirm",
        json={"plan_hash": _plan().plan_hash},
    )
    assert response.status_code == 200
    assert response.json()["broker_order_id"] == "paper-1"


def test_existing_durable_order_with_lost_gateway_state_requires_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _result()
    assert result.order is not None

    class MissingOrder(grpc.RpcError):  # type: ignore[misc]
        def code(self) -> grpc.StatusCode:
            return grpc.StatusCode.NOT_FOUND

    monkeypatch.setattr(main, "_require_execution_engine", lambda: object())
    monkeypatch.setattr(main, "_require_confirmation_cipher", lambda: object())
    monkeypatch.setattr(
        main,
        "staged_plan_projection",
        lambda *_args: ("AWAITING_CONFIRMATION", result.order),
    )
    monkeypatch.setattr(
        main, "grpc_get_order", lambda *_args: (_ for _ in ()).throw(MissingOrder())
    )
    monkeypatch.setattr(
        main,
        "grpc_stage_candidate",
        lambda *_args: (_ for _ in ()).throw(AssertionError("must not recreate order")),
    )
    response = TestClient(main.app).post(
        "/api/v1/trading/candidates/stage", json=_plan().model_dump(mode="json")
    )
    assert response.status_code == 409
    assert response.json()["detail"] == "execution_reconciliation_required"


def test_reconciliation_broker_config_rejects_multiple_authorities() -> None:
    assert main._reconciliation_brokers("ibkr") == ("ibkr",)
    assert main._reconciliation_brokers(" longbridge ") == ("longbridge",)
    with pytest.raises(ValueError, match="exactly one"):
        main._reconciliation_brokers("ibkr,longbridge")
    with pytest.raises(ValueError, match="exactly one"):
        main._reconciliation_brokers("")


def test_real_paper_execution_and_reconciliation_route_must_match() -> None:
    main._validate_execution_reconciliation_route("simulated-paper", False, ())
    main._validate_execution_reconciliation_route("ibkr-paper", True, ("ibkr",))
    main._validate_execution_reconciliation_route("longbridge-paper", True, ("longbridge",))
    with pytest.raises(ValueError, match="enabled reconciliation"):
        main._validate_execution_reconciliation_route("ibkr-paper", False, ())
    with pytest.raises(ValueError, match="same broker"):
        main._validate_execution_reconciliation_route("ibkr-paper", True, ("longbridge",))


def test_latest_order_never_serves_durable_projection_after_gateway_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _result()
    assert result.order is not None

    class MissingOrder(grpc.RpcError):  # type: ignore[misc]
        def code(self) -> grpc.StatusCode:
            return grpc.StatusCode.NOT_FOUND

    monkeypatch.setattr(main, "_require_execution_engine", lambda: object())
    monkeypatch.setattr(
        main, "latest_execution_ticket", lambda *_args, **_kwargs: (_plan(), result.order)
    )
    monkeypatch.setattr(
        main, "grpc_get_order", lambda *_args: (_ for _ in ()).throw(MissingOrder())
    )
    response = TestClient(main.app).get("/api/v1/trading/orders")
    assert response.status_code == 409
    assert response.json()["detail"] == "execution_reconciliation_required"


def test_latest_order_rejects_gateway_version_rollback(monkeypatch: pytest.MonkeyPatch) -> None:
    result = _result()
    assert result.order is not None
    durable = result.order.model_copy(update={"state_version": 3})
    stale = result.order.model_copy(update={"state_version": 2})
    monkeypatch.setattr(main, "_require_execution_engine", lambda: object())
    monkeypatch.setattr(
        main, "latest_execution_ticket", lambda *_args, **_kwargs: (_plan(), durable)
    )
    monkeypatch.setattr(main, "grpc_get_order", lambda *_args: stale)
    response = TestClient(main.app).get("/api/v1/trading/orders")
    assert response.status_code == 409
    assert response.json()["detail"] == "execution_reconciliation_required"


def test_startup_restore_persists_successful_broker_auto_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    working = _result("WORKING").order
    assert working is not None
    pending = working.model_copy(
        update={"state": "RECONCILE_PENDING", "state_version": 2, "residual_exposure": True}
    )
    actions: list[str] = []
    monkeypatch.setattr(main, "_require_execution_engine", lambda: object())
    monkeypatch.setattr(main, "_require_confirmation_cipher", lambda: object())
    monkeypatch.setattr(main, "rotate_confirmation_capabilities", lambda *_args: 0)
    monkeypatch.setattr(
        main, "restorable_execution_workflow", lambda *_args: [(_plan(), working, "")]
    )
    monkeypatch.setattr(
        main, "grpc_restore_workflow", lambda *_args: ([pending], [pending.order_id])
    )
    monkeypatch.setattr(main, "grpc_reconcile_execution_order", lambda *_args: working)

    def persist(*_args: object, **kwargs: object) -> bool:
        actions.append(str(kwargs["action"]))
        return True

    monkeypatch.setattr(
        main,
        "persist_order_projection",
        persist,
    )
    assert main.restore_durable_execution_workflow() == (
        1,
        {"ibkr": 0, "longbridge": 0},
    )
    assert actions == ["PROCESS_RESTART_RECONCILIATION", "BROKER_AUTO_RECONCILED"]


def test_startup_restore_keeps_unavailable_broker_order_unresolved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    working = _result("WORKING").order
    assert working is not None
    pending = working.model_copy(
        update={"state": "RECONCILE_PENDING", "state_version": 2, "residual_exposure": True}
    )

    class BrokerUnavailable(grpc.RpcError):  # type: ignore[misc]
        pass

    monkeypatch.setattr(main, "_require_execution_engine", lambda: object())
    monkeypatch.setattr(main, "_require_confirmation_cipher", lambda: object())
    monkeypatch.setattr(main, "rotate_confirmation_capabilities", lambda *_args: 0)
    monkeypatch.setattr(
        main, "restorable_execution_workflow", lambda *_args: [(_plan(), working, "")]
    )
    monkeypatch.setattr(
        main, "grpc_restore_workflow", lambda *_args: ([pending], [pending.order_id])
    )
    monkeypatch.setattr(
        main,
        "grpc_reconcile_execution_order",
        lambda *_args: (_ for _ in ()).throw(BrokerUnavailable()),
    )
    monkeypatch.setattr(main, "persist_order_projection", lambda *_args, **_kwargs: True)
    failures: list[tuple[str, str]] = []
    monkeypatch.setattr(
        main,
        "persist_broker_reconciliation_failure",
        lambda _engine, broker, reason, **_kwargs: failures.append((broker, reason)),
    )
    assert main.restore_durable_execution_workflow() == (
        1,
        {"ibkr": 1, "longbridge": 0},
    )
    assert failures == [("ibkr", "BROKER_RPC_FAILURE")]


def test_reconciliation_status_endpoint_exposes_unresolved_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        reconciliation.supervisor,
        "status",
        lambda _broker_id="ibkr": {
            "broker_id": "ibkr",
            "running": False,
            "last_attempt_at_utc": "2026-07-21T14:30:00Z",
            "last_success_at_utc": None,
            "broker_reconciled": False,
            "snapshot_sequence": 12,
            "snapshot_hash": "a" * 64,
            "unresolved_order_ids": ["order_demo_001"],
            "mismatch_codes": ["UNKNOWN_BROKER_FILL"],
            "failure_code": None,
        },
    )
    response = TestClient(main.app).get("/api/v1/trading/reconciliation")
    assert response.status_code == 200
    assert response.json()["broker_reconciled"] is False
    assert response.json()["unresolved_order_ids"] == ["order_demo_001"]


def test_reconciliation_selects_one_broker_per_shared_authority() -> None:
    assert main._reconciliation_brokers("longbridge") == ("longbridge",)
    with pytest.raises(ValueError):
        main._reconciliation_brokers("ibkr,longbridge")
    with pytest.raises(ValueError):
        main._reconciliation_brokers("")


def test_startup_restore_counts_broker_pending_response_as_unresolved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    working = _result("WORKING").order
    assert working is not None
    pending = working.model_copy(
        update={"state": "RECONCILE_PENDING", "state_version": 2, "residual_exposure": True}
    )
    actions: list[str] = []
    monkeypatch.setattr(main, "_require_execution_engine", lambda: object())
    monkeypatch.setattr(main, "_require_confirmation_cipher", lambda: object())
    monkeypatch.setattr(main, "rotate_confirmation_capabilities", lambda *_args: 0)
    monkeypatch.setattr(
        main, "restorable_execution_workflow", lambda *_args: [(_plan(), working, "")]
    )
    monkeypatch.setattr(
        main, "grpc_restore_workflow", lambda *_args: ([pending], [pending.order_id])
    )
    monkeypatch.setattr(main, "grpc_reconcile_execution_order", lambda *_args: pending)

    def persist(*_args: object, **kwargs: object) -> bool:
        actions.append(str(kwargs["action"]))
        return True

    monkeypatch.setattr(main, "persist_order_projection", persist)
    assert main.restore_durable_execution_workflow() == (
        1,
        {"ibkr": 1, "longbridge": 0},
    )
    assert actions[-1] == "BROKER_RECONCILIATION_PENDING"
