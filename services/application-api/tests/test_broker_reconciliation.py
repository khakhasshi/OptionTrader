from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.trading import reconciliation


def _batch() -> SimpleNamespace:
    return SimpleNamespace(
        broker_id=2,
        snapshot_sequence=42,
        snapshot_hash="a" * 64,
    )


def test_supervisor_persists_before_hash_commit_and_reports_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def begin(_broker: str) -> SimpleNamespace:
        calls.append("begin")
        return _batch()

    def persist(_engine: object, _raw_batch: object) -> list[str]:
        calls.append("persist")
        return []

    monkeypatch.setattr(reconciliation, "pending_reconciliation_orders", lambda _engine: [])
    monkeypatch.setattr(reconciliation, "begin_broker_reconciliation", begin)
    monkeypatch.setattr(reconciliation, "persist_broker_reconciliation", persist)

    def commit(
        _batch: object,
        *,
        persistence_succeeded: bool,
        mismatch_codes: list[str],
    ) -> tuple[bool, list[str]]:
        assert persistence_succeeded is True
        assert mismatch_codes == []
        calls.append("commit")
        return True, []

    monkeypatch.setattr(reconciliation, "commit_broker_reconciliation", commit)
    status = reconciliation.BrokerReconciliationSupervisor().run_once(object())  # type: ignore[arg-type]
    assert calls == ["begin", "persist", "commit"]
    assert status["broker_reconciled"] is True
    assert status["snapshot_sequence"] == 42


def test_supervisor_persistence_failure_sends_negative_receipt_and_stays_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipts: list[bool] = []
    audits: list[str] = []

    def audit(_engine: object, _broker: str, reason: str, **_kwargs: object) -> None:
        audits.append(reason)

    def fail_persistence(_engine: object, _raw_batch: object) -> list[str]:
        raise ValueError("bad snapshot")

    def negative_commit(
        _raw_batch: object,
        *,
        persistence_succeeded: bool,
        mismatch_codes: list[str],
    ) -> tuple[bool, list[str]]:
        receipts.append(persistence_succeeded)
        return False, mismatch_codes

    monkeypatch.setattr(reconciliation, "pending_reconciliation_orders", lambda _engine: [])
    monkeypatch.setattr(reconciliation, "begin_broker_reconciliation", lambda _broker: _batch())
    monkeypatch.setattr(reconciliation, "persist_broker_reconciliation", fail_persistence)
    monkeypatch.setattr(reconciliation, "commit_broker_reconciliation", negative_commit)
    monkeypatch.setattr(
        reconciliation,
        "persist_broker_reconciliation_failure",
        audit,
    )
    status = reconciliation.BrokerReconciliationSupervisor().run_once(object())  # type: ignore[arg-type]
    assert receipts == [False]
    assert audits == ["BROKER_SNAPSHOT_INVALID"]
    assert status["broker_reconciled"] is False
    assert status["failure_code"] == "BROKER_SNAPSHOT_INVALID"
