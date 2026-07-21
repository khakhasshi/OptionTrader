"""Continuous read-only broker fact reconciliation supervisor."""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Any

import grpc
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.persistence import (
    pending_reconciliation_orders,
    persist_broker_reconciliation,
    persist_broker_reconciliation_failure,
    persist_order_projection,
)
from app.trading.grpc_client import (
    begin_broker_reconciliation,
    commit_broker_reconciliation,
    reconcile_execution_order,
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _grpc_reason(exc: grpc.RpcError) -> str:
    try:
        return f"BROKER_RPC_{exc.code().name}"
    except (AttributeError, NotImplementedError):
        return "BROKER_RPC_UNKNOWN"


@dataclass
class ReconciliationStatus:
    broker_id: str = "ibkr"
    running: bool = False
    last_attempt_at_utc: str | None = None
    last_success_at_utc: str | None = None
    broker_reconciled: bool = False
    snapshot_sequence: int | None = None
    snapshot_hash: str | None = None
    unresolved_order_ids: list[str] = field(default_factory=list)
    mismatch_codes: list[str] = field(default_factory=list)
    failure_code: str | None = None


class BrokerReconciliationSupervisor:
    def __init__(self) -> None:
        self._lock = Lock()
        self._status = ReconciliationStatus()

    def status(self) -> dict[str, Any]:
        with self._lock:
            return asdict(self._status)

    def note_startup(self, unresolved: int) -> None:
        with self._lock:
            self._status.unresolved_order_ids = ["UNRESOLVED_AT_STARTUP"] * unresolved

    def run_once(self, engine: Engine, broker_id: str = "ibkr") -> dict[str, Any]:
        with self._lock:
            self._status.running = True
            self._status.last_attempt_at_utc = _utc_now()
            self._status.failure_code = None
            self._status.broker_id = broker_id

        unresolved: list[str] = []
        batch: Any | None = None
        try:
            for order_id, order_broker in pending_reconciliation_orders(engine):
                try:
                    order = reconcile_execution_order(order_id)
                    still_pending = order.state == "RECONCILE_PENDING"
                    persist_order_projection(
                        engine,
                        order,
                        action=(
                            "BROKER_RECONCILIATION_PENDING"
                            if still_pending
                            else "BROKER_AUTO_RECONCILED"
                        ),
                        actor="rust-execution-gateway",
                    )
                    if still_pending:
                        unresolved.append(order_id)
                except grpc.RpcError as exc:
                    unresolved.append(order_id)
                    persist_broker_reconciliation_failure(
                        engine, order_broker, _grpc_reason(exc), order_id=order_id
                    )
            batch = begin_broker_reconciliation(broker_id)
            mismatches = persist_broker_reconciliation(engine, batch)
            reconciled, reasons = commit_broker_reconciliation(
                batch,
                persistence_succeeded=True,
                mismatch_codes=mismatches,
            )
            final_codes = sorted(set(mismatches + reasons))
            with self._lock:
                self._status.running = False
                self._status.broker_reconciled = reconciled and not unresolved
                self._status.snapshot_sequence = int(batch.snapshot_sequence)
                self._status.snapshot_hash = str(batch.snapshot_hash)
                self._status.unresolved_order_ids = unresolved
                self._status.mismatch_codes = final_codes
                self._status.failure_code = None
                if self._status.broker_reconciled:
                    self._status.last_success_at_utc = _utc_now()
                return asdict(self._status)
        except grpc.RpcError as exc:
            reason = _grpc_reason(exc)
        except (ValueError, TypeError, ArithmeticError, AttributeError):
            reason = "BROKER_SNAPSHOT_INVALID"
        except SQLAlchemyError as exc:
            # Exception text can include connection details, so audit only the
            # SQLAlchemy exception class.
            reason = f"BROKER_PERSISTENCE_{type(exc).__name__.upper()}"
        except Exception as exc:  # noqa: BLE001 - supervisor must not silently die
            reason = f"BROKER_RUNTIME_{type(exc).__name__.upper()}"

        if batch is not None:
            try:
                commit_broker_reconciliation(
                    batch,
                    persistence_succeeded=False,
                    mismatch_codes=[],
                )
            except (grpc.RpcError, ValueError):
                pass
        try:
            persist_broker_reconciliation_failure(engine, broker_id, reason)
        except Exception:  # noqa: BLE001 - status must still expose the failed cycle
            pass
        with self._lock:
            self._status.running = False
            self._status.broker_reconciled = False
            self._status.unresolved_order_ids = unresolved
            self._status.failure_code = reason
            return asdict(self._status)

    async def serve(self, engine: Engine, interval_seconds: int) -> None:
        while True:
            await asyncio.to_thread(self.run_once, engine)
            await asyncio.sleep(interval_seconds)


supervisor = BrokerReconciliationSupervisor()


__all__ = ["BrokerReconciliationSupervisor", "ReconciliationStatus", "supervisor"]
