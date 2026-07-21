"""Loopback gRPC broker adapter backed by the official IBKR TWS API."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from threading import Lock
from typing import Any, Protocol
from zoneinfo import ZoneInfo

import grpc

from app.grpc_gen import broker_pb2, broker_pb2_grpc
from app.ibkr_sidecar.mapping import map_submit_request
from app.ibkr_sidecar.native import IbkrSocketClient


class IbkrBackend(Protocol):
    def snapshot(self) -> broker_pb2.BrokerSnapshot: ...

    def submit(
        self, request: broker_pb2.SubmitBrokerOrderRequest
    ) -> broker_pb2.BrokerOrderSnapshot: ...

    def cancel(self, broker_order_id: str) -> broker_pb2.BrokerOrderSnapshot: ...


def _status(value: str) -> Any:
    if value in {"PendingSubmit", "PreSubmitted", "Submitted", "PendingCancel"}:
        return broker_pb2.BROKER_ORDER_STATUS_WORKING
    if value == "Filled":
        return broker_pb2.BROKER_ORDER_STATUS_FILLED
    if value in {"Cancelled", "ApiCancelled"}:
        return broker_pb2.BROKER_ORDER_STATUS_CANCELLED
    if value == "Inactive":
        return broker_pb2.BROKER_ORDER_STATUS_REJECTED
    return broker_pb2.BROKER_ORDER_STATUS_RECONCILE_PENDING


def _side(value: str) -> Any:
    if value.upper() in {"BUY", "BOT"}:
        return broker_pb2.ORDER_SIDE_BUY
    if value.upper() in {"SELL", "SLD"}:
        return broker_pb2.ORDER_SIDE_SELL
    return broker_pb2.ORDER_SIDE_UNSPECIFIED


def _execution_time(value: str) -> str:
    for suffix in (" US/Eastern", " America/New_York"):
        if value.endswith(suffix):
            value = value[: -len(suffix)]
    for pattern in ("%Y%m%d %H:%M:%S %Z", "%Y%m%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(value, pattern)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=ZoneInfo("America/New_York"))
            return parsed.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
        except ValueError:
            continue
    raise ValueError("IBKR execution timestamp is invalid")


class NativeIbkrBackend:
    def __init__(self, client: IbkrSocketClient) -> None:
        self._client = client
        self._lock = Lock()
        self._requests: dict[str, broker_pb2.SubmitBrokerOrderRequest] = {}
        self._order_by_key: dict[str, str] = {}

    def _known_order(
        self, broker_order_id: str, raw: dict[str, object] | None = None
    ) -> broker_pb2.BrokerOrderSnapshot:
        request = self._requests[broker_order_id]
        status = (
            _status(str(raw.get("status", "")))
            if raw
            else broker_pb2.BROKER_ORDER_STATUS_RECONCILE_PENDING
        )
        filled = int(str(raw.get("filled", 0))) if raw else 0
        if 0 < filled < request.total_quantity:
            status = broker_pb2.BROKER_ORDER_STATUS_PARTIAL_FILL
        return broker_pb2.BrokerOrderSnapshot(
            broker_order_id=broker_order_id,
            idempotency_key=request.idempotency_key,
            plan_hash=request.plan_hash,
            status=status,
            total_quantity=request.total_quantity,
            filled_quantity=filled,
            submitted_price=request.submitted_price,
            legs=request.legs,
            side=request.side,
            order_type=request.order_type,
            adaptive_priority=request.adaptive_priority,
            residual_exposure=status
            in {
                broker_pb2.BROKER_ORDER_STATUS_WORKING,
                broker_pb2.BROKER_ORDER_STATUS_PARTIAL_FILL,
                broker_pb2.BROKER_ORDER_STATUS_RECONCILE_PENDING,
            },
        )

    def snapshot(self) -> broker_pb2.BrokerSnapshot:
        self._client.refresh_snapshot()
        raw = self._client.snapshot()
        reconciled = bool(raw["reconciled"])
        account_values = raw["account"]
        assert isinstance(account_values, dict)
        raw_orders = raw["orders"]
        assert isinstance(raw_orders, list)
        orders_by_id = {str(item["broker_order_id"]): item for item in raw_orders}
        with self._lock:
            known_orders = [
                self._known_order(order_id, orders_by_id.get(order_id))
                for order_id in self._requests
            ]
            unknown_orders = [
                broker_pb2.BrokerOrderSnapshot(
                    broker_order_id=order_id,
                    idempotency_key=f"external:{order_id}",
                    plan_hash="0" * 64,
                    status=_status(str(item.get("status", ""))),
                    total_quantity=max(1, int(str(item.get("quantity", 0)))),
                    filled_quantity=max(0, int(str(item.get("filled", 0)))),
                    submitted_price=str(item.get("submitted_price", "")),
                    side=_side(str(item.get("side", ""))),
                    order_type=(
                        broker_pb2.BROKER_ORDER_TYPE_MARKET
                        if item.get("order_type") == "MKT"
                        else broker_pb2.BROKER_ORDER_TYPE_LIMIT
                    ),
                    residual_exposure=True,
                )
                for order_id, item in orders_by_id.items()
                if order_id not in self._requests
            ]
        positions_raw = raw["positions"]
        fills_raw = raw["fills"]
        assert isinstance(positions_raw, list) and isinstance(fills_raw, list)
        with self._lock:
            unknown_fill = any(
                str(item.get("broker_order_id", "")) not in self._requests for item in fills_raw
            )
        return broker_pb2.BrokerSnapshot(
            schema_version="1.0",
            snapshot_sequence=int(str(raw["sequence"])),
            account=broker_pb2.AccountSnapshot(
                broker_id=broker_pb2.BROKER_ID_IBKR,
                occurred_at_utc=datetime.now(UTC)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
                health=(
                    broker_pb2.BROKER_HEALTH_HEALTHY
                    if reconciled
                    else broker_pb2.BROKER_HEALTH_RECONCILING
                ),
                reconciled=reconciled and not unknown_orders and not unknown_fill,
                buying_power=str(account_values.get("BuyingPower", "0")),
                net_liquidation=str(account_values.get("NetLiquidation", "0")),
                currency=str(account_values.get("Currency", "USD")),
            ),
            positions=[broker_pb2.PositionSnapshot(**item) for item in positions_raw],
            orders=[*known_orders, *unknown_orders],
            fills=[
                broker_pb2.FillSnapshot(
                    fill_id=str(item["fill_id"]),
                    broker_order_id=str(item["broker_order_id"]),
                    contract_id=str(item["contract_id"]),
                    side=_side(str(item["side"])),
                    quantity=int(item["quantity"]),
                    price=str(item["price"]),
                    occurred_at_utc=_execution_time(str(item["occurred_at_utc"])),
                )
                for item in fills_raw
            ],
        )

    def submit(
        self, request: broker_pb2.SubmitBrokerOrderRequest
    ) -> broker_pb2.BrokerOrderSnapshot:
        with self._lock:
            existing_id = self._order_by_key.get(request.idempotency_key)
            if existing_id is not None:
                existing = self._requests[existing_id]
                if existing.SerializeToString(deterministic=True) != request.SerializeToString(
                    deterministic=True
                ):
                    raise ValueError("IBKR idempotency key conflicts with a different order")
                return self._known_order(existing_id)
        contract, order = map_submit_request(request, account=self._client.config.account)
        native_id = str(self._client.place_order(contract, order))
        with self._lock:
            saved = broker_pb2.SubmitBrokerOrderRequest()
            saved.CopyFrom(request)
            self._requests[native_id] = saved
            self._order_by_key[request.idempotency_key] = native_id
            return self._known_order(native_id)

    def cancel(self, broker_order_id: str) -> broker_pb2.BrokerOrderSnapshot:
        try:
            native_id = int(broker_order_id)
        except ValueError as exc:
            raise ValueError("IBKR broker_order_id must be numeric") from exc
        with self._lock:
            if broker_order_id not in self._requests:
                raise KeyError("IBKR order is not owned by this sidecar")
        self._client.cancel_order(native_id)
        return self._known_order(broker_order_id)


class IbkrBrokerService(broker_pb2_grpc.BrokerAdapterServiceServicer):  # type: ignore[misc]
    def __init__(self, backend: IbkrBackend) -> None:
        self._backend = backend

    @staticmethod
    async def _abort(context: grpc.aio.ServicerContext[Any, Any], exc: Exception) -> None:
        if isinstance(exc, ValueError):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, type(exc).__name__)
        if isinstance(exc, KeyError):
            await context.abort(grpc.StatusCode.NOT_FOUND, type(exc).__name__)
        await context.abort(grpc.StatusCode.UNAVAILABLE, type(exc).__name__)

    async def GetBrokerSnapshot(self, request: Any, context: Any) -> Any:
        if request.broker_id != broker_pb2.BROKER_ID_IBKR:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "wrong broker route")
        try:
            return await asyncio.to_thread(self._backend.snapshot)
        except Exception as exc:
            await self._abort(context, exc)
            raise AssertionError("gRPC abort must raise") from exc

    async def SubmitBrokerOrder(self, request: Any, context: Any) -> Any:
        if request.broker_id != broker_pb2.BROKER_ID_IBKR:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "wrong broker route")
        try:
            return await asyncio.to_thread(self._backend.submit, request)
        except Exception as exc:
            await self._abort(context, exc)
            raise AssertionError("gRPC abort must raise") from exc

    async def CancelBrokerOrder(self, request: Any, context: Any) -> Any:
        if request.broker_id != broker_pb2.BROKER_ID_IBKR:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "wrong broker route")
        try:
            return await asyncio.to_thread(self._backend.cancel, str(request.broker_order_id))
        except Exception as exc:
            await self._abort(context, exc)
            raise AssertionError("gRPC abort must raise") from exc

    async def ReconcileBroker(self, request: Any, context: Any) -> Any:
        if request.broker_id != broker_pb2.BROKER_ID_IBKR:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "wrong broker route")
        try:
            snapshot = await asyncio.to_thread(self._backend.snapshot)
        except Exception as exc:
            await self._abort(context, exc)
            raise AssertionError("gRPC abort must raise") from exc
        matched = bool(snapshot.account.reconciled) and snapshot.snapshot_sequence >= int(
            request.expected_snapshot_sequence
        )
        return broker_pb2.ReconcileBrokerResponse(
            snapshot=snapshot,
            matched=matched,
            mismatch_codes=[] if matched else ["BROKER_SNAPSHOT_NOT_RECONCILED"],
        )


__all__ = ["IbkrBackend", "IbkrBrokerService", "NativeIbkrBackend"]
