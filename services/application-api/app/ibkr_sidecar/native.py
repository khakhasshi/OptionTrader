"""Thin runtime wrapper around the official IBKR TWS Python API.

The official ``ibapi`` package is installed from the TWS API distribution on
the sidecar host. Import is delayed so the main Application API never acquires
the trading capability by importing this package.
"""

from __future__ import annotations

import importlib
from threading import Event, Lock, Thread
from time import monotonic, sleep
from typing import Any

from app.ibkr_sidecar.config import IbkrEndpointConfig
from app.ibkr_sidecar.mapping import IbkrContractSpec, IbkrOrderSpec


class IbkrSocketClient:
    def __init__(self, config: IbkrEndpointConfig) -> None:
        self.config = config
        self._app: Any | None = None
        self._next_order_id: int | None = None
        self._id_lock = Lock()
        self._message_lock = Lock()
        self._refresh_lock = Lock()
        self._last_message_at = 0.0
        self._ready = Event()
        self._accounts = Event()
        self._managed_accounts: set[str] = set()
        self._types: dict[str, Any] = {}
        self._snapshot_lock = Lock()
        self._account_values: dict[str, str] = {}
        self._positions: dict[str, dict[str, object]] = {}
        self._open_orders: dict[int, dict[str, object]] = {}
        self._executions: dict[str, dict[str, object]] = {}
        self._snapshot_events = {
            name: Event() for name in ("account", "positions", "orders", "fills")
        }
        self._snapshot_sequence = 0

    @property
    def ready(self) -> bool:
        return self._ready.is_set() and self._app is not None and bool(self._app.isConnected())

    @property
    def snapshot_reconciled(self) -> bool:
        return self.ready and all(event.is_set() for event in self._snapshot_events.values())

    def connect(self) -> None:
        try:
            client_module = importlib.import_module("ibapi.client")
            wrapper_module = importlib.import_module("ibapi.wrapper")
            contract_module = importlib.import_module("ibapi.contract")
            order_module = importlib.import_module("ibapi.order")
            tag_module = importlib.import_module("ibapi.tag_value")
        except ModuleNotFoundError as exc:
            raise RuntimeError("official IBKR TWS API package is not installed") from exc
        owner = self

        class NativeApp(wrapper_module.EWrapper, client_module.EClient):  # type: ignore[misc]
            def __init__(self) -> None:
                client_module.EClient.__init__(self, self)

            def nextValidId(self, order_id: int) -> None:  # noqa: N802
                owner._next_order_id = order_id
                owner._ready.set()

            def managedAccounts(self, accounts: str) -> None:  # noqa: N802
                owner._managed_accounts = {item for item in accounts.split(",") if item}
                owner._accounts.set()

            def accountSummary(  # noqa: N802
                self, req_id: int, account: str, tag: str, value: str, currency: str
            ) -> None:
                del req_id
                if account == owner.config.account:
                    with owner._snapshot_lock:
                        owner._account_values[tag] = value
                        if currency:
                            owner._account_values["Currency"] = currency

            def accountSummaryEnd(self, req_id: int) -> None:  # noqa: N802
                del req_id
                owner._finish_snapshot_part("account")

            def position(self, account: str, contract: Any, position: Any, avg_cost: float) -> None:
                if account != owner.config.account:
                    return
                contract_id = str(
                    getattr(contract, "conId", "") or getattr(contract, "localSymbol", "")
                )
                if not contract_id:
                    return
                with owner._snapshot_lock:
                    owner._positions[contract_id] = {
                        "contract_id": contract_id,
                        "quantity": int(position),
                        "average_price": str(avg_cost),
                    }

            def positionEnd(self) -> None:  # noqa: N802
                owner._finish_snapshot_part("positions")

            def openOrder(self, order_id: int, contract: Any, order: Any, order_state: Any) -> None:  # noqa: N802
                account = str(getattr(order, "account", ""))
                if account != owner.config.account:
                    return
                order_type = str(getattr(order, "orderType", ""))
                combo_legs = list(getattr(contract, "comboLegs", []) or [])
                algo_params = list(getattr(order, "algoParams", []) or [])
                with owner._snapshot_lock:
                    owner._open_orders[order_id] = {
                        "broker_order_id": str(order_id),
                        "account": account,
                        "contract_id": str(
                            getattr(contract, "conId", "") or getattr(contract, "localSymbol", "")
                        ),
                        "contract_ids": [int(getattr(leg, "conId", 0)) for leg in combo_legs]
                        or [int(getattr(contract, "conId", 0))],
                        "combo_actions": [str(getattr(leg, "action", "")) for leg in combo_legs],
                        "sec_type": str(getattr(contract, "secType", "")),
                        "symbol": str(getattr(contract, "symbol", "")),
                        "exchange": str(getattr(contract, "exchange", "")),
                        "order_ref": str(getattr(order, "orderRef", "")),
                        "side": str(getattr(order, "action", "")),
                        "order_type": order_type,
                        "quantity": int(getattr(order, "totalQuantity", 0)),
                        "submitted_price": (
                            "" if order_type == "MKT" else str(getattr(order, "lmtPrice", "") or "")
                        ),
                        "algo_strategy": str(getattr(order, "algoStrategy", "")),
                        "adaptive_priority": next(
                            (
                                str(getattr(item, "value", ""))
                                for item in algo_params
                                if str(getattr(item, "tag", "")) == "adaptivePriority"
                            ),
                            "",
                        ),
                        "status": str(getattr(order_state, "status", "")),
                        "filled": 0,
                    }

            def openOrderEnd(self) -> None:  # noqa: N802
                owner._finish_snapshot_part("orders")

            def orderStatus(  # noqa: N802
                self,
                order_id: int,
                status: str,
                filled: Any,
                remaining: Any,
                avg_fill_price: float,
                *args: object,
            ) -> None:
                del remaining, avg_fill_price, args
                with owner._snapshot_lock:
                    order = owner._open_orders.get(order_id)
                    if order is None:
                        return
                    order["status"] = status
                    order["filled"] = int(filled)
                    owner._snapshot_sequence += 1

            def execDetails(self, req_id: int, contract: Any, execution: Any) -> None:  # noqa: N802
                del req_id
                fill_id = str(getattr(execution, "execId", ""))
                if not fill_id or str(getattr(execution, "acctNumber", "")) != owner.config.account:
                    return
                with owner._snapshot_lock:
                    owner._executions[fill_id] = {
                        "fill_id": fill_id,
                        "broker_order_id": str(getattr(execution, "orderId", "")),
                        "order_ref": str(getattr(execution, "orderRef", "")),
                        "contract_id": str(
                            getattr(contract, "conId", "") or getattr(contract, "localSymbol", "")
                        ),
                        "side": str(getattr(execution, "side", "")),
                        "quantity": int(getattr(execution, "shares", 0)),
                        "price": str(getattr(execution, "price", "")),
                        "occurred_at_utc": str(getattr(execution, "time", "")),
                    }

            def execDetailsEnd(self, req_id: int) -> None:  # noqa: N802
                del req_id
                owner._finish_snapshot_part("fills")

            def error(
                self,
                req_id: int,
                error_code: int,
                error_string: str,
                advanced_order_reject_json: str = "",
            ) -> None:
                del req_id, error_string, advanced_order_reject_json
                if error_code in {1100, 1101, 1102, 1300}:
                    owner._ready.clear()

            def connectionClosed(self) -> None:  # noqa: N802
                owner._ready.clear()

        app = NativeApp()
        self._app = app
        self._types = {
            "Contract": contract_module.Contract,
            "ComboLeg": contract_module.ComboLeg,
            "Order": order_module.Order,
            "TagValue": tag_module.TagValue,
        }
        execution_module = importlib.import_module("ibapi.execution")
        self._types["ExecutionFilter"] = execution_module.ExecutionFilter
        app.connect(self.config.host, self.config.port, self.config.client_id)
        Thread(target=app.run, name="ibkr-tws-api", daemon=True).start()
        if not self._ready.wait(self.config.connect_timeout_seconds):
            app.disconnect()
            raise TimeoutError("IBKR nextValidId handshake timed out")
        app.reqManagedAccts()
        if not self._accounts.wait(self.config.connect_timeout_seconds):
            app.disconnect()
            raise TimeoutError("IBKR managed-account handshake timed out")
        if self.config.account not in self._managed_accounts:
            app.disconnect()
            self._ready.clear()
            raise PermissionError("configured IBKR account is not managed by this session")
        self.refresh_snapshot()

    def disconnect(self) -> None:
        if self._app is not None:
            self._app.disconnect()
        self._ready.clear()

    def place_order(self, contract_spec: IbkrContractSpec, order_spec: IbkrOrderSpec) -> int:
        if not self.config.submission_enabled:
            raise PermissionError("IBKR submission is disabled")
        if not self.ready or self._next_order_id is None:
            raise ConnectionError("IBKR nextValidId handshake is not ready")
        if order_spec.account != self.config.account:
            raise PermissionError("IBKR order account mismatch")
        contract = self._contract(contract_spec)
        order = self._order(order_spec)
        with self._id_lock:
            if self._next_order_id is None:
                raise ConnectionError("IBKR order id is unavailable")
            order_id = self._next_order_id
            self._next_order_id += 1
        self._pace_message()
        self._app.placeOrder(order_id, contract, order)
        return order_id

    def cancel_order(self, order_id: int) -> None:
        if not self.config.submission_enabled:
            raise PermissionError("IBKR submission is disabled")
        if not self.ready:
            raise ConnectionError("IBKR connection is not ready")
        self._pace_message()
        self._app.cancelOrder(order_id, "")

    def refresh_snapshot(self) -> None:
        with self._refresh_lock:
            if not self.ready:
                raise ConnectionError("IBKR connection is not ready")
            for event in self._snapshot_events.values():
                event.clear()
            with self._snapshot_lock:
                self._account_values.clear()
                self._positions.clear()
                self._open_orders.clear()
                self._executions.clear()
            self._app.reqAccountSummary(90_001, "All", "BuyingPower,NetLiquidation")
            self._app.reqPositions()
            self._app.reqAllOpenOrders()
            execution_filter = self._types["ExecutionFilter"]()
            execution_filter.acctCode = self.config.account
            self._app.reqExecutions(90_002, execution_filter)
            deadline = monotonic() + self.config.connect_timeout_seconds
            for event in self._snapshot_events.values():
                remaining = deadline - monotonic()
                if remaining <= 0 or not event.wait(remaining):
                    self._ready.clear()
                    raise TimeoutError("IBKR account/order/fill snapshot timed out")

    def snapshot(self) -> dict[str, object]:
        with self._snapshot_lock:
            return {
                "sequence": self._snapshot_sequence,
                "reconciled": self.snapshot_reconciled,
                "account": dict(self._account_values),
                "positions": [dict(item) for item in self._positions.values()],
                "orders": [dict(item) for item in self._open_orders.values()],
                "fills": [dict(item) for item in self._executions.values()],
            }

    def _finish_snapshot_part(self, name: str) -> None:
        with self._snapshot_lock:
            self._snapshot_sequence += 1
        self._snapshot_events[name].set()

    def _pace_message(self) -> None:
        # TWS API permits at most 50 client messages per second. Serialize this
        # sidecar's mutations at 20 ms spacing rather than relying on callers.
        with self._message_lock:
            wait = 0.02 - (monotonic() - self._last_message_at)
            if wait > 0:
                sleep(wait)
            self._last_message_at = monotonic()

    def _contract(self, spec: IbkrContractSpec) -> Any:
        contract = self._types["Contract"]()
        contract.secType = spec.sec_type
        contract.symbol = spec.symbol
        contract.currency = spec.currency
        contract.exchange = spec.exchange
        if spec.con_id is not None:
            contract.conId = spec.con_id
        if spec.combo_legs:
            contract.comboLegs = []
            for source in spec.combo_legs:
                leg = self._types["ComboLeg"]()
                leg.conId = source.con_id
                leg.ratio = source.ratio
                leg.action = source.action
                leg.exchange = source.exchange
                contract.comboLegs.append(leg)
        return contract

    def _order(self, spec: IbkrOrderSpec) -> Any:
        order = self._types["Order"]()
        order.action = spec.action
        order.orderType = spec.order_type
        order.totalQuantity = spec.quantity
        order.tif = spec.tif
        order.account = spec.account
        order.orderRef = spec.order_ref
        order.outsideRth = False
        order.transmit = True
        if spec.limit_price is not None:
            order.lmtPrice = float(spec.limit_price)
        if spec.adaptive_priority is not None:
            order.algoStrategy = "Adaptive"
            order.algoParams = [self._types["TagValue"]("adaptivePriority", spec.adaptive_priority)]
        return order
