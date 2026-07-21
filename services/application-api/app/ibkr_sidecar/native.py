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
        self._last_message_at = 0.0
        self._ready = Event()
        self._accounts = Event()
        self._managed_accounts: set[str] = set()
        self._types: dict[str, Any] = {}

    @property
    def ready(self) -> bool:
        return self._ready.is_set() and self._app is not None and bool(self._app.isConnected())

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
