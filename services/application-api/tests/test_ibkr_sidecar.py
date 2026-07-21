from __future__ import annotations

import asyncio
from app.grpc_gen import broker_pb2
from app.ibkr_sidecar.config import IbkrEndpointConfig
from app.ibkr_sidecar.mapping import map_submit_request
from app.ibkr_sidecar.native import IbkrSocketClient
from app.ibkr_sidecar.service import IbkrBrokerService, NativeIbkrBackend
from app.grpc_gen import broker_pb2_grpc
import grpc
import pytest
from types import SimpleNamespace


def _request(
    *,
    side: broker_pb2.OrderSide = broker_pb2.ORDER_SIDE_SELL,
    order_type: broker_pb2.BrokerOrderType = broker_pb2.BROKER_ORDER_TYPE_ADAPTIVE_LIMIT,
) -> broker_pb2.SubmitBrokerOrderRequest:
    return broker_pb2.SubmitBrokerOrderRequest(
        broker_id=broker_pb2.BROKER_ID_IBKR,
        idempotency_key="submit-key",
        plan_hash="a" * 64,
        total_quantity=2,
        submitted_price="1.25" if order_type != broker_pb2.BROKER_ORDER_TYPE_MARKET else "",
        side=side,
        order_type=order_type,
        adaptive_priority=(
            broker_pb2.ADAPTIVE_PRIORITY_NORMAL
            if order_type == broker_pb2.BROKER_ORDER_TYPE_ADAPTIVE_LIMIT
            else broker_pb2.ADAPTIVE_PRIORITY_UNSPECIFIED
        ),
        legs=[
            broker_pb2.BrokerOrderLeg(
                contract_id="short",
                broker_contract_id="101",
                symbol="QQQ",
                exchange="SMART",
                side=broker_pb2.ORDER_SIDE_SELL,
                quantity=2,
            ),
            broker_pb2.BrokerOrderLeg(
                contract_id="hedge",
                broker_contract_id="102",
                symbol="QQQ",
                exchange="SMART",
                side=broker_pb2.ORDER_SIDE_BUY,
                quantity=2,
            ),
        ],
    )


def test_tws_and_gateway_defaults_are_distinct_and_loopback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPTIONTRADER_IBKR_ACCOUNT", "DU123")
    monkeypatch.setenv("OPTIONTRADER_IBKR_MODE", "TWS")
    tws = IbkrEndpointConfig.from_env()
    assert (tws.host, tws.port, tws.paper, tws.submission_enabled) == (
        "127.0.0.1",
        7497,
        True,
        False,
    )
    monkeypatch.setenv("OPTIONTRADER_IBKR_MODE", "GATEWAY")
    gateway = IbkrEndpointConfig.from_env()
    assert gateway.port == 4002


def test_sell_combo_normalizes_bag_legs_without_changing_intended_execution() -> None:
    contract, order = map_submit_request(_request(), account="DU123")
    assert contract.sec_type == "BAG"
    assert [leg.action for leg in contract.combo_legs] == ["BUY", "SELL"]
    assert order.action == "SELL"
    assert order.order_type == "LMT"
    assert order.adaptive_priority == "Normal"


def test_market_limit_and_adaptive_semantics_are_strict() -> None:
    _, market = map_submit_request(
        _request(order_type=broker_pb2.BROKER_ORDER_TYPE_MARKET), account="DU123"
    )
    assert market.order_type == "MKT" and market.limit_price is None
    _, limit = map_submit_request(
        _request(order_type=broker_pb2.BROKER_ORDER_TYPE_LIMIT), account="DU123"
    )
    assert limit.order_type == "LMT" and str(limit.limit_price) == "1.25"
    missing_price = _request(order_type=broker_pb2.BROKER_ORDER_TYPE_LIMIT)
    missing_price.submitted_price = ""
    with pytest.raises(ValueError, match="submitted_price"):
        map_submit_request(missing_price, account="DU123")


def test_missing_conid_cross_symbol_and_remote_host_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing = _request()
    missing.legs[0].broker_contract_id = ""
    with pytest.raises(ValueError, match="conId"):
        map_submit_request(missing, account="DU123")
    crossed = _request()
    crossed.legs[1].symbol = "SPY"
    with pytest.raises(ValueError, match="underlying"):
        map_submit_request(crossed, account="DU123")
    monkeypatch.setenv("OPTIONTRADER_IBKR_ACCOUNT", "DU123")
    monkeypatch.setenv("OPTIONTRADER_IBKR_HOST", "10.0.0.2")
    with pytest.raises(ValueError, match="loopback"):
        IbkrEndpointConfig.from_env()


def test_native_client_cannot_submit_before_explicit_enable_and_handshake() -> None:
    config = IbkrEndpointConfig(
        mode="GATEWAY",
        host="127.0.0.1",
        port=4002,
        client_id=37,
        account="DU123",
        paper=True,
        submission_enabled=False,
    )
    client = IbkrSocketClient(config)
    contract, order = map_submit_request(_request(), account=config.account)
    with pytest.raises(PermissionError, match="disabled"):
        client.place_order(contract, order)


class _FakeNativeClient:
    def __init__(self) -> None:
        self.config = SimpleNamespace(account="DU123")
        self.placed = 0
        self.cancelled: list[int] = []

    def refresh_snapshot(self) -> None:
        return

    def snapshot(self) -> dict[str, object]:
        return {
            "sequence": 7,
            "reconciled": True,
            "account": {"BuyingPower": "10000", "NetLiquidation": "25000", "Currency": "USD"},
            "positions": [{"contract_id": "101", "quantity": 2, "average_price": "1.25"}],
            "orders": [
                {
                    "broker_order_id": "901",
                    "contract_id": "999",
                    "quantity": 1,
                    "filled": 0,
                    "status": "Submitted",
                    "side": "BUY",
                    "order_type": "LMT",
                    "submitted_price": "1.00",
                }
            ],
            "fills": [
                {
                    "fill_id": "fill-1",
                    "broker_order_id": "900",
                    "contract_id": "101",
                    "side": "BOT",
                    "quantity": 1,
                    "price": "1.2",
                    "occurred_at_utc": "20260721 10:30:00",
                }
            ],
        }

    def place_order(self, _contract: object, _order: object) -> int:
        self.placed += 1
        return 900

    def cancel_order(self, order_id: int) -> None:
        self.cancelled.append(order_id)


def test_native_backend_projects_full_snapshot_and_idempotent_mutations() -> None:
    client = _FakeNativeClient()
    backend = NativeIbkrBackend(client)  # type: ignore[arg-type]
    submitted = backend.submit(_request())
    repeated = backend.submit(_request())
    assert submitted.broker_order_id == repeated.broker_order_id == "900"
    assert client.placed == 1
    snapshot = backend.snapshot()
    assert snapshot.account.buying_power == "10000"
    assert snapshot.positions[0].average_price == "1.25"
    assert snapshot.fills[0].fill_id == "fill-1"
    assert snapshot.orders[0].plan_hash == "a" * 64
    assert snapshot.orders[1].idempotency_key == "external:901"
    assert snapshot.account.reconciled is False
    backend.cancel("900")
    assert client.cancelled == [900]


class _GrpcBackend:
    def snapshot(self) -> broker_pb2.BrokerSnapshot:
        return broker_pb2.BrokerSnapshot(
            schema_version="1.0",
            snapshot_sequence=3,
            account=broker_pb2.AccountSnapshot(
                broker_id=broker_pb2.BROKER_ID_IBKR,
                health=broker_pb2.BROKER_HEALTH_HEALTHY,
                reconciled=True,
                buying_power="1",
                net_liquidation="1",
                currency="USD",
            ),
        )

    def submit(
        self, request: broker_pb2.SubmitBrokerOrderRequest
    ) -> broker_pb2.BrokerOrderSnapshot:
        return broker_pb2.BrokerOrderSnapshot(
            broker_order_id="1",
            idempotency_key=request.idempotency_key,
            plan_hash=request.plan_hash,
        )

    def cancel(self, broker_order_id: str) -> broker_pb2.BrokerOrderSnapshot:
        return broker_pb2.BrokerOrderSnapshot(broker_order_id=broker_order_id)


def test_ibkr_loopback_grpc_snapshot_and_reconcile_contract() -> None:
    async def scenario() -> None:
        server = grpc.aio.server()
        broker_pb2_grpc.add_BrokerAdapterServiceServicer_to_server(
            IbkrBrokerService(_GrpcBackend()), server
        )
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
        try:
            stub = broker_pb2_grpc.BrokerAdapterServiceStub(channel)
            snapshot = await stub.GetBrokerSnapshot(
                broker_pb2.GetBrokerSnapshotRequest(broker_id=broker_pb2.BROKER_ID_IBKR)
            )
            assert snapshot.snapshot_sequence == 3
            reconciled = await stub.ReconcileBroker(
                broker_pb2.ReconcileBrokerRequest(
                    broker_id=broker_pb2.BROKER_ID_IBKR, expected_snapshot_sequence=3
                )
            )
            assert reconciled.matched is True
        finally:
            await channel.close()
            await server.stop(None)

    asyncio.run(scenario())
