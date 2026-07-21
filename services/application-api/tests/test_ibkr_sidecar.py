from __future__ import annotations

from app.grpc_gen import broker_pb2
from app.ibkr_sidecar.config import IbkrEndpointConfig
from app.ibkr_sidecar.mapping import map_submit_request
from app.ibkr_sidecar.native import IbkrSocketClient
import pytest


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
