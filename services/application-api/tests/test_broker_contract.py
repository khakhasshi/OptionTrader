from app.grpc_gen import broker_pb2, execution_pb2


def test_broker_sidecar_contract_exposes_complete_phase3_boundary() -> None:
    service = broker_pb2.DESCRIPTOR.services_by_name["BrokerAdapterService"]
    assert [method.name for method in service.methods] == [
        "GetBrokerSnapshot",
        "SubmitBrokerOrder",
        "CancelBrokerOrder",
        "ReconcileBroker",
    ]


def test_duplicated_boundary_enums_remain_wire_compatible() -> None:
    assert int(broker_pb2.BROKER_ID_LONGBRIDGE) == int(execution_pb2.BROKER_ID_LONGBRIDGE)
    assert int(broker_pb2.BROKER_ID_IBKR) == int(execution_pb2.BROKER_ID_IBKR)
    assert int(broker_pb2.ORDER_SIDE_BUY) == int(execution_pb2.ORDER_SIDE_BUY)
    assert int(broker_pb2.ORDER_SIDE_SELL) == int(execution_pb2.ORDER_SIDE_SELL)


def test_broker_order_round_trip_preserves_all_combo_legs() -> None:
    request = broker_pb2.SubmitBrokerOrderRequest(
        broker_id=broker_pb2.BROKER_ID_IBKR,
        idempotency_key="submit_abc",
        plan_hash="a" * 64,
        total_quantity=2,
        limit_price="1.25",
        legs=[
            broker_pb2.BrokerOrderLeg(
                contract_id="QQQ-20260721-C-500",
                side=broker_pb2.ORDER_SIDE_SELL,
                quantity=2,
            ),
            broker_pb2.BrokerOrderLeg(
                contract_id="QQQ-20260721-C-501",
                side=broker_pb2.ORDER_SIDE_BUY,
                quantity=2,
            ),
        ],
    )
    restored = broker_pb2.SubmitBrokerOrderRequest.FromString(request.SerializeToString())
    assert restored == request
    assert [leg.contract_id for leg in restored.legs] == [
        "QQQ-20260721-C-500",
        "QQQ-20260721-C-501",
    ]
