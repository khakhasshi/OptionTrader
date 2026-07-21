from app.grpc_gen import broker_pb2, execution_pb2


def test_broker_sidecar_contract_exposes_complete_phase3_boundary() -> None:
    service = broker_pb2.DESCRIPTOR.services_by_name["BrokerAdapterService"]
    assert [method.name for method in service.methods] == [
        "GetBrokerSnapshot",
        "SubmitBrokerOrder",
        "CancelBrokerOrder",
        "RecoverBrokerOrder",
        "ReconcileBroker",
    ]


def test_duplicated_boundary_enums_remain_wire_compatible() -> None:
    assert int(broker_pb2.BROKER_ID_LONGBRIDGE) == int(execution_pb2.BROKER_ID_LONGBRIDGE)
    assert int(broker_pb2.BROKER_ID_IBKR) == int(execution_pb2.BROKER_ID_IBKR)
    assert int(broker_pb2.ORDER_SIDE_BUY) == int(execution_pb2.ORDER_SIDE_BUY)
    assert int(broker_pb2.ORDER_SIDE_SELL) == int(execution_pb2.ORDER_SIDE_SELL)
    assert int(broker_pb2.BROKER_ORDER_TYPE_MARKET) == int(execution_pb2.BROKER_ORDER_TYPE_MARKET)
    assert int(broker_pb2.BROKER_ORDER_TYPE_LIMIT) == int(execution_pb2.BROKER_ORDER_TYPE_LIMIT)
    assert int(broker_pb2.BROKER_ORDER_TYPE_ADAPTIVE_LIMIT) == int(
        execution_pb2.BROKER_ORDER_TYPE_ADAPTIVE_LIMIT
    )


def test_broker_order_round_trip_preserves_all_combo_legs() -> None:
    request = broker_pb2.SubmitBrokerOrderRequest(
        broker_id=broker_pb2.BROKER_ID_IBKR,
        idempotency_key="submit_abc",
        plan_hash="a" * 64,
        total_quantity=2,
        submitted_price="1.25",
        side=broker_pb2.ORDER_SIDE_SELL,
        order_type=broker_pb2.BROKER_ORDER_TYPE_ADAPTIVE_LIMIT,
        adaptive_priority=broker_pb2.ADAPTIVE_PRIORITY_NORMAL,
        legs=[
            broker_pb2.BrokerOrderLeg(
                contract_id="QQQ-20260721-C-500",
                side=broker_pb2.ORDER_SIDE_SELL,
                quantity=2,
                broker_contract_id="101",
                symbol="QQQ",
                exchange="SMART",
                submitted_price="1.50",
            ),
            broker_pb2.BrokerOrderLeg(
                contract_id="QQQ-20260721-C-501",
                side=broker_pb2.ORDER_SIDE_BUY,
                quantity=2,
                broker_contract_id="102",
                symbol="QQQ",
                exchange="SMART",
                submitted_price="0.50",
            ),
        ],
    )
    restored = broker_pb2.SubmitBrokerOrderRequest.FromString(request.SerializeToString())
    assert restored == request
    assert [leg.contract_id for leg in restored.legs] == [
        "QQQ-20260721-C-500",
        "QQQ-20260721-C-501",
    ]
    assert [leg.submitted_price for leg in restored.legs] == ["1.50", "0.50"]

    recovery = broker_pb2.RecoverBrokerOrderRequest(
        expected_order=request, expected_broker_order_id="900"
    )
    assert broker_pb2.RecoverBrokerOrderRequest.FromString(recovery.SerializeToString()) == recovery
