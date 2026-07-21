"""Fail-closed broker.proto to IBKR Contract / Order specifications."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from app.grpc_gen import broker_pb2


@dataclass(frozen=True)
class IbkrComboLegSpec:
    con_id: int
    ratio: int
    action: str
    exchange: str


@dataclass(frozen=True)
class IbkrContractSpec:
    sec_type: str
    symbol: str
    currency: str
    exchange: str
    con_id: int | None
    combo_legs: tuple[IbkrComboLegSpec, ...]


@dataclass(frozen=True)
class IbkrOrderSpec:
    action: str
    order_type: str
    quantity: int
    limit_price: Decimal | None
    tif: str
    account: str
    order_ref: str
    adaptive_priority: str | None


def _price(raw: str) -> Decimal:
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise ValueError("submitted_price must be a decimal") from exc
    if not value.is_finite() or value <= 0:
        raise ValueError("submitted_price must be positive")
    return value


def _action(value: int) -> str:
    if value == broker_pb2.ORDER_SIDE_BUY:
        return "BUY"
    if value == broker_pb2.ORDER_SIDE_SELL:
        return "SELL"
    raise ValueError("order side is unspecified")


def _inverse(action: str) -> str:
    return "SELL" if action == "BUY" else "BUY"


def map_submit_request(
    request: broker_pb2.SubmitBrokerOrderRequest, *, account: str
) -> tuple[IbkrContractSpec, IbkrOrderSpec]:
    if request.broker_id != broker_pb2.BROKER_ID_IBKR:
        raise ValueError("request is not routed to IBKR")
    if not account or not request.idempotency_key or len(request.plan_hash) != 64:
        raise ValueError("account and idempotency identity are required")
    if request.total_quantity < 1 or not 1 <= len(request.legs) <= 4:
        raise ValueError("IBKR order requires one to four equal-ratio legs")
    parent_action = _action(request.side)
    symbols = {leg.symbol for leg in request.legs if leg.symbol}
    if len(symbols) != 1:
        raise ValueError("all IBKR combo legs must share one underlying symbol")
    symbol = next(iter(symbols))
    combo_legs: list[IbkrComboLegSpec] = []
    for leg in request.legs:
        if leg.quantity != request.total_quantity:
            raise ValueError("leg quantity must equal combo-unit quantity")
        try:
            con_id = int(leg.broker_contract_id)
        except ValueError as exc:
            raise ValueError("every IBKR leg requires a numeric conId") from exc
        if con_id <= 0:
            raise ValueError("every IBKR leg requires a positive conId")
        intended = _action(leg.side)
        # A SELL parent reverses the canonical BAG definition. Normalize the
        # combo leg so the resulting execution still matches intended sides.
        canonical = intended if parent_action == "BUY" else _inverse(intended)
        combo_legs.append(
            IbkrComboLegSpec(
                con_id=con_id,
                ratio=1,
                action=canonical,
                exchange=leg.exchange or "SMART",
            )
        )

    if request.order_type == broker_pb2.BROKER_ORDER_TYPE_MARKET:
        if (
            request.submitted_price
            or request.adaptive_priority != broker_pb2.ADAPTIVE_PRIORITY_UNSPECIFIED
        ):
            raise ValueError("market order must not carry limit pricing")
        order_type = "MKT"
        limit_price = None
        priority = None
    elif request.order_type == broker_pb2.BROKER_ORDER_TYPE_LIMIT:
        order_type = "LMT"
        limit_price = _price(request.submitted_price)
        priority = None
        if request.adaptive_priority != broker_pb2.ADAPTIVE_PRIORITY_UNSPECIFIED:
            raise ValueError("plain limit order must not carry adaptive priority")
    elif request.order_type == broker_pb2.BROKER_ORDER_TYPE_ADAPTIVE_LIMIT:
        order_type = "LMT"
        limit_price = _price(request.submitted_price)
        priorities = {
            broker_pb2.ADAPTIVE_PRIORITY_PASSIVE: "Patient",
            broker_pb2.ADAPTIVE_PRIORITY_NORMAL: "Normal",
            broker_pb2.ADAPTIVE_PRIORITY_URGENT: "Urgent",
        }
        try:
            priority = priorities[request.adaptive_priority]
        except KeyError as exc:
            raise ValueError("adaptive limit requires an explicit priority") from exc
    else:
        raise ValueError("broker order type is unspecified")

    contract = IbkrContractSpec(
        sec_type="OPT" if len(combo_legs) == 1 else "BAG",
        symbol=symbol,
        currency="USD",
        exchange=request.legs[0].exchange or "SMART",
        con_id=combo_legs[0].con_id if len(combo_legs) == 1 else None,
        combo_legs=tuple() if len(combo_legs) == 1 else tuple(combo_legs),
    )
    order = IbkrOrderSpec(
        action=parent_action,
        order_type=order_type,
        quantity=request.total_quantity,
        limit_price=limit_price,
        tif="DAY",
        account=account,
        order_ref=f"ot:{request.plan_hash[:24]}",
        adaptive_priority=priority,
    )
    return contract, order
