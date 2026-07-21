"""Trusted ThetaData option quote/Greeks acquisition for candidate construction."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256

import grpc

from app.grpc_gen import market_pb2, market_pb2_grpc
from app.trading.candidate import QuotedLeg


@dataclass(frozen=True)
class OptionContractSelection:
    side: str
    option_right: str
    contract_id: str
    expiry: str
    strike: str
    broker_contract_id: str
    exchange: str | None = None
    symbol: str = "QQQ"


def fetch_quoted_legs(
    selections: tuple[OptionContractSelection, ...], *, target: str | None = None
) -> tuple[str, tuple[QuotedLeg, ...]]:
    """Fetch one atomic proof batch; no broker quote may enter the trade plan."""
    if not 1 <= len(selections) <= 4 or len({item.contract_id for item in selections}) != len(
        selections
    ):
        raise ValueError("option selections must contain one to four unique contracts")
    rights = {"CALL": market_pb2.THETA_OPTION_RIGHT_CALL, "PUT": market_pb2.THETA_OPTION_RIGHT_PUT}
    try:
        contracts = [
            market_pb2.ThetaOptionContractRequest(
                contract_id=item.contract_id,
                symbol=item.symbol,
                expiration=item.expiry,
                strike=item.strike,
                right=rights[item.option_right],
            )
            for item in selections
        ]
    except KeyError as exc:
        raise ValueError("option right must be CALL or PUT") from exc
    channel = grpc.insecure_channel(target or os.getenv("THETADATA_SDK_GRPC", "localhost:50052"))
    try:
        batch = market_pb2_grpc.ThetaDataSdkServiceStub(channel).GetOptionSnapshots(
            market_pb2.ThetaOptionSnapshotRequest(contracts=contracts), timeout=3
        )
    finally:
        channel.close()
    if batch.provider != "THETADATA" or len(batch.snapshots) != len(selections):
        raise ValueError("ThetaData option batch is incomplete")
    digest = sha256()
    for snapshot in batch.snapshots:
        digest.update(snapshot.SerializeToString(deterministic=True))
    if batch.chain_snapshot_id != f"thetaopt_{digest.hexdigest()}":
        raise ValueError("ThetaData option batch identity is invalid")

    quoted: list[QuotedLeg] = []
    for selection, snapshot in zip(selections, batch.snapshots, strict=True):
        expected_right = rights[selection.option_right]
        if (
            snapshot.contract_id != selection.contract_id
            or snapshot.symbol != selection.symbol
            or snapshot.expiration != selection.expiry
            or snapshot.strike != selection.strike
            or snapshot.right != expected_right
            or snapshot.provider != "THETADATA"
        ):
            raise ValueError("ThetaData option response changed the requested contract")
        occurred_at = datetime.fromisoformat(snapshot.occurred_at_utc.replace("Z", "+00:00"))
        if occurred_at.tzinfo is None:
            raise ValueError("ThetaData option timestamp is not timezone-aware")
        quoted.append(
            QuotedLeg(
                side=selection.side,
                option_right=selection.option_right,
                contract_id=selection.contract_id,
                expiry=selection.expiry,
                strike=selection.strike,
                bid=snapshot.bid,
                ask=snapshot.ask,
                bid_size=snapshot.bid_size,
                ask_size=snapshot.ask_size,
                quote_at_utc=occurred_at.astimezone(UTC),
                delta=snapshot.delta,
                gamma=snapshot.gamma,
                theta=snapshot.theta,
                vega=snapshot.vega,
                chain_snapshot_id=batch.chain_snapshot_id,
                broker_contract_id=selection.broker_contract_id,
                symbol=selection.symbol,
                exchange=selection.exchange,
                quote_provider="THETADATA",
            )
        )
    return batch.chain_snapshot_id, tuple(quoted)


__all__ = ["OptionContractSelection", "fetch_quoted_legs"]
