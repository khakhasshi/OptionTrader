"""Executable loopback IBKR BrokerAdapterService sidecar."""

from __future__ import annotations

import asyncio
import os

import grpc

from app.grpc_gen import broker_pb2_grpc
from app.ibkr_sidecar.config import IbkrEndpointConfig
from app.ibkr_sidecar.native import IbkrSocketClient
from app.ibkr_sidecar.service import IbkrBrokerService, NativeIbkrBackend


async def serve() -> None:
    bind = os.getenv("OPTIONTRADER_IBKR_SIDECAR_BIND", "127.0.0.1:50053")
    if not bind.startswith(("127.0.0.1:", "localhost:", "[::1]:")):
        raise ValueError("IBKR sidecar must bind to loopback")
    client = IbkrSocketClient(IbkrEndpointConfig.from_env())
    await asyncio.to_thread(client.connect)
    server = grpc.aio.server()
    broker_pb2_grpc.add_BrokerAdapterServiceServicer_to_server(
        IbkrBrokerService(NativeIbkrBackend(client)), server
    )
    server.add_insecure_port(bind)
    await server.start()
    try:
        await server.wait_for_termination()
    finally:
        client.disconnect()


if __name__ == "__main__":
    asyncio.run(serve())
