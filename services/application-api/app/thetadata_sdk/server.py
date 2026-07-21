"""CLI entrypoint for the internal ThetaData Python SDK gRPC bridge."""

from __future__ import annotations

import asyncio
import os

import grpc

from app.grpc_gen import market_pb2_grpc
from app.thetadata_sdk.service import (
    ThetaDataBarSource,
    ThetaDataOptionSource,
    ThetaDataSdkService,
    create_sdk_client,
)


async def serve() -> None:
    bind = os.getenv("THETADATA_SDK_BIND", "127.0.0.1:50052")
    server = grpc.aio.server()
    client = create_sdk_client()
    market_pb2_grpc.add_ThetaDataSdkServiceServicer_to_server(
        ThetaDataSdkService(ThetaDataBarSource(client), ThetaDataOptionSource(client)), server
    )
    if server.add_insecure_port(bind) == 0:
        raise RuntimeError(f"cannot bind ThetaData SDK bridge to {bind}")
    await server.start()
    print(f"ThetaData SDK bridge listening on {bind}", flush=True)
    try:
        await server.wait_for_termination()
    finally:
        await server.stop(grace=2)


if __name__ == "__main__":
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        pass
