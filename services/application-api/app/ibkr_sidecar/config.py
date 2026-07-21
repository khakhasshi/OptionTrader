"""Strict local endpoint configuration for TWS and IB Gateway."""

from __future__ import annotations

from dataclasses import dataclass
import os
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def _boolean(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    if raw == "true":
        return True
    if raw == "false":
        return False
    raise ValueError(f"{name} must be exactly true or false")


@dataclass(frozen=True)
class IbkrEndpointConfig:
    mode: str
    host: str
    port: int
    client_id: int
    account: str
    paper: bool
    submission_enabled: bool
    timezone: str = "America/New_York"
    connect_timeout_seconds: float = 10.0

    @classmethod
    def from_env(cls) -> IbkrEndpointConfig:
        mode = os.getenv("OPTIONTRADER_IBKR_MODE", "GATEWAY").upper()
        if mode not in {"TWS", "GATEWAY"}:
            raise ValueError("OPTIONTRADER_IBKR_MODE must be TWS or GATEWAY")
        paper = _boolean("OPTIONTRADER_IBKR_PAPER", True)
        default_port = (
            7497 if mode == "TWS" and paper else 7496 if mode == "TWS" else 4002 if paper else 4001
        )
        host = os.getenv("OPTIONTRADER_IBKR_HOST", "127.0.0.1")
        if host not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("IBKR sidecar may connect only to a loopback TWS/Gateway")
        account = os.getenv("OPTIONTRADER_IBKR_ACCOUNT", "").strip()
        if not account:
            raise ValueError("OPTIONTRADER_IBKR_ACCOUNT is required")
        port = int(os.getenv("OPTIONTRADER_IBKR_PORT", str(default_port)))
        client_id = int(os.getenv("OPTIONTRADER_IBKR_CLIENT_ID", "37"))
        timezone = os.getenv("OPTIONTRADER_IBKR_TIMEZONE", "America/New_York")
        try:
            ZoneInfo(timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("OPTIONTRADER_IBKR_TIMEZONE is invalid") from exc
        if not 1 <= port <= 65_535 or not 1 <= client_id <= 2_147_483_647:
            raise ValueError("IBKR port or client id is outside its valid range")
        return cls(
            mode=mode,
            host=host,
            port=port,
            client_id=client_id,
            account=account,
            paper=paper,
            submission_enabled=_boolean("OPTIONTRADER_IBKR_SUBMISSION_ENABLED", False),
            timezone=timezone,
        )
