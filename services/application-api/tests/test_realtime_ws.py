"""WebSocket /stream/cockpit + REST /cockpit/state recovery, with a fake stream."""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from app import main
from app.realtime import session


def _tick(minute_et: int, seq: int, health: str = "HEALTHY") -> dict[str, Any]:
    hh, mm = minute_et // 60, minute_et % 60
    uhh = hh + 4
    close = 500.0 + seq * 0.1
    ts_utc = f"2026-07-20T{uhh:02d}:{mm:02d}:00Z"
    ts_et = f"2026-07-20T{hh:02d}:{mm:02d}:00-04:00"
    return {
        "snapshot": {
            "schema_version": "1.0",
            "snapshot_id": f"mkt_{minute_et}_{seq:06d}",
            "occurred_at_utc": ts_utc,
            "timestamp_et": ts_et,
            "symbol": "QQQ.US",
            "price": f"{close:.2f}",
            "open": "498.50",
            "high": f"{close + 1:.2f}",
            "low": "497.90",
            "previous_close": "497.20",
            "vwap": f"{close:.2f}",
            "volume": 1_000_000,
            "premarket_high": None,
            "premarket_low": None,
            "sequence_number": seq,
            "quote_age_ms": 0,
            "data_health": health,
        },
        "bar": {
            "occurred_at_utc": ts_utc,
            "timestamp_et": ts_et,
            "minute_et": minute_et,
            "open": f"{close:.2f}",
            "high": f"{close + 0.5:.2f}",
            "low": f"{close - 0.5:.2f}",
            "close": f"{close:.2f}",
            "volume": 1000 + seq,
            "vwap": f"{close:.2f}",
        },
    }


def _install_fake_stream(monkeypatch: Any, ticks: list[dict[str, Any]]) -> None:
    monkeypatch.setattr(session, "stream_ticks", lambda sid, target=None: iter(list(ticks)))


def test_websocket_pushes_frames_and_closes(monkeypatch: Any) -> None:
    _install_fake_stream(monkeypatch, [_tick(570 + i, i + 1) for i in range(3)])
    client = TestClient(main.app)
    with client.websocket_connect("/api/v1/stream/cockpit?session_id=ws1") as ws:
        received = []
        for _ in range(4):  # 3 ticks + terminal disconnect
            received.append(ws.receive_json())
    assert [f["seq"] for f in received] == [0, 1, 2, 3]
    assert received[0]["connection"] == "LIVE"
    assert received[-1]["connection"] == "DISCONNECTED"


def test_websocket_stale_frame_is_not_tradable(monkeypatch: Any) -> None:
    _install_fake_stream(monkeypatch, [_tick(570, 1, health="STALE")])
    client = TestClient(main.app)
    with client.websocket_connect("/api/v1/stream/cockpit?session_id=ws2") as ws:
        frame = ws.receive_json()
    assert frame["connection"] == "STALE"
    assert frame["new_position_allowed"] is False


def test_rest_recovery_returns_latest_then_fail_closed_default(monkeypatch: Any) -> None:
    _install_fake_stream(monkeypatch, [_tick(570 + i, i + 1) for i in range(2)])
    client = TestClient(main.app)
    # Unknown session: fail-closed default frame.
    resp = client.get("/api/v1/cockpit/state", params={"session_id": "never-seen"})
    assert resp.status_code == 200
    assert resp.json()["new_position_allowed"] is False
    assert resp.json()["connection"] == "DISCONNECTED"
    # After a stream runs, recovery returns the cached latest frame.
    with client.websocket_connect("/api/v1/stream/cockpit?session_id=ws3") as ws:
        for _ in range(3):
            ws.receive_json()
    recovered = client.get("/api/v1/cockpit/state", params={"session_id": "ws3"}).json()
    assert recovered["session_id"] == "ws3"
