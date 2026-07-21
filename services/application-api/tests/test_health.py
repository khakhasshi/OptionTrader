from fastapi.testclient import TestClient
import pytest

from app.main import _validate_single_worker_configuration, app

client = TestClient(app)


def test_health_ok() -> None:
    resp = client.get("/api/v1/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["service"] == "application-api"
    assert "environment" in body


@pytest.mark.parametrize(
    "environment",
    [
        {"OPTIONTRADER_API_WORKERS": "2"},
        {"WEB_CONCURRENCY": "4"},
        {"UVICORN_WORKERS": "many"},
    ],
)
def test_execution_api_rejects_multi_worker_configuration(environment: dict[str, str]) -> None:
    with pytest.raises(RuntimeError, match="required|integer"):
        _validate_single_worker_configuration(environment)


def test_execution_api_accepts_explicit_single_worker() -> None:
    _validate_single_worker_configuration({"OPTIONTRADER_API_WORKERS": "1", "WEB_CONCURRENCY": "1"})
