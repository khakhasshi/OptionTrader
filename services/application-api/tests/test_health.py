from fastapi.testclient import TestClient
import pytest
from cryptography.fernet import Fernet

from app.main import _confirmation_cipher, app

client = TestClient(app)


def test_health_ok() -> None:
    resp = client.get("/api/v1/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["service"] == "application-api"
    assert "environment" in body


def test_confirmation_cipher_accepts_a_fernet_key() -> None:
    key = Fernet.generate_key().decode("ascii")
    cipher = _confirmation_cipher(key)
    assert cipher.decrypt(cipher.encrypt("opaque-capability")) == "opaque-capability"


def test_confirmation_cipher_supports_key_rotation_ring() -> None:
    old_key = Fernet.generate_key().decode("ascii")
    new_key = Fernet.generate_key().decode("ascii")
    old_cipher = _confirmation_cipher(old_key)
    rotated = _confirmation_cipher(f"{new_key},{old_key}")
    ciphertext = old_cipher.encrypt("legacy-capability")
    assert rotated.decrypt(ciphertext) == "legacy-capability"
    assert _confirmation_cipher(new_key).decrypt(rotated.encrypt("fresh-capability")) == (
        "fresh-capability"
    )
    with pytest.raises(ValueError, match="cannot be decrypted"):
        old_cipher.decrypt(rotated.encrypt("fresh-capability"))


@pytest.mark.parametrize("key", ["", "not-a-fernet-key", "非 ASCII"])
def test_confirmation_cipher_rejects_invalid_keys(key: str) -> None:
    with pytest.raises(ValueError, match="key is invalid"):
        _confirmation_cipher(key)
