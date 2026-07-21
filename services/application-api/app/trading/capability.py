"""Authenticated encryption for short-lived confirmation capabilities."""

from __future__ import annotations

from cryptography.fernet import Fernet, InvalidToken


class ConfirmationCipher:
    """Encrypt capabilities before they enter the shared PostgreSQL store."""

    def __init__(self, key: str) -> None:
        try:
            self._fernet = Fernet(key.encode("ascii"))
        except (ValueError, UnicodeEncodeError) as exc:
            raise ValueError("confirmation encryption key is invalid") from exc

    def encrypt(self, token: str) -> str:
        if not token:
            raise ValueError("confirmation token is empty")
        return self._fernet.encrypt(token.encode("utf-8")).decode("ascii")

    def decrypt(self, ciphertext: str) -> str:
        try:
            return self._fernet.decrypt(ciphertext.encode("ascii")).decode("utf-8")
        except (InvalidToken, UnicodeDecodeError, UnicodeEncodeError) as exc:
            raise ValueError("confirmation capability cannot be decrypted") from exc


__all__ = ["ConfirmationCipher"]
