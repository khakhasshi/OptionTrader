"""Authenticated encryption for short-lived confirmation capabilities."""

from __future__ import annotations

from cryptography.fernet import Fernet, InvalidToken, MultiFernet


class ConfirmationCipher:
    """Encrypt capabilities before they enter the shared PostgreSQL store."""

    def __init__(self, key: str) -> None:
        keys = [item.strip() for item in key.split(",") if item.strip()]
        if not keys:
            raise ValueError("confirmation encryption key is invalid")
        try:
            fernets = [Fernet(item.encode("ascii")) for item in keys]
        except (ValueError, UnicodeEncodeError) as exc:
            raise ValueError("confirmation encryption key is invalid") from exc
        self._primary = fernets[0]
        self._fernet = MultiFernet(fernets)
        self._key_count = len(fernets)

    @property
    def key_count(self) -> int:
        return self._key_count

    def encrypt(self, token: str) -> str:
        if not token:
            raise ValueError("confirmation token is empty")
        return self._fernet.encrypt(token.encode("utf-8")).decode("ascii")

    def decrypt(self, ciphertext: str) -> str:
        try:
            return self._fernet.decrypt(ciphertext.encode("ascii")).decode("utf-8")
        except (InvalidToken, UnicodeDecodeError, UnicodeEncodeError) as exc:
            raise ValueError("confirmation capability cannot be decrypted") from exc

    def rotate(self, ciphertext: str) -> str:
        """Re-encrypt with the primary key while preserving the Fernet timestamp."""
        try:
            return self._fernet.rotate(ciphertext.encode("ascii")).decode("ascii")
        except (InvalidToken, UnicodeDecodeError, UnicodeEncodeError) as exc:
            raise ValueError("confirmation capability cannot be rotated") from exc

    def requires_rotation(self, ciphertext: str) -> bool:
        """Return whether a valid ciphertext was encrypted by a non-primary key."""
        try:
            encoded = ciphertext.encode("ascii")
            self._primary.decrypt(encoded).decode("utf-8")
            return False
        except InvalidToken:
            try:
                self._fernet.decrypt(encoded).decode("utf-8")
            except (InvalidToken, UnicodeDecodeError) as exc:
                raise ValueError("confirmation capability cannot be rotated") from exc
            return True
        except (UnicodeDecodeError, UnicodeEncodeError) as exc:
            raise ValueError("confirmation capability cannot be rotated") from exc


__all__ = ["ConfirmationCipher"]
