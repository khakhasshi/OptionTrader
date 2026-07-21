"""Validated provider configuration with secret-safe local dotenv loading."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
import os
from pathlib import Path
import re
import stat
from typing import Mapping
from urllib.parse import urlsplit


_PROVIDER_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,78}[a-z0-9]$")
_MODEL_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,118}[A-Za-z0-9]$")
_LLM_ENV_KEYS = {
    "LLM_PROVIDER",
    "LLM_BASE_URL",
    "LLM_API_KEY",
    "LLM_MODEL",
    "LLM_TIMEOUT_SECONDS",
    "LLM_MAX_RETRIES",
    "LLM_MAX_INPUT_CHARS",
    "LLM_MAX_OUTPUT_TOKENS",
    "LLM_CACHE_TTL_SECONDS",
    "LLM_DAILY_MAX_REQUESTS",
    "LLM_DAILY_MAX_ESTIMATED_USD",
    "LLM_INPUT_COST_PER_MILLION_USD",
    "LLM_OUTPUT_COST_PER_MILLION_USD",
    "LLM_MAX_CONCURRENCY",
}


@dataclass(frozen=True)
class LLMSettings:
    provider: str
    base_url: str
    api_key: str = field(repr=False)
    model: str
    timeout_seconds: float
    max_retries: int
    max_input_chars: int
    max_output_tokens: int
    cache_ttl_seconds: int
    daily_max_requests: int
    daily_max_estimated_usd: Decimal
    input_cost_per_million_usd: Decimal
    output_cost_per_million_usd: Decimal
    max_concurrency: int
    configuration_error: str | None = None

    @property
    def configured(self) -> bool:
        return self.configuration_error is None

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> LLMSettings:
        source = os.environ if env is None else env
        errors: list[str] = []
        provider = source.get("LLM_PROVIDER", "").strip()
        base_url = source.get("LLM_BASE_URL", "").strip().rstrip("/")
        api_key = source.get("LLM_API_KEY", "").strip()
        model = source.get("LLM_MODEL", "").strip()

        if not _PROVIDER_PATTERN.fullmatch(provider):
            errors.append("provider")
        if not _valid_base_url(base_url):
            errors.append("base_url")
        if len(api_key) < 8:
            errors.append("api_key")
        if not _MODEL_PATTERN.fullmatch(model):
            errors.append("model")

        timeout = _float_setting(source, "LLM_TIMEOUT_SECONDS", 8.0, 1.0, 30.0, errors)
        retries = _int_setting(source, "LLM_MAX_RETRIES", 2, 0, 2, errors)
        max_input = _int_setting(source, "LLM_MAX_INPUT_CHARS", 60_000, 1_000, 200_000, errors)
        max_output = _int_setting(source, "LLM_MAX_OUTPUT_TOKENS", 1_200, 128, 4_096, errors)
        cache_ttl = _int_setting(source, "LLM_CACHE_TTL_SECONDS", 300, 0, 3_600, errors)
        max_requests = _int_setting(source, "LLM_DAILY_MAX_REQUESTS", 100, 1, 1_000, errors)
        max_concurrency = _int_setting(source, "LLM_MAX_CONCURRENCY", 2, 1, 8, errors)
        daily_budget = _decimal_setting(
            source, "LLM_DAILY_MAX_ESTIMATED_USD", Decimal("1.00"), Decimal("0.01"), errors
        )
        input_cost = _decimal_setting(
            source, "LLM_INPUT_COST_PER_MILLION_USD", Decimal("0.14"), Decimal("0"), errors
        )
        output_cost = _decimal_setting(
            source, "LLM_OUTPUT_COST_PER_MILLION_USD", Decimal("0.28"), Decimal("0"), errors
        )
        return cls(
            provider=provider or "unconfigured",
            base_url=base_url or "https://invalid.local",
            api_key=api_key,
            model=model or "unconfigured",
            timeout_seconds=timeout,
            max_retries=retries,
            max_input_chars=max_input,
            max_output_tokens=max_output,
            cache_ttl_seconds=cache_ttl,
            daily_max_requests=max_requests,
            daily_max_estimated_usd=daily_budget,
            input_cost_per_million_usd=input_cost,
            output_cost_per_million_usd=output_cost,
            max_concurrency=max_concurrency,
            configuration_error=("invalid_or_missing:" + ",".join(sorted(set(errors))))
            if errors
            else None,
        )


def _valid_base_url(value: str) -> bool:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return False
    if parsed.scheme != "https" or not parsed.hostname:
        return False
    return (
        parsed.username is None
        and parsed.password is None
        and not parsed.query
        and not parsed.fragment
    )


def _int_setting(
    env: Mapping[str, str], key: str, default: int, minimum: int, maximum: int, errors: list[str]
) -> int:
    raw = env.get(key, str(default))
    try:
        value = int(raw)
    except ValueError:
        errors.append(key.lower())
        return default
    if not minimum <= value <= maximum:
        errors.append(key.lower())
        return default
    return value


def _float_setting(
    env: Mapping[str, str],
    key: str,
    default: float,
    minimum: float,
    maximum: float,
    errors: list[str],
) -> float:
    raw = env.get(key, str(default))
    try:
        value = float(raw)
    except ValueError:
        errors.append(key.lower())
        return default
    if not minimum <= value <= maximum:
        errors.append(key.lower())
        return default
    return value


def _decimal_setting(
    env: Mapping[str, str], key: str, default: Decimal, minimum: Decimal, errors: list[str]
) -> Decimal:
    raw = env.get(key, str(default))
    try:
        value = Decimal(raw)
    except InvalidOperation:
        errors.append(key.lower())
        return default
    if not value.is_finite() or value < minimum:
        errors.append(key.lower())
        return default
    return value


def load_llm_env_file(path: Path) -> dict[str, str]:
    """Parse only allowlisted LLM keys without evaluating shell syntax.

    This helper exists for explicit live-smoke tests. It refuses group/world
    readable files and never mutates ``os.environ``.
    """
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode != 0o600:
        raise ValueError("LLM dotenv file must have mode 0600")
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in _LLM_ENV_KEYS:
            continue
        if key in values:
            raise ValueError(f"duplicate LLM dotenv key at line {line_number}")
        if any(character in value for character in ("\x00", "\r", "\n")):
            raise ValueError(f"invalid LLM dotenv value at line {line_number}")
        values[key] = value.strip()
    return values


__all__ = ["LLMSettings", "load_llm_env_file"]
