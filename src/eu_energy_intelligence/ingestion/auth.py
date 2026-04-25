"""Authentication helpers for external data providers."""

from __future__ import annotations

import os


def get_entsoe_api_key(default: str | None = None) -> str | None:
    """Read the ENTSO-E API token from the environment."""
    return os.getenv("ENTSOE_API_KEY", default)


def require_entsoe_api_key() -> str:
    """Return the ENTSO-E API token or raise when it is missing."""
    api_key = get_entsoe_api_key()
    if not api_key:
        raise ValueError("ENTSOE_API_KEY not set in environment or .env")
    return api_key
