"""Timezone alignment helpers for Silver records."""

from __future__ import annotations

from datetime import UTC, datetime


def normalize_timestamp_to_utc(timestamp_value: str) -> str:
    """Convert ISO-like timestamps to normalized UTC strings when possible."""
    parsed = datetime.fromisoformat(timestamp_value.replace("Z", "+00:00"))
    return parsed.astimezone(UTC).isoformat()
