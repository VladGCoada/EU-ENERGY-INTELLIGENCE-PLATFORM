"""Freshness checks for dataset timestamps."""

from __future__ import annotations

from datetime import UTC, datetime


def is_dataset_fresh(latest_timestamp_iso: str, max_age_minutes: int) -> bool:
    """Return whether a dataset timestamp falls within the freshness window."""
    latest = datetime.fromisoformat(latest_timestamp_iso.replace("Z", "+00:00")).astimezone(UTC)
    age_minutes = (datetime.now(UTC) - latest).total_seconds() / 60
    return age_minutes <= max_age_minutes
