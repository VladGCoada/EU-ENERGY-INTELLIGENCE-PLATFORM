"""Quarantine helpers for Silver data quality workflows."""

from __future__ import annotations


def quarantine_failed_rows(
    rows: list[dict[str, object]],
    reason: str,
) -> list[dict[str, object]]:
    """Annotate quarantined rows with a failure reason."""
    return [{**row, "_quarantine_reason": reason} for row in rows]
