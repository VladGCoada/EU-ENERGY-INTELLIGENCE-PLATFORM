"""Deduplication helpers."""

from __future__ import annotations


def deduplicate_rows(rows: list[dict[str, object]], key_fields: list[str]) -> list[dict[str, object]]:
    """Deduplicate rows using a tuple of selected key fields."""
    seen: set[tuple[object, ...]] = set()
    deduplicated: list[dict[str, object]] = []
    for row in rows:
        key = tuple(row.get(field) for field in key_fields)
        if key in seen:
            continue
        seen.add(key)
        deduplicated.append(row)
    return deduplicated
