"""Unit standardization helpers."""

from __future__ import annotations


def ensure_mw(
    rows: list[dict[str, object]],
    value_field: str,
    scale_factor: float = 1.0,
) -> list[dict[str, object]]:
    """Normalize numeric fields into MW-scale values."""
    normalized: list[dict[str, object]] = []
    for row in rows:
        value = row.get(value_field)
        if isinstance(value, (int, float)):
            normalized.append({**row, value_field: float(value) * scale_factor})
        else:
            normalized.append(dict(row))
    return normalized
