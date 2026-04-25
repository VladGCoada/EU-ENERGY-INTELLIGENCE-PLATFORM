"""Renewable-share feature engineering helpers."""

from __future__ import annotations


def build_renewable_share_features(
    rows: list[dict[str, object]],
    renewable_field: str = "renewable_mw",
    total_field: str = "total_generation_mw",
) -> list[dict[str, object]]:
    """Compute renewable share percentage features."""
    output: list[dict[str, object]] = []
    for row in rows:
        renewable = row.get(renewable_field)
        total = row.get(total_field)
        share = None
        if isinstance(renewable, (int, float)) and isinstance(total, (int, float)) and total:
            share = float(renewable) / float(total) * 100.0
        output.append({**row, "renewable_share_feature_pct": share})
    return output
