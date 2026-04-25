"""Volatility feature engineering helpers."""

from __future__ import annotations


def build_volatility_features(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Create a minimal volatility feature from high/low generation values."""
    output: list[dict[str, object]] = []
    for row in rows:
        max_generation = row.get("max_generation")
        min_generation = row.get("min_generation")
        volatility = None
        if isinstance(max_generation, (int, float)) and isinstance(min_generation, (int, float)):
            volatility = float(max_generation) - float(min_generation)
        output.append({**row, "volatility_feature": volatility})
    return output
