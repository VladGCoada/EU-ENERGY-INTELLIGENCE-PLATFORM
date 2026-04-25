"""Price spike analytics."""

from __future__ import annotations


def build_price_spike_analysis(
    rows: list[dict[str, object]],
    spike_threshold: float = 200.0,
) -> list[dict[str, object]]:
    """Annotate prices that exceed a configurable spike threshold."""
    output: list[dict[str, object]] = []
    for row in rows:
        value = row.get("price_eur_mwh")
        is_spike = isinstance(value, (int, float)) and float(value) >= spike_threshold
        output.append({**row, "is_price_spike": is_spike, "spike_threshold": spike_threshold})
    return output
