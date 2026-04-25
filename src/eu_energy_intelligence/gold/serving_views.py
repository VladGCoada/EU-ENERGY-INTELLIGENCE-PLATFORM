"""Serving-view helpers backed by the Platinum mart."""

from __future__ import annotations

from eu_energy_intelligence.platinum import build_market_summary


def build_serving_market_view(
    gold_rows: list[dict[str, object]],
    carbon_rows: list[dict[str, object]] | None = None,
    weather_rows: list[dict[str, object]] | None = None,
    fx_rows: list[dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    """Expose the Platinum market summary as a serving view builder."""
    return build_market_summary(gold_rows, carbon_rows, weather_rows, fx_rows)
