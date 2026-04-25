from __future__ import annotations

from typing import Any


def build_market_summary(
    gold_rows: list[dict[str, Any]],
    carbon_rows: list[dict[str, Any]] | None = None,
    weather_rows: list[dict[str, Any]] | None = None,
    fx_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Join gold metrics with exogenous drivers into a serving-friendly mart."""
    carbon_by_zone = _average_by_key(carbon_rows or [], "zone", "carbon_intensity_gco2_kwh")
    wind_by_zone = _average_by_key(weather_rows or [], "zone", "wind_speed_m_s")
    solar_by_zone = _average_by_key(weather_rows or [], "zone", "solar_radiation_w_m2")
    fx_rate = _latest_fx_rate(fx_rows or [])

    summary_rows: list[dict[str, Any]] = []
    for row in gold_rows:
        country_code = str(row["country_code"])
        total_generation = float(row.get("total_generation", 0.0) or 0.0)
        volatility = float(row.get("volatility_index", 0.0) or 0.0)
        carbon_intensity = carbon_by_zone.get(country_code)
        summary_rows.append(
            {
                **row,
                "carbon_intensity_gco2_kwh": carbon_intensity,
                "wind_speed_m_s_avg": wind_by_zone.get(country_code),
                "solar_radiation_w_m2_avg": solar_by_zone.get(country_code),
                "fx_rate_to_eur": fx_rate,
                "stability_score": round(max(0.0, total_generation - volatility), 3),
                "carbon_efficiency_score": _carbon_efficiency(total_generation, carbon_intensity),
            }
        )
    return summary_rows


def _average_by_key(rows: list[dict[str, Any]], key: str, value_field: str) -> dict[str, float]:
    grouped: dict[str, list[float]] = {}
    for row in rows:
        grouping_key = row.get(key)
        value = row.get(value_field)
        if grouping_key is None or not isinstance(value, (int, float)):
            continue
        grouped.setdefault(str(grouping_key), []).append(float(value))
    return {group: sum(values) / len(values) for group, values in grouped.items()}


def _latest_fx_rate(rows: list[dict[str, Any]]) -> float | None:
    if not rows:
        return None
    latest_row = sorted(rows, key=lambda row: str(row.get("timestamp", "")))[-1]
    value = latest_row.get("fx_rate")
    return float(value) if isinstance(value, (int, float)) else None


def _carbon_efficiency(total_generation: float, carbon_intensity: float | None) -> float | None:
    if carbon_intensity is None:
        return None
    return round(total_generation / max(carbon_intensity, 1.0), 6)
