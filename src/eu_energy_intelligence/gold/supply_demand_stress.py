"""Supply-demand stress analytics."""

from __future__ import annotations


def build_supply_demand_stress(
    rows: list[dict[str, object]],
    supply_field: str = "supply_mw",
    demand_field: str = "demand_mw",
) -> list[dict[str, object]]:
    """Compute simple stress deltas between supply and demand."""
    output: list[dict[str, object]] = []
    for row in rows:
        supply = row.get(supply_field)
        demand = row.get(demand_field)
        stress_delta = None
        if isinstance(supply, (int, float)) and isinstance(demand, (int, float)):
            stress_delta = float(supply) - float(demand)
        output.append({**row, "stress_delta_mw": stress_delta})
    return output
