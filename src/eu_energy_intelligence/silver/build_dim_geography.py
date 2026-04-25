"""Geography dimension builders."""

from __future__ import annotations

from eu_energy_intelligence.ingestion.entsoe_client import ZONE_EIC


def build_dim_geography() -> list[dict[str, str]]:
    """Build a simple geography dimension from known bidding zones."""
    return [{"zone": zone, "eic": eic} for zone, eic in sorted(ZONE_EIC.items())]
