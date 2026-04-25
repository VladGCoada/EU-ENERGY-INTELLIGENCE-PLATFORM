"""Reference-data extraction helpers."""

from __future__ import annotations

from eu_energy_intelligence.ingestion.entsoe_client import (
    FLOW_CORRIDORS,
    RENEWABLE_PSR_TYPES,
    ZONE_EIC,
)


def extract_reference_data() -> dict[str, object]:
    """Return static ENTSO-E reference data used by the platform."""
    return {
        "bidding_zones": dict(ZONE_EIC),
        "flow_corridors": list(FLOW_CORRIDORS),
        "renewable_psr_types": sorted(RENEWABLE_PSR_TYPES),
    }
