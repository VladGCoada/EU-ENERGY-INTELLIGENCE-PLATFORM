from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

from eu_energy_intelligence.settings import PlatformConfig


@dataclass(slots=True)
class CarbonIntensityClient:
    """Normalize carbon-intensity series for downstream market analytics."""

    config: PlatformConfig | None = None

    def __post_init__(self) -> None:
        self.config = self.config or PlatformConfig()

    def fetch_intensity(self, from_time: str, to_time: str, timeout: int = 30) -> dict[str, Any]:
        url = f"{self.config.carbon_base_url}/intensity/{from_time}/{to_time}"
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def normalize(payload: dict[str, Any], zone: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for item in payload.get("data", []):
            intensity = item.get("intensity", {})
            rows.append(
                {
                    "zone": zone,
                    "from_timestamp": item.get("from"),
                    "to_timestamp": item.get("to"),
                    "carbon_intensity_gco2_kwh": CarbonIntensityClient._to_float(
                        intensity.get("actual")
                    ),
                    "carbon_index": intensity.get("index"),
                }
            )
        return rows

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        return float(value)
