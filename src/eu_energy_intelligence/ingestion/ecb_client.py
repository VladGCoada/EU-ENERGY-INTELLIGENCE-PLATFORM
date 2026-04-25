from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

from eu_energy_intelligence.settings import PlatformConfig


@dataclass(slots=True)
class EcbExchangeRateClient:
    """Lightweight ECB client for currency normalization in serving marts."""

    config: PlatformConfig | None = None

    def __post_init__(self) -> None:
        self.config = self.config or PlatformConfig()

    def fetch_series(
        self,
        series_key: str,
        params: dict[str, str] | None = None,
        timeout: int = 30,
    ) -> dict[str, Any]:
        response = requests.get(
            f"{self.config.ecb_base_url}/{series_key}",
            params=params or {"format": "jsondata"},
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def normalize_observations(
        payload: dict[str, Any],
        base_currency: str,
        quote_currency: str,
    ) -> list[dict[str, Any]]:
        observations = (
            payload.get("dataSets", [{}])[0]
            .get("series", {})
            .get("0:0:0:0:0", {})
            .get("observations", {})
        )
        dimension_values = (
            payload.get("structure", {})
            .get("dimensions", {})
            .get("observation", [{}])[0]
            .get("values", [])
        )

        rows: list[dict[str, Any]] = []
        for position, values in observations.items():
            index = int(position)
            timestamp = dimension_values[index]["id"] if index < len(dimension_values) else str(index)
            rows.append(
                {
                    "base_currency": base_currency,
                    "quote_currency": quote_currency,
                    "timestamp": timestamp,
                    "fx_rate": float(values[0]),
                }
            )
        return rows
