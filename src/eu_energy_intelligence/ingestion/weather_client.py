from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

from eu_energy_intelligence.settings import PlatformConfig


@dataclass(slots=True)
class WeatherClient:
    """Small weather client for extension-style exogenous feature ingestion."""

    config: PlatformConfig | None = None

    def __post_init__(self) -> None:
        self.config = self.config or PlatformConfig()

    def build_params(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
    ) -> dict[str, str]:
        return {
            "latitude": str(latitude),
            "longitude": str(longitude),
            "start_date": start_date,
            "end_date": end_date,
            "hourly": "temperature_2m,wind_speed_10m,shortwave_radiation",
            "timezone": "UTC",
        }

    def fetch_hourly(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        timeout: int = 30,
    ) -> dict[str, Any]:
        response = requests.get(
            self.config.weather_base_url,
            params=self.build_params(latitude, longitude, start_date, end_date),
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def normalize_hourly(payload: dict[str, Any], zone: str) -> list[dict[str, Any]]:
        hourly = payload.get("hourly", {})
        timestamps = hourly.get("time", [])
        temperatures = hourly.get("temperature_2m", [])
        wind_speeds = hourly.get("wind_speed_10m", [])
        radiation = hourly.get("shortwave_radiation", [])

        rows: list[dict[str, Any]] = []
        for index, timestamp in enumerate(timestamps):
            rows.append(
                {
                    "zone": zone,
                    "timestamp": timestamp,
                    "temperature_c": WeatherClient._safe_number(temperatures, index),
                    "wind_speed_m_s": WeatherClient._safe_number(wind_speeds, index),
                    "solar_radiation_w_m2": WeatherClient._safe_number(radiation, index),
                }
            )
        return rows

    @staticmethod
    def _safe_number(values: list[Any], index: int) -> float | None:
        if index >= len(values) or values[index] is None:
            return None
        return float(values[index])
