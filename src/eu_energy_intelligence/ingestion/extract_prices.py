from __future__ import annotations

from eu_energy_intelligence.ingestion.entsoe_client import EntsoeClient
from eu_energy_intelligence.ingestion.write_raw_files import write_raw_xml
from eu_energy_intelligence.settings import get_env, load_config


def run_prices_extract(country: str, period_start: str, period_end: str) -> str:
    """Extract day-ahead prices from ENTSO-E and save raw XML."""
    config = load_config(get_env("APP_ENV", "dev"))
    client = EntsoeClient()
    payload = client.get(
        {
            "documentType": "A44",
            "in_Domain": country,
            "out_Domain": country,
            "periodStart": period_start,
            "periodEnd": period_end,
        }
    )
    return write_raw_xml("prices", country, payload, config["raw_data_dir"])
