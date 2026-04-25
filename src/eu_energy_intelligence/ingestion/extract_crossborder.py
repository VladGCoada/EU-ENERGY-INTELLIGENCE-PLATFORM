from __future__ import annotations

from eu_energy_intelligence.ingestion.entsoe_client import EntsoeClient
from eu_energy_intelligence.ingestion.write_raw_files import write_raw_xml
from eu_energy_intelligence.settings import get_env, load_config


def run_crossborder_extract(
    in_domain: str,
    out_domain: str,
    period_start: str,
    period_end: str,
) -> str:
    """Extract cross-border flow data from ENTSO-E and save raw XML."""
    config = load_config(get_env("APP_ENV", "dev"))
    client = EntsoeClient()
    payload = client.get(
        {
            "documentType": "A11",
            "in_Domain": in_domain,
            "out_Domain": out_domain,
            "periodStart": period_start,
            "periodEnd": period_end,
        }
    )
    country_pair = f"{in_domain}_to_{out_domain}".replace("-", "")
    return write_raw_xml("crossborder", country_pair, payload, config["raw_data_dir"])
