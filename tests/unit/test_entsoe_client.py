from eu_energy_intelligence.ingestion.entsoe_client import ProductionEntsoeClient, ZONE_EIC


def test_entsoe_client_exposes_known_zone_eic_codes() -> None:
    assert ProductionEntsoeClient.eic("NL") == ZONE_EIC["NL"]
