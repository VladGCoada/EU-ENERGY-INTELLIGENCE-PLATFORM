from eu_energy_intelligence.ingestion.normalize_xml import normalize_generation_xml, normalize_price_xml


def test_normalize_xml_helpers_return_lists() -> None:
    assert isinstance(normalize_generation_xml("<Document />"), list)
    assert isinstance(normalize_price_xml("<Document />"), list)
