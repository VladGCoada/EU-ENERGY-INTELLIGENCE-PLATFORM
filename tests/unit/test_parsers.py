from pathlib import Path

from eu_energy_intelligence.ingestion.parsers import parse_generation_xml


def test_parse_generation_xml_fixture() -> None:
    fixture_path = Path("tests/fixtures/entsoe_generation_sample.xml")
    xml_text = fixture_path.read_text(encoding="utf-8")

    rows = parse_generation_xml(xml_text, source_file="sample.xml", country_code="NL")

    assert rows == [
        {
            "position": 1,
            "quantity": 10.0,
            "source_file": "sample.xml",
            "country_code": "NL",
            "dataset_name": "generation",
        },
        {
            "position": 2,
            "quantity": 20.0,
            "source_file": "sample.xml",
            "country_code": "NL",
            "dataset_name": "generation",
        },
    ]
