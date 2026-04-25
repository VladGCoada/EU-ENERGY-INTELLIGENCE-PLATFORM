from eu_energy_intelligence.bronze.generation import build_bronze_generation
from eu_energy_intelligence.silver.generation import build_generation_silver


def test_bronze_to_silver_pipeline_fixture() -> None:
    bronze_rows = build_bronze_generation("tests/fixtures/entsoe_generation_sample.xml", "NL")
    silver_rows, quarantine_rows = build_generation_silver(bronze_rows)

    assert len(bronze_rows) == 2
    assert len(silver_rows) == 2
    assert quarantine_rows == []
