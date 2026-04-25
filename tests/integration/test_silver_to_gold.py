from eu_energy_intelligence.gold.renewable_stability import build_renewable_stability


def test_silver_to_gold_pipeline_fixture() -> None:
    rows = build_renewable_stability(
        [
            {"country_code": "NL", "quantity": 10.0},
            {"country_code": "NL", "quantity": 20.0},
        ]
    )

    assert rows[0]["total_generation"] == 30.0
