from eu_energy_intelligence.gold.renewable_stability import build_renewable_stability


def test_renewable_stability_builder_returns_country_rows() -> None:
    rows = build_renewable_stability([{"country_code": "DE", "quantity": 10.0}])
    assert rows[0]["country_code"] == "DE"
