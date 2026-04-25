from eu_energy_intelligence.silver.standardize_units import ensure_mw


def test_ensure_mw_scales_numeric_values() -> None:
    rows = ensure_mw([{"quantity": 2.5}], "quantity", scale_factor=1000.0)
    assert rows == [{"quantity": 2500.0}]
