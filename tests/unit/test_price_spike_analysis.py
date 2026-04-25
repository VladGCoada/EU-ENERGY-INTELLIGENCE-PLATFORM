from eu_energy_intelligence.gold.price_spike_analysis import build_price_spike_analysis


def test_price_spike_analysis_flags_spikes() -> None:
    rows = build_price_spike_analysis([{"price_eur_mwh": 250.0}], spike_threshold=200.0)
    assert rows[0]["is_price_spike"] is True
