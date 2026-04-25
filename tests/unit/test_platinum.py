from eu_energy_intelligence.platinum import build_market_summary


def test_build_market_summary_enriches_gold_rows() -> None:
    rows = build_market_summary(
        gold_rows=[
            {
                "country_code": "NL",
                "total_generation": 100.0,
                "avg_generation": 50.0,
                "max_generation": 60.0,
                "min_generation": 40.0,
                "volatility_index": 20.0,
            }
        ],
        carbon_rows=[
            {
                "zone": "NL",
                "carbon_intensity_gco2_kwh": 200.0,
            }
        ],
        weather_rows=[
            {
                "zone": "NL",
                "wind_speed_m_s": 8.0,
                "solar_radiation_w_m2": 25.0,
            }
        ],
        fx_rows=[
            {"timestamp": "2024-01-01", "fx_rate": 1.07},
            {"timestamp": "2024-01-02", "fx_rate": 1.08},
        ],
    )

    assert rows[0]["stability_score"] == 80.0
    assert rows[0]["carbon_efficiency_score"] == 0.5
    assert rows[0]["fx_rate_to_eur"] == 1.08
