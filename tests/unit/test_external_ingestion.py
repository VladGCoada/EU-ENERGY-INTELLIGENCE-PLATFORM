from eu_energy_intelligence.ingestion import (
    CarbonIntensityClient,
    EcbExchangeRateClient,
    WeatherClient,
)


def test_weather_client_builds_extension_style_params() -> None:
    client = WeatherClient()

    params = client.build_params(52.37, 4.90, "2024-01-01", "2024-01-02")

    assert params["latitude"] == "52.37"
    assert "wind_speed_10m" in params["hourly"]


def test_weather_client_normalizes_hourly_payload() -> None:
    rows = WeatherClient.normalize_hourly(
        {
            "hourly": {
                "time": ["2024-01-01T00:00", "2024-01-01T01:00"],
                "temperature_2m": [5.0, 4.5],
                "wind_speed_10m": [7.5, 8.0],
                "shortwave_radiation": [0.0, 10.0],
            }
        },
        zone="NL",
    )

    assert rows[0]["zone"] == "NL"
    assert rows[1]["solar_radiation_w_m2"] == 10.0


def test_carbon_client_normalizes_payload() -> None:
    rows = CarbonIntensityClient.normalize(
        {
            "data": [
                {
                    "from": "2024-01-01T00:00Z",
                    "to": "2024-01-01T00:30Z",
                    "intensity": {"actual": 123, "index": "moderate"},
                }
            ]
        },
        zone="NL",
    )

    assert rows == [
        {
            "zone": "NL",
            "from_timestamp": "2024-01-01T00:00Z",
            "to_timestamp": "2024-01-01T00:30Z",
            "carbon_intensity_gco2_kwh": 123.0,
            "carbon_index": "moderate",
        }
    ]


def test_ecb_client_normalizes_observations() -> None:
    rows = EcbExchangeRateClient.normalize_observations(
        {
            "dataSets": [{"series": {"0:0:0:0:0": {"observations": {"0": [1.08], "1": [1.09]}}}}],
            "structure": {
                "dimensions": {
                    "observation": [
                        {"values": [{"id": "2024-01-01"}, {"id": "2024-01-02"}]}
                    ]
                }
            },
        },
        base_currency="USD",
        quote_currency="EUR",
    )

    assert rows[-1]["fx_rate"] == 1.09
    assert rows[-1]["timestamp"] == "2024-01-02"
